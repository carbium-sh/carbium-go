package carbium

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcErrorBody struct {
	Code    json.RawMessage `json:"code"`
	Message json.RawMessage `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type rpcAnswer struct {
	ID     json.RawMessage `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *rpcErrorBody   `json:"error"`
}

type answer struct {
	status int
	header http.Header
	body   []byte
}

var (
	requestCodes   = map[int]bool{-32600: true, -32601: true, -32602: true, -32700: true, -32012: true, -32015: true}
	requestReasons = map[string]bool{"PROGRAM_NOT_PROVISIONED": true, "CLASSIC_SPL_SCAN": true, "METHOD_NOT_SERVED": true, "UNKNOWN_METHOD": true}
)

func kindOf(status int, code int, hasCode bool, reason string) ErrorKind {
	switch {
	case status == 401 || status == 403:
		return KindAuth
	case status == 402:
		return KindCredits
	case status == 429:
		return KindRateLimit
	case status == 400 || status == 413 || status == 425 || status == 501:
		return KindRequest
	case status != 200:
		return KindUpstream
	case hasCode && ((code <= -32051 && code >= -32054) || (code <= -32060 && code >= -32099)):
		return KindRoute
	case (hasCode && requestCodes[code]) || requestReasons[reason]:
		return KindRequest
	}
	return KindNode
}

// encode writes JSON without HTML escaping, so the body is what the caller wrote.
func encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// rpcError builds the Error for one JSON-RPC error answer, and its retry plan.
func (c *core) rpcError(method string, status int, body *rpcErrorBody, h http.Header, mcs *uint64) (*Error, *retryPlan) {
	var code int
	hasCode := false
	if body != nil && len(body.Code) > 0 {
		var f float64
		if json.Unmarshal(body.Code, &f) == nil {
			code, hasCode = int(f), true
		}
	}
	message := statusText(status)
	var data map[string]json.RawMessage
	var rawData []byte
	if body != nil {
		var m string
		if json.Unmarshal(body.Message, &m) == nil && m != "" {
			message = m
		}
		rawData = body.Data
		_ = json.Unmarshal(body.Data, &data)
	}
	reason, _ := dataString(data, "reason")
	plan := decideRetry(retryContext{method: method, httpStatus: status, code: code, hasCode: hasCode, message: message, data: data, minContextSlot: mcs})
	retryable := plan != nil
	if b, ok := dataBool(data, "retryable"); ok {
		retryable = b
	}
	in := ErrorInit{Kind: kindOf(status, code, hasCode, reason), Message: message, HTTPStatus: status, Code: code, Reason: reason,
		Retryable: retryable, Method: method, Data: rawData}
	if hint, ok := serverHint(h, data, c.now()); ok {
		in.RetryAfter = hint
	}
	if h != nil {
		in.RequestID = h.Get("X-Request-Id")
	}
	return NewError(in), plan
}

func (c *core) stop(plan *retryPlan, attempt int) bool {
	if plan == nil {
		return true
	}
	limit := plan.maxAttempts
	if c.maxAttempts > 0 && c.maxAttempts < limit {
		limit = c.maxAttempts
	}
	return attempt >= limit
}

// sendMode says how a 200 answer is read: one JSON-RPC answer, a batch, or a REST object.
type sendMode int

const (
	modeCall sendMode = iota
	modeBatch
	modeREST
)

// send makes one request and applies the retry table. A 200 answer that is a JSON object or array comes back;
// every other outcome is an error.
func (c *core) send(ctx context.Context, verb, url string, body []byte, method string, mode sendMode, mcs *uint64) (*answer, error) {
	if n := len(body); n > MaxBodyBytes {
		return nil, NewError(ErrorInit{Kind: KindRequest, Method: method, Message: fmt.Sprintf("the request body is %d bytes; the cap is %d: split the batch or narrow the call", n, MaxBodyBytes)})
	}
	for attempt := 1; ; attempt++ {
		a, attemptTimedOut, err := c.do(ctx, verb, url, body)
		if err != nil {
			if ctx.Err() != nil {
				return nil, context.Cause(ctx) // the caller's own cancel or deadline, as it is
			}
			plan := decideRetry(retryContext{method: method})
			if plan != nil && attemptTimedOut {
				plan = withAttempts(plan, 2) // a read that outran the client timeout is tried once more
			}
			if c.stop(plan, attempt) {
				return nil, NewError(ErrorInit{Kind: KindTransport, Method: method, Message: err.Error(), Retryable: plan != nil, Cause: err})
			}
			if err := c.sleep(ctx, backoff(plan, attempt, 0, c.jitter())); err != nil {
				return nil, err
			}
			continue
		}
		if a.status >= 300 && a.status < 400 {
			return nil, NewError(ErrorInit{Kind: KindUpstream, Method: method, HTTPStatus: a.status, Message: fmt.Sprintf("the service answered with a redirect (HTTP %d), which is not followed", a.status),
				RequestID: a.header.Get("X-Request-Id")})
		}
		trimmed := bytes.TrimSpace(a.body)
		isObject := len(trimmed) > 0 && trimmed[0] == '{' && json.Valid(trimmed)
		isArray := len(trimmed) > 0 && trimmed[0] == '[' && json.Valid(trimmed)
		if a.status == 200 && (isObject || isArray) {
			if isArray || mode != modeCall {
				return a, nil // a batch's per-element errors are the caller's; a REST object is the answer
			}
			var one rpcAnswer
			_ = json.Unmarshal(trimmed, &one)
			if one.Error == nil {
				return a, nil
			}
			e, plan := c.rpcError(method, 200, one.Error, a.header, mcs)
			if c.stop(plan, attempt) || e.RetryAfter > c.maxHint {
				return a, nil // the caller reads the error answer, with its hint
			}
			if err := c.sleep(ctx, backoff(plan, attempt, e.RetryAfter, c.jitter())); err != nil {
				return nil, err
			}
			continue
		}
		var errBody *rpcErrorBody
		switch {
		case isArray:
			var many []rpcAnswer
			if json.Unmarshal(trimmed, &many) == nil && len(many) > 0 {
				errBody = many[0].Error
			}
		case isObject:
			var one rpcAnswer
			if json.Unmarshal(trimmed, &one) == nil {
				errBody = one.Error
			}
		}
		if a.status == 200 && errBody == nil {
			text := clip(validText(a.body), 120)
			if text == "" {
				text = "empty body"
			}
			what := "malformed JSON-RPC answer: "
			if mode == modeREST {
				what = "malformed answer: "
			}
			return nil, NewError(ErrorInit{Kind: KindUpstream, Method: method, HTTPStatus: 200, Message: what + text, RequestID: a.header.Get("X-Request-Id")})
		}
		if errBody == nil {
			text := clip(validText(a.body), 200)
			if text == "" {
				text = statusText(a.status)
			}
			msg, _ := json.Marshal(text)
			errBody = &rpcErrorBody{Message: msg}
		}
		e, plan := c.rpcError(method, a.status, errBody, a.header, nil)
		if c.stop(plan, attempt) {
			return nil, e
		}
		if e.RetryAfter > c.maxHint {
			return nil, e
		}
		if err := c.sleep(ctx, backoff(plan, attempt, e.RetryAfter, c.jitter())); err != nil {
			return nil, err
		}
	}
}

// do makes one attempt. timedOut is true only when this attempt's own deadline ended it: a dial or TLS timeout of
// the HTTP client is a network error like any other.
func (c *core) do(ctx context.Context, verb, url string, body []byte) (a *answer, timedOut bool, err error) {
	actx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	defer func() {
		timedOut = err != nil && ctx.Err() == nil && errors.Is(actx.Err(), context.DeadlineExceeded)
	}()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(actx, verb, url, rd)
	if err != nil {
		return nil, false, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Solana-Client", "go/carbium-go@"+Version)
	if c.key.v != "" {
		req.Header.Set("X-Api-Key", c.key.v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, err
	}
	return &answer{status: resp.StatusCode, header: resp.Header, body: data}, false, nil
}

func validText(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	return string(bytes.ToValidUTF8(b, []byte("?")))
}

// versioned are the methods that refuse a version-1 transaction unless maxSupportedTransactionVersion is sent.
var versioned = map[string]bool{"getTransaction": true, "getBlock": true}

// withVersion adds maxSupportedTransactionVersion 1 to the config of getTransaction and getBlock unless the caller
// set it.
func withVersion(method string, params json.RawMessage) json.RawMessage {
	if !versioned[method] || firstByte(params) != '[' {
		return params
	}
	var list []json.RawMessage
	if json.Unmarshal(params, &list) != nil || len(list) == 0 {
		return params
	}
	const dflt = `{"maxSupportedTransactionVersion":1}`
	switch {
	case len(list) == 1:
		list = append(list, json.RawMessage(dflt))
	case isNull(list[1]):
		list[1] = json.RawMessage(dflt)
	case firstByte(list[1]) == '{':
		var cfg map[string]json.RawMessage
		if json.Unmarshal(list[1], &cfg) != nil {
			return params
		}
		if _, set := cfg["maxSupportedTransactionVersion"]; set {
			return params
		}
		rest := bytes.TrimSpace(list[1])[1:]
		if firstByte(rest) == '}' {
			list[1] = json.RawMessage(dflt)
		} else {
			list[1] = append([]byte(`{"maxSupportedTransactionVersion":1,`), rest...)
		}
	default:
		return params
	}
	out, err := json.Marshal(list)
	if err != nil {
		return params
	}
	return out
}

// minContextSlotOf reads minContextSlot from the trailing config of positional params, so a -32016 wait can be sized.
func minContextSlotOf(params json.RawMessage) *uint64 {
	if firstByte(params) != '[' {
		return nil
	}
	var list []json.RawMessage
	if json.Unmarshal(params, &list) != nil || len(list) == 0 {
		return nil
	}
	var cfg struct {
		MinContextSlot *uint64 `json:"minContextSlot"`
	}
	if firstByte(list[len(list)-1]) != '{' || json.Unmarshal(list[len(list)-1], &cfg) != nil {
		return nil
	}
	return cfg.MinContextSlot
}

func (c *core) body(id uint64, method string, params any) (request, error) {
	r := request{JSONRPC: "2.0", ID: id, Method: method}
	if params != nil {
		raw, err := encode(params)
		if err != nil {
			return r, NewError(ErrorInit{Kind: KindRequest, Method: method, Message: "params do not encode as JSON: " + err.Error()})
		}
		if !isNull(raw) {
			r.Params = withVersion(method, raw)
		}
	}
	return r, nil
}

// call sends one served method and decodes its result into out.
func (cl *Client) call(ctx context.Context, method string, params any, out any) error {
	if err := cl.check(method); err != nil {
		return err
	}
	c := cl.c
	r, err := c.body(c.nextID.Add(1), method, params)
	if err != nil {
		return err
	}
	body, err := encode(r)
	if err != nil {
		return NewError(ErrorInit{Kind: KindRequest, Method: method, Message: err.Error()})
	}
	mcs := minContextSlotOf(r.Params)
	a, err := c.send(ctx, http.MethodPost, c.endpoint, body, method, modeCall, mcs)
	if err != nil {
		return err
	}
	var one rpcAnswer
	if firstByte(a.body) != '{' || json.Unmarshal(a.body, &one) != nil {
		return NewError(ErrorInit{Kind: KindUpstream, Method: method, HTTPStatus: a.status, Message: "malformed JSON-RPC answer"})
	}
	if one.Error != nil {
		e, _ := c.rpcError(method, a.status, one.Error, a.header, mcs)
		return e
	}
	if one.Result == nil {
		return NewError(ErrorInit{Kind: KindUpstream, Method: method, HTTPStatus: a.status, Message: "malformed JSON-RPC answer: neither result nor error"})
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(one.Result, out); err != nil {
		return NewError(ErrorInit{Kind: KindUpstream, Method: method, HTTPStatus: a.status, Message: "the " + method + " answer does not fit its type: " + err.Error(),
			RequestID: a.header.Get("X-Request-Id"), Cause: err})
	}
	return nil
}

// Call sends any served method by name with params as given (a slice for positional params, a struct or map for
// by-name ones) and decodes the result into out, which may be nil. A name the services do not serve is refused
// before anything is sent.
func (cl *Client) Call(ctx context.Context, method string, params any, out any) error {
	if _, ok := methodTable[method]; !ok {
		return NewError(ErrorInit{Kind: KindRequest, Method: method, Code: -32601, Message: method + " is not a method the services serve"})
	}
	return cl.call(ctx, method, params, out)
}

// BatchCall is one call of a batch.
type BatchCall struct {
	Method string
	Params any
}

// BatchResult is the answer to one call of a batch: Result, or Err when that call failed.
type BatchResult struct {
	Result json.RawMessage
	Err    *Error
}

// Decode reads the result into out, or returns the call's error.
func (r BatchResult) Decode(out any) error {
	if r.Err != nil {
		return r.Err
	}
	return json.Unmarshal(r.Result, out)
}

// Batch sends 1 to 100 calls in one JSON-RPC batch and returns their answers in the order sent. A batch the service
// would refuse is refused before it is sent: a name it does not serve, a method that never batches, DAS mixed with
// other methods, more than two permit methods, or a body over 51,200 bytes. A refusal of the whole batch is
// returned as the error; a failed call comes back in its BatchResult.
func (cl *Client) Batch(ctx context.Context, calls []BatchCall) ([]BatchResult, error) {
	if err := cl.check("batch"); err != nil {
		return nil, err
	}
	c := cl.c
	if len(calls) == 0 || len(calls) > MaxBatch {
		return nil, NewError(ErrorInit{Kind: KindRequest, Message: fmt.Sprintf("a batch holds 1 to %d calls, not %d", MaxBatch, len(calls))})
	}
	permits, das := 0, 0
	for _, call := range calls {
		spec, ok := methodTable[call.Method]
		if !ok {
			return nil, NewError(ErrorInit{Kind: KindRequest, Method: call.Method, Code: -32601, Message: call.Method + " is not a method the services serve"})
		}
		if spec.Batch == "never" {
			return nil, NewError(ErrorInit{Kind: KindRequest, Method: call.Method, Message: call.Method + " is not batchable: send it alone"})
		}
		if spec.Permit {
			permits++
		}
		if spec.Batch == "das" {
			das++
		}
	}
	if das != 0 && das != len(calls) {
		return nil, NewError(ErrorInit{Kind: KindRequest, Message: "DAS methods batch only with other DAS methods"})
	}
	if permits > MaxPermitPerBatch {
		return nil, NewError(ErrorInit{Kind: KindRequest, Message: fmt.Sprintf("a batch holds at most %d permit methods (getAccountInfo, getMultipleAccounts, getTransaction, ...), not %d: split it", MaxPermitPerBatch, permits)})
	}
	first := c.nextID.Add(uint64(len(calls))) - uint64(len(calls)) + 1
	reqs := make([]request, len(calls))
	label := "batch(" + calls[0].Method + ")"
	for i, call := range calls {
		r, err := c.body(first+uint64(i), call.Method, call.Params)
		if err != nil {
			return nil, err
		}
		reqs[i] = r
		if call.Method == "sendTransaction" {
			label = "sendTransaction" // a batch holding a send is never sent twice
		}
	}
	body, err := encode(reqs)
	if err != nil {
		return nil, NewError(ErrorInit{Kind: KindRequest, Message: err.Error()})
	}
	a, err := c.send(ctx, http.MethodPost, c.endpoint, body, label, modeBatch, nil)
	if err != nil {
		return nil, err
	}
	var answers []rpcAnswer
	if firstByte(a.body) != '[' || json.Unmarshal(a.body, &answers) != nil {
		var one rpcAnswer
		if json.Unmarshal(a.body, &one) == nil && one.Error != nil {
			e, _ := c.rpcError(label, a.status, one.Error, a.header, nil)
			return nil, e
		}
		return nil, NewError(ErrorInit{Kind: KindUpstream, HTTPStatus: a.status, Message: "the service answered a batch with a single object", RequestID: a.header.Get("X-Request-Id")})
	}
	if refused := wholeBatchRefusal(answers); refused != nil {
		e, _ := c.rpcError(label, a.status, refused, a.header, nil)
		return nil, e
	}
	byID := map[uint64]rpcAnswer{}
	for _, el := range answers {
		if id, err := strconv.ParseUint(string(bytes.TrimSpace(el.ID)), 10, 64); err == nil {
			byID[id] = el
		}
	}
	out := make([]BatchResult, len(calls))
	for i, call := range calls {
		el, ok := byID[first+uint64(i)]
		switch {
		case !ok:
			out[i].Err = NewError(ErrorInit{Kind: KindUpstream, Method: call.Method, HTTPStatus: a.status, Message: "no answer for this batch element"})
		case el.Error != nil:
			out[i].Err, _ = c.rpcError(call.Method, a.status, el.Error, a.header, nil)
		default:
			out[i].Result = el.Result
		}
	}
	return out, nil
}

// wholeBatchRefusal returns the shared error when every element carries the same code and the same reason: the
// service refused the batch as a whole.
func wholeBatchRefusal(answers []rpcAnswer) *rpcErrorBody {
	if len(answers) == 0 || answers[0].Error == nil {
		return nil
	}
	first := answers[0].Error
	reasonOf := func(b *rpcErrorBody) string {
		var d map[string]json.RawMessage
		_ = json.Unmarshal(b.Data, &d)
		r, _ := dataString(d, "reason")
		return r
	}
	reason := reasonOf(first)
	if reason == "" {
		return nil
	}
	for _, el := range answers {
		if el.Error == nil || string(el.Error.Code) != string(first.Code) || reasonOf(el.Error) != reason {
			return nil
		}
	}
	return first
}

// RPCError builds the Error for one JSON-RPC error object as the service sent it, at HTTP 200. Package ws uses it
// for refusals that arrive on the socket.
func RPCError(method string, errorObject json.RawMessage) *Error {
	var body rpcErrorBody
	_ = json.Unmarshal(errorObject, &body)
	e, _ := (&core{now: time.Now}).rpcError(method, 200, &body, nil, nil)
	return e
}

// check refuses a Client that was not made by NewClient.
func (cl *Client) check(method string) error {
	if cl == nil || cl.c == nil {
		return NewError(ErrorInit{Kind: KindRequest, Method: method, Message: "this carbium.Client was not made by NewClient: make one with carbium.NewClient"})
	}
	return nil
}
