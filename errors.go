package carbium

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ErrorKind is the coarse category of an Error. Code and Reason carry the service's own detail.
type ErrorKind string

const (
	// KindAuth: 401 or 403. The key is unknown, suspended or revoked, lacks the plane or module, or the address is
	// not allowed. Also a key of the wrong shape, refused before any call.
	KindAuth ErrorKind = "auth"
	// KindCredits: 402, credits exhausted. Waiting never helps.
	KindCredits ErrorKind = "credits"
	// KindRateLimit: 429, a rate, slot or load refusal, or credits still loading. Waiting helps.
	KindRateLimit ErrorKind = "rate_limit"
	// KindRequest: the request itself must change (bad params, an unknown method, a body or batch over a cap).
	KindRequest ErrorKind = "request"
	// KindUpstream: a 5xx, or an answer that is not JSON-RPC.
	KindUpstream ErrorKind = "upstream"
	// KindNode: HTTP 200 with an error from the node, the index or the DAS store.
	KindNode ErrorKind = "node"
	// KindRoute: the quote and swap plane found no route, or the quote expired.
	KindRoute ErrorKind = "route"
	// KindTransport: no HTTP answer at all (a network error or a timeout).
	KindTransport ErrorKind = "transport"
	// KindSocket: a WebSocket refusal or a terminal close.
	KindSocket ErrorKind = "socket"
	// KindStream: a Carbon Stream refusal that ends the stream. The stream module reports it in its own error
	// type, so gRPC stays out of this module.
	KindStream ErrorKind = "stream"
	// KindAccount: the account API on carbium.sh answered with an error.
	KindAccount ErrorKind = "account"
	// KindDCA: the DCA API answered ok:false or an error status.
	KindDCA ErrorKind = "dca"
	// KindTokens: the token API answered with an error.
	KindTokens ErrorKind = "tokens"
	// KindWebhook: a webhook delivery failed its signature check.
	KindWebhook ErrorKind = "webhook"
)

// Error is the one error type of this module. Check for it with errors.As. Message, Data and the cause never hold
// an API key, an account key or a webhook secret: they are cut out before the Error is made.
type Error struct {
	Kind ErrorKind
	// Message is the service's own message, redacted, with " (x-request-id: <id>)" added when the answer had one.
	Message string
	// HTTPStatus is the status of the answer; 0 when nothing answered.
	HTTPStatus int
	// Code is the JSON-RPC error code, a WebSocket close code or a gRPC status code; 0 when there was none.
	Code int
	// ErrorCode is the error string of the account, DCA or token API ("rate_limited"); empty for JSON-RPC.
	ErrorCode string
	// Reason is the service's error.data.reason (UPPER_SNAKE), a close reason or a webhook check's reason.
	Reason string
	// Retryable says whether waiting and sending the same request again can change the answer.
	Retryable bool
	// RetryAfter is the wait the service asked for; 0 when it gave none.
	RetryAfter time.Duration
	// RequestID is the answer's x-request-id, to quote to support.
	RequestID string
	// Method is the method, route or subscription the error belongs to.
	Method string
	// Data is the error's data object as served, redacted.
	Data json.RawMessage

	cause error
}

func (e *Error) Error() string { return "carbium: " + string(e.Kind) + ": " + e.Message }

// Unwrap returns the cause with its text redacted. errors.Is and errors.As still reach the original cause.
func (e *Error) Unwrap() error {
	if e.cause == nil {
		return nil
	}
	return &redactedError{inner: e.cause}
}

// Format prints the error with every verb, never a secret: %v and %s print Error(), %+v adds the fields, %#v
// prints them as Go syntax. It has a value receiver so a dereferenced Error prints the same way.
func (e Error) Format(f fmt.State, verb rune) {
	switch {
	case verb == 'v' && f.Flag('#'):
		fmt.Fprintf(f, "&carbium.Error{Kind:%q, Message:%q, HTTPStatus:%d, Code:%d, ErrorCode:%q, Reason:%q, Retryable:%t, RetryAfter:%d, RequestID:%q, Method:%q, Data:%q}",
			e.Kind, e.Message, e.HTTPStatus, e.Code, e.ErrorCode, e.Reason, e.Retryable, e.RetryAfter, e.RequestID, e.Method, string(e.Data))
	case verb == 'v' && f.Flag('+'):
		fmt.Fprintf(f, "%s (kind=%s http=%d code=%d error=%q reason=%q retryable=%t retry_after=%s request_id=%q method=%q data=%s)",
			e.Error(), e.Kind, e.HTTPStatus, e.Code, e.ErrorCode, e.Reason, e.Retryable, e.RetryAfter, e.RequestID, e.Method, string(e.Data))
		if e.cause != nil {
			fmt.Fprintf(f, ": %s", RedactSecrets(e.cause.Error()))
		}
	case verb == 'q':
		fmt.Fprintf(f, "%q", e.Error())
	default:
		fmt.Fprint(f, e.Error())
	}
}

// MarshalJSON writes the fields in the names the TypeScript SDK uses.
func (e *Error) MarshalJSON() ([]byte, error) {
	out := struct {
		Name         string          `json:"name"`
		Kind         ErrorKind       `json:"kind"`
		Message      string          `json:"message"`
		HTTPStatus   int             `json:"httpStatus"`
		Code         any             `json:"code"`
		Reason       string          `json:"reason,omitempty"`
		Retryable    bool            `json:"retryable"`
		RetryAfterMs *int64          `json:"retryAfterMs,omitempty"`
		RequestID    string          `json:"requestId,omitempty"`
		Method       string          `json:"method,omitempty"`
		Data         json.RawMessage `json:"data,omitempty"`
	}{Name: "CarbiumError", Kind: e.Kind, Message: e.Message, HTTPStatus: e.HTTPStatus, Reason: e.Reason, Retryable: e.Retryable,
		RequestID: e.RequestID, Method: e.Method, Data: e.Data}
	switch {
	case e.ErrorCode != "":
		out.Code = e.ErrorCode
	case e.Code != 0:
		out.Code = e.Code
	}
	if e.RetryAfter > 0 {
		ms := e.RetryAfter.Milliseconds()
		out.RetryAfterMs = &ms
	}
	return json.Marshal(out)
}

// ErrorInit holds the fields of a new Error; NewError redacts them. The sub-packages of this module use it.
type ErrorInit struct {
	Kind       ErrorKind
	Message    string
	HTTPStatus int
	Code       int
	ErrorCode  string
	Reason     string
	Retryable  bool
	RetryAfter time.Duration
	RequestID  string
	Method     string
	Data       []byte
	Cause      error
}

// NewError makes an Error with every secret cut out of its message, data and cause.
func NewError(in ErrorInit) *Error {
	msg := RedactSecrets(in.Message)
	if in.RequestID != "" {
		msg += " (x-request-id: " + RedactSecrets(in.RequestID) + ")"
	}
	return &Error{Kind: in.Kind, Message: msg, HTTPStatus: in.HTTPStatus, Code: in.Code, ErrorCode: RedactSecrets(in.ErrorCode),
		Reason: RedactSecrets(in.Reason), Retryable: in.Retryable, RetryAfter: in.RetryAfter, RequestID: RedactSecrets(in.RequestID),
		Method: in.Method, Data: redactJSON(in.Data), cause: in.Cause}
}

// Five or more characters after the label: the 4-character prefix the dashboard shows (c_serv_AbCd) names a key and
// is not one.
var (
	secretPattern       = regexp.MustCompile(`\b(c_serv_|c_acct_|whsec_)[A-Za-z0-9+/=_-]{5,}`)
	keyInURLPattern     = regexp.MustCompile(`(?i)((?:api[-_]?key|apiKey|token)=)[^&\s"'<>]+`)
	subprotocolPattern  = regexp.MustCompile(`(api-key\.)[A-Za-z0-9_-]+`)
	redactedPlaceholder = "${1}…"
)

// RedactSecrets replaces service keys, account keys, webhook secrets and key-bearing URL parts with a placeholder.
func RedactSecrets(text string) string {
	text = secretPattern.ReplaceAllString(text, redactedPlaceholder)
	text = keyInURLPattern.ReplaceAllString(text, redactedPlaceholder)
	return subprotocolPattern.ReplaceAllString(text, redactedPlaceholder)
}

// redactJSON redacts every string in a JSON value, keys included. Text that is not JSON is redacted as text and
// kept as a JSON string.
func redactJSON(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		s, _ := json.Marshal(RedactSecrets(string(b)))
		return s
	}
	out, err := json.Marshal(redactValue(v))
	if err != nil {
		return nil
	}
	return out
}

func redactValue(v any) any {
	switch t := v.(type) {
	case string:
		return RedactSecrets(t)
	case []any:
		for i := range t {
			t[i] = redactValue(t[i])
		}
		return t
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[RedactSecrets(k)] = redactValue(x)
		}
		return out
	}
	return v
}

// redactedError shows a cause's text with secrets cut out, and lets errors.Is and errors.As reach the cause.
type redactedError struct{ inner error }

func (r *redactedError) Error() string { return RedactSecrets(r.inner.Error()) }

func (r *redactedError) Unwrap() error {
	next := errors.Unwrap(r.inner)
	if next == nil {
		return nil
	}
	return &redactedError{inner: next}
}

func (r *redactedError) Is(target error) bool { return errors.Is(r.inner, target) }

func (r *redactedError) As(target any) bool { return errors.As(r.inner, target) }

func (r *redactedError) Format(f fmt.State, verb rune) {
	if verb == 'q' {
		fmt.Fprintf(f, "%q", r.Error())
		return
	}
	fmt.Fprint(f, r.Error())
}

// statusText is a short text for an answer without a JSON-RPC message.
func statusText(status int) string { return "HTTP " + strconv.Itoa(status) }

// clip trims text and cuts it to n bytes.
func clip(text string, n int) string {
	text = strings.TrimSpace(text)
	if len(text) > n {
		text = text[:n]
	}
	return text
}
