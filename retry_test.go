package carbium

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

// The numbered cases of the retry and request rules, each against a planted server through the client's own path.
// Waits are recorded instead of slept (jitter 0), so the suite stays fast; TestWallClockRetryAfter proves the real
// sleep once.

func TestCase01RetryAfterIsWaitedThenOneRetry(t *testing.T) {
	p := plant(t, reply{status: 429, body: rpcErr(-32005, "rate limit exceeded", "RATE_LIMITED", nil), header: map[string]string{"Retry-After": "2"}}, reply{body: ok("7")})
	c, w := testClient(t, p, nil)
	slot, err := c.GetSlot(context.Background(), nil)
	if err != nil || slot != 7 {
		t.Fatalf("GetSlot = %d, %v", slot, err)
	}
	if n := len(p.seen()); n != 2 {
		t.Fatalf("%d requests, want 2", n)
	}
	if got := w.list(); !sameWaits(got, 2*time.Second) {
		t.Fatalf("waits %v, want [2s]: the Retry-After hint must be waited in full", got)
	}
}

func TestWallClockRetryAfter(t *testing.T) {
	p := plant(t, reply{status: 429, body: rpcErr(-32005, "rate limit exceeded", "RATE_LIMITED", nil), header: map[string]string{"Retry-After": "2"}}, reply{body: ok("7")})
	c, err := NewClient(testKey, &Options{Endpoint: p.srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetSlot(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	gaps := p.gaps()
	if len(gaps) != 1 || gaps[0] < 2000*time.Millisecond || gaps[0] >= 2600*time.Millisecond {
		t.Fatalf("gaps %v, want one in [2000 ms, 2600 ms)", gaps)
	}
}

func TestCase02RetryAfterMsWinsOverRetryAfter(t *testing.T) {
	p := plant(t, reply{status: 429, body: rpcErr(-32005, "rate limit exceeded", "RATE_LIMITED", nil), header: map[string]string{"Retry-After": "2", "Retry-After-Ms": "1200"}}, reply{body: ok("1")})
	c, w := testClient(t, p, nil)
	if _, err := c.GetSlot(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if got := w.list(); !sameWaits(got, ms(1200)) {
		t.Fatalf("waits %v, want [1.2s]", got)
	}
}

func TestCase03CreditsExhaustedIsSentOnce(t *testing.T) {
	p := plant(t, reply{status: 402, body: rpcErr(-32002, "credits exhausted", "CREDITS_EXHAUSTED", nil), header: map[string]string{"Retry-After": "1"}})
	c, w := testClient(t, p, nil)
	_, err := c.GetSlot(context.Background(), nil)
	e := asError(t, err)
	if n := len(p.seen()); n != 1 {
		t.Fatalf("%d requests, want 1: a 402 is never sent again", n)
	}
	if len(w.list()) != 0 {
		t.Fatalf("waited %v before giving up", w.list())
	}
	if e.Kind != KindCredits || e.Retryable || e.RetryAfter != time.Second || e.RequestID != "rid-1" || e.Code != -32002 || e.Reason != "CREDITS_EXHAUSTED" {
		t.Fatalf("%+v", e)
	}
	if !strings.HasSuffix(e.Message, "credits exhausted (x-request-id: rid-1)") {
		t.Fatalf("message %q", e.Message)
	}
}

func TestCase04PreflightAt200IsANodeError(t *testing.T) {
	p := plant(t, reply{body: rpcErr(-32002, "Transaction simulation failed", "", nil)})
	c, _ := testClient(t, p, nil)
	_, err := c.SimulateTransaction(context.Background(), "AA==", nil)
	e := asError(t, err)
	if len(p.seen()) != 1 || e.Kind != KindNode || e.Reason != "" || e.Code != -32002 {
		t.Fatalf("%d requests, %+v", len(p.seen()), e)
	}
}

func TestCase05CreditsPendingWaitsTheHint(t *testing.T) {
	p := plant(t, reply{status: 429, body: rpcErr(-32005, "credits pending", "CREDITS_PENDING", nil), header: map[string]string{"Retry-After-Ms": "300"}}, reply{body: ok("1")})
	c, w := testClient(t, p, nil)
	if _, err := c.GetSlot(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if len(p.seen()) != 2 || !sameWaits(w.list(), ms(300)) {
		t.Fatalf("%d requests, waits %v", len(p.seen()), w.list())
	}
}

func TestCase06A503IsTriedFourTimes(t *testing.T) {
	p := plant(t, reply{status: 503, body: rpcErr(-32005, "load shed", "LOAD_SHED", nil)})
	c, w := testClient(t, p, nil)
	_, err := c.GetSlot(context.Background(), nil)
	e := asError(t, err)
	if len(p.seen()) != 4 || e.Kind != KindUpstream || !e.Retryable {
		t.Fatalf("%d requests, %+v", len(p.seen()), e)
	}
	if !sameWaits(w.list(), ms(250), ms(500), ms(1000)) {
		t.Fatalf("waits %v, want 250 ms doubling", w.list())
	}
}

func TestCase07SendTransactionIsNeverSentTwice(t *testing.T) {
	p := plant(t, reply{status: 503, body: rpcErr(-32011, "upstream down", "", nil)})
	c, _ := testClient(t, p, nil)
	_, err := c.SendTransaction(context.Background(), "AA==", nil)
	e := asError(t, err)
	if len(p.seen()) != 1 || e.Kind != KindUpstream || e.Method != "sendTransaction" {
		t.Fatalf("%d requests, %+v", len(p.seen()), e)
	}
	// A batch holding a send is never sent twice either.
	p2 := plant(t, reply{status: 503, body: rpcErr(-32011, "upstream down", "", nil)})
	c2, _ := testClient(t, p2, nil)
	if _, err := c2.Batch(context.Background(), []BatchCall{{Method: "getSlot"}, {Method: "sendTransaction", Params: []any{"AA=="}}}); err == nil || len(p2.seen()) != 1 {
		t.Fatalf("batch with a send: %d requests, %v", len(p2.seen()), err)
	}
}

func TestCase08RetriesOffSendsOnceAndReportsTheHint(t *testing.T) {
	p := plant(t, reply{status: 429, body: rpcErr(-32005, "rate limit exceeded", "RATE_LIMITED", nil), header: map[string]string{"Retry-After-Ms": "50"}})
	c, _ := testClient(t, p, &Options{MaxAttempts: 1})
	_, err := c.GetSlot(context.Background(), nil)
	e := asError(t, err)
	if len(p.seen()) != 1 || e.Kind != KindRateLimit || e.RetryAfter != ms(50) {
		t.Fatalf("%d requests, %+v", len(p.seen()), e)
	}
}

func TestCase09AHintLongerThanTheCapIsHandedToTheCaller(t *testing.T) {
	p := plant(t, reply{status: 503, body: rpcErr(-32011, "upstream down", "", nil), header: map[string]string{"Retry-After": "60"}})
	c, w := testClient(t, p, nil)
	_, err := c.GetSlot(context.Background(), nil)
	e := asError(t, err)
	if len(p.seen()) != 1 || e.RetryAfter != time.Minute || !e.Retryable || len(w.list()) != 0 {
		t.Fatalf("%d requests, waits %v, %+v", len(p.seen()), w.list(), e)
	}
}

// MaxHint is 30 s by default: a hint of 30 s is still waited, and TestCase09's 60 s is not.
func TestA30SecondHintIsWaitedByDefault(t *testing.T) {
	p := plant(t, reply{status: 503, body: rpcErr(-32011, "upstream down", "", nil), header: map[string]string{"Retry-After": "30"}}, reply{body: ok("7")})
	c, w := testClient(t, p, nil)
	if slot, err := c.GetSlot(context.Background(), nil); err != nil || slot != 7 || !sameWaits(w.list(), 30*time.Second) {
		t.Fatalf("GetSlot = %d, %v; waits %v, want [30s]", slot, err, w.list())
	}
}

func TestCase10TheKeyTravelsInTheHeaderOnly(t *testing.T) {
	p := plant(t, reply{body: ok("1")})
	c, _ := testClient(t, p, nil)
	if _, err := c.GetSlot(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	got := p.seen()[0]
	if got.path != "/" || got.header.Get("X-Api-Key") != testKey || got.header.Get("Authorization") != "" {
		t.Fatalf("path %q, headers %v", got.path, got.header)
	}
	if !strings.HasPrefix(got.header.Get("Solana-Client"), "go/carbium-go@") || got.header.Get("Content-Type") != "application/json" {
		t.Fatalf("headers %v", got.header)
	}
	if strings.Contains(got.path, "c_serv_") || string(got.body) != `{"jsonrpc":"2.0","id":1,"method":"getSlot"}` {
		t.Fatalf("body %s", got.body)
	}
}

func TestCase11VersionOneTransactionsAreAskedForUnlessTheCallerChose(t *testing.T) {
	p := plant(t, reply{body: ok("null")})
	c, _ := testClient(t, p, nil)
	ctx := context.Background()
	if tx, err := c.GetTransaction(ctx, "sig", nil); err != nil || tx != nil {
		t.Fatal(tx, err)
	}
	if _, err := c.GetBlock(ctx, 5, &BlockConfig{TransactionDetails: TransactionDetailsNone}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetTransaction(ctx, "sig", &TransactionConfig{MaxSupportedTransactionVersion: Ptr[uint64](0)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Call(ctx, "getBlock", []any{6, nil}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetSlot(ctx, nil); err != nil {
		t.Fatal(err)
	}
	want := []string{
		`["sig",{"maxSupportedTransactionVersion":1}]`,
		`[5,{"maxSupportedTransactionVersion":1,"transactionDetails":"none"}]`,
		`["sig",{"maxSupportedTransactionVersion":0}]`,
		`[6,{"maxSupportedTransactionVersion":1}]`,
		``,
	}
	for i, r := range p.seen() {
		var body struct{ Params json.RawMessage }
		_ = json.Unmarshal(r.body, &body)
		if string(body.Params) != want[i] {
			t.Errorf("call %d sent params %s, want %s", i, body.Params, want[i])
		}
	}
}

func TestCase12AScanIndexBehindIsTriedSixTimesAndTheCallerCanOnlyLowerThat(t *testing.T) {
	row := reply{status: 429, body: rpcErr(-32010, "scan index temporarily unavailable, retry", "CHOT_INDEX_UNAVAILABLE", map[string]any{"retryable": true}), header: map[string]string{"Retry-After-Ms": "100"}}
	p := plant(t, row)
	c, w := testClient(t, p, nil)
	_, err := c.GetProgramAccounts(context.Background(), "p", nil)
	if e := asError(t, err); e.Kind != KindRateLimit || len(p.seen()) != 6 {
		t.Fatalf("%d requests, %+v", len(p.seen()), e)
	}
	if !sameWaits(w.list(), ms(100), ms(200), ms(400), ms(800), ms(1600)) {
		t.Fatalf("waits %v", w.list())
	}
	c2, _ := testClient(t, p, &Options{MaxAttempts: 2})
	_, _ = c2.GetProgramAccounts(context.Background(), "p", nil)
	if n := len(p.seen()); n != 8 {
		t.Fatalf("%d requests in all, want 6 + 2", n)
	}
	c3, _ := testClient(t, p, &Options{MaxAttempts: 9})
	_, _ = c3.GetProgramAccounts(context.Background(), "p", nil)
	if n := len(p.seen()); n != 14 {
		t.Fatalf("%d requests in all, want 6 + 2 + 6: MaxAttempts must not raise a row's count", n)
	}
}

func TestCase13ANodeBehindTheAskedSlotWaitsForIt(t *testing.T) {
	p := plant(t, reply{body: rpcErr(-32016, "Minimum context slot has not been reached", "", map[string]any{"contextSlot": 100})}, reply{body: ok(`{"context":{"slot":105},"value":7}`)})
	c, w := testClient(t, p, nil)
	bal, err := c.GetBalance(context.Background(), "So11111111111111111111111111111111111111112", &CommitmentConfig{MinContextSlot: Ptr[uint64](105)})
	if err != nil || bal.Value != 7 || bal.Context.Slot != 105 {
		t.Fatal(bal, err)
	}
	if !sameWaits(w.list(), ms(1100)) {
		t.Fatalf("waits %v, want 220 ms × 5 missing slots", w.list())
	}
}

func TestCase14ATimeoutIsTriedTwiceAndKeepsItsCause(t *testing.T) {
	p := plant(t, reply{body: ok("1"), delay: 1500 * time.Millisecond})
	c, _ := testClient(t, p, &Options{Timeout: 150 * time.Millisecond})
	_, err := c.GetSlot(context.Background(), nil)
	e := asError(t, err)
	if len(p.seen()) != 2 || e.Kind != KindTransport || e.HTTPStatus != 0 || !e.Retryable {
		t.Fatalf("%d requests, %+v", len(p.seen()), e)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the cause is not the timeout: %v", errors.Unwrap(err))
	}
}

func TestCallerCancelIsReturnedAsIs(t *testing.T) {
	p := plant(t, reply{status: 503, body: rpcErr(-32005, "load shed", "LOAD_SHED", nil), header: map[string]string{"Retry-After": "5"}})
	c, err := NewClient(testKey, &Options{Endpoint: p.srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = c.GetSlot(ctx, nil)
	if err != context.DeadlineExceeded || time.Since(start) > time.Second {
		t.Fatalf("got %v after %v, want the context's own error at once", err, time.Since(start))
	}
}

func TestACancelInFlightReturnsPromptly(t *testing.T) {
	p := plant(t, reply{body: ok("1"), delay: 3 * time.Second})
	c, err := NewClient(testKey, &Options{Endpoint: p.srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	_, err = c.GetSlot(ctx, nil)
	if !errors.Is(err, context.Canceled) || time.Since(start) > time.Second || len(p.seen()) != 1 {
		t.Fatalf("got %v after %v and %d requests, want context.Canceled at once", err, time.Since(start), len(p.seen()))
	}
}

// timeoutTransport fails every request with a network timeout, as a dial or TLS handshake timeout does.
type timeoutTransport struct{}

type netTimeout struct{}

func (netTimeout) Error() string   { return "dial tcp: i/o timeout" }
func (netTimeout) Timeout() bool   { return true }
func (netTimeout) Temporary() bool { return true }

func (timeoutTransport) RoundTrip(*http.Request) (*http.Response, error) { return nil, netTimeout{} }

// Only the attempt's own deadline cuts the attempts to two; a dial timeout is a network error like any other.
func TestADialTimeoutKeepsTheNetworkErrorRow(t *testing.T) {
	p := plant(t)
	c, w := testClient(t, p, &Options{HTTPClient: &http.Client{Transport: timeoutTransport{}}})
	_, err := c.GetSlot(context.Background(), nil)
	if e := asError(t, err); e.Kind != KindTransport || len(w.list()) != 3 {
		t.Fatalf("%v after %d waits, want 4 attempts", e, len(w.list()))
	}
}

func TestAZeroValueClientRefusesEveryCall(t *testing.T) {
	var c Client
	var nilClient *Client
	for _, call := range []func() error{
		func() error { _, err := c.GetSlot(context.Background(), nil); return err },
		func() error { return c.Call(context.Background(), "getSlot", nil, nil) },
		func() error { _, err := c.Batch(context.Background(), []BatchCall{{Method: "getSlot"}}); return err },
		func() error { _, err := c.Cq1Quote(context.Background(), cq1Params); return err },
		func() error { _, err := nilClient.GetSlot(context.Background(), nil); return err },
	} {
		if e := asError(t, call()); e.Kind != KindRequest || !strings.Contains(e.Message, "NewClient") {
			t.Fatalf("%+v", e)
		}
	}
	if c.Endpoint() != "" || fmt.Sprint(&c) == "" {
		t.Fatal("Endpoint and Format must not panic")
	}
}

func TestCase15ABatchRefusedAsAWholeIsOneError(t *testing.T) {
	refused := `[{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"batch exceeds the cardinality cap","data":{"reason":"BATCH_TOO_LARGE","retryable":false}}}]`
	everyOne := func(body []byte) string {
		var calls []struct{ ID uint64 }
		_ = json.Unmarshal(body, &calls)
		var out []map[string]any
		for _, call := range calls {
			out = append(out, map[string]any{"jsonrpc": "2.0", "id": call.ID, "error": map[string]any{"code": -32600, "message": "scan is not batchable", "data": map[string]any{"reason": "SCAN_NOT_BATCHABLE", "retryable": false}}})
		}
		b, _ := json.Marshal(out)
		return string(b)
	}
	p := plant(t, reply{status: 400, body: refused}, reply{answer: everyOne})
	c, _ := testClient(t, p, nil)
	calls := []BatchCall{{Method: "getSlot"}, {Method: "getHealth"}}
	_, err := c.Batch(context.Background(), calls)
	if e := asError(t, err); e.Kind != KindRequest || e.Reason != "BATCH_TOO_LARGE" {
		t.Fatalf("%+v", e)
	}
	_, err = c.Batch(context.Background(), calls)
	if e := asError(t, err); e.Kind != KindRequest || e.Reason != "SCAN_NOT_BATCHABLE" {
		t.Fatalf("%+v", e)
	}
	_, err = c.Batch(context.Background(), []BatchCall{{Method: "getSlot"}, {Method: "notAMethod"}})
	if e := asError(t, err); e.Kind != KindRequest || e.Code != -32601 {
		t.Fatalf("%+v", e)
	}
	if err := c.Call(context.Background(), "notAMethod", nil, nil); asError(t, err).Code != -32601 {
		t.Fatal(err)
	}
	if n := len(p.seen()); n != 2 {
		t.Fatalf("%d requests, want 2: unserved names are refused before sending", n)
	}
}

func TestCase16AnAnswerThatIsNotJSONRPCIsAnUpstreamError(t *testing.T) {
	p := plant(t, reply{body: "null"}, reply{body: "<html>proxy</html>"}, reply{body: `{"jsonrpc":"2.0","id":1}`})
	c, _ := testClient(t, p, &Options{MaxAttempts: 1})
	_, err := c.GetSlot(context.Background(), nil)
	if e := asError(t, err); e.Kind != KindUpstream || e.RequestID != "rid-1" {
		t.Fatalf("%+v", e)
	}
	_, err = c.GetSlot(context.Background(), nil)
	if e := asError(t, err); e.Kind != KindUpstream || !strings.Contains(e.Message, "proxy") {
		t.Fatalf("%+v", e)
	}
	_, err = c.GetSlot(context.Background(), nil)
	if e := asError(t, err); e.Kind != KindUpstream || !strings.Contains(e.Message, "neither result nor error") {
		t.Fatalf("%+v", e)
	}
	if len(p.seen()) != 3 {
		t.Fatalf("%d requests", len(p.seen()))
	}
}

func TestCase17ANetworkErrorKeepsItsCause(t *testing.T) {
	p := plant(t)
	url := p.srv.URL
	p.srv.Close()
	c, err := NewClient(testKey, &Options{Endpoint: url, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.GetSlot(context.Background(), nil)
	e := asError(t, err)
	var op *net.OpError
	if e.Kind != KindTransport || e.HTTPStatus != 0 || !e.Retryable || errors.Unwrap(err) == nil || !errors.As(err, &op) {
		t.Fatalf("%+v (cause %v)", e, errors.Unwrap(err))
	}
}

func TestCase18ABatchTheServiceWouldRefuseIsRefusedBeforeItIsSent(t *testing.T) {
	p := plant(t)
	c, _ := testClient(t, p, nil)
	ctx := context.Background()
	many := make([]BatchCall, 101)
	for i := range many {
		many[i] = BatchCall{Method: "getSlot"}
	}
	keys := make([]string, 2000)
	for i := range keys {
		keys[i] = strings.Repeat("x", 44)
	}
	for _, tc := range []struct {
		calls []BatchCall
		want  string
	}{
		{nil, "1 to 100"},
		{many, "1 to 100"},
		{[]BatchCall{{Method: "getSlot"}, {Method: "getProgramAccounts", Params: []any{"x"}}}, "not batchable"},
		{[]BatchCall{{Method: "quote", Params: QuoteParams{SrcMint: "a", DstMint: "b", AmountIn: "1", SlippageBps: 50}}}, "not batchable"},
		{[]BatchCall{{Method: "getAsset", Params: GetAssetParams{ID: "x"}}, {Method: "getSlot"}}, "DAS"},
		{[]BatchCall{{Method: "getAccountInfo", Params: []any{"a"}}, {Method: "getTransaction", Params: []any{"b"}}, {Method: "getMultipleAccounts", Params: []any{[]string{"c"}}}}, "permit"},
		{[]BatchCall{{Method: "getMultipleAccounts", Params: []any{keys}}}, "51200"},
	} {
		_, err := c.Batch(ctx, tc.calls)
		if e := asError(t, err); e.Kind != KindRequest || !strings.Contains(e.Message, tc.want) {
			t.Errorf("%d calls: %+v, want a request error naming %q", len(tc.calls), e, tc.want)
		}
	}
	_, err := c.GetMultipleAccounts(ctx, keys, nil)
	if e := asError(t, err); e.Kind != KindRequest || !strings.Contains(e.Message, "51200") {
		t.Fatalf("%+v", e)
	}
	if n := len(p.seen()); n != 0 {
		t.Fatalf("%d requests sent, want 0", n)
	}
}

func TestCase19ABatchComesBackInTheOrderSent(t *testing.T) {
	p := plant(t, reply{answer: func(body []byte) string {
		var calls []struct{ ID uint64 }
		_ = json.Unmarshal(body, &calls)
		out := []map[string]any{
			{"jsonrpc": "2.0", "id": calls[1].ID, "error": map[string]any{"code": -32602, "message": "invalid params"}},
			{"jsonrpc": "2.0", "id": calls[2].ID, "result": "ok"},
			{"jsonrpc": "2.0", "id": calls[0].ID, "result": 7},
		}
		b, _ := json.Marshal(out)
		return string(b)
	}})
	c, _ := testClient(t, p, nil)
	res, err := c.Batch(context.Background(), []BatchCall{{Method: "getSlot"}, {Method: "getBalance", Params: []any{"x"}}, {Method: "getHealth"}})
	if err != nil {
		t.Fatal(err)
	}
	var slot uint64
	var health string
	if res[0].Decode(&slot) != nil || slot != 7 || res[2].Decode(&health) != nil || health != "ok" {
		t.Fatalf("%+v", res)
	}
	if res[1].Err == nil || res[1].Err.Kind != KindRequest || res[1].Err.Method != "getBalance" {
		t.Fatalf("%+v", res[1])
	}
	var sent []struct {
		ID     uint64
		Method string
	}
	_ = json.Unmarshal(p.seen()[0].body, &sent)
	if len(sent) != 3 || sent[0].Method != "getSlot" || sent[1].Method != "getBalance" || sent[2].Method != "getHealth" || !(sent[0].ID < sent[1].ID && sent[1].ID < sent[2].ID) {
		t.Fatalf("sent %+v", sent)
	}
}

func TestCase21AKeyOfTheWrongKindOrInTheEndpointIsRefused(t *testing.T) {
	account := "c_acct_" + "Abc123Def456"
	for _, tc := range []struct {
		key, endpoint string
		kind          ErrorKind
	}{
		{account, "", KindAuth},
		{"not a key", "", KindAuth},
		{testKey + "!", "", KindAuth},
		{testKey, "https://services.carbium.sh/?api-key=" + testKey, KindRequest},
		{testKey, "https://services.carbium.sh/" + testKey, KindRequest},
		{testKey, "ftp://services.carbium.sh", KindRequest},
	} {
		_, err := NewClient(tc.key, &Options{Endpoint: tc.endpoint})
		e := asError(t, err)
		if e.Kind != tc.kind || strings.Contains(e.Error(), testKey) || strings.Contains(e.Error(), account) {
			t.Errorf("%q at %q: %v", tc.key, tc.endpoint, e)
		}
	}
	if _, err := NewClient("", nil); err != nil {
		t.Fatalf("an empty key is keyless: %v", err)
	}
}

func TestKeylessSendsNoKeyHeader(t *testing.T) {
	p := plant(t, reply{status: 401, body: rpcErr(-32001, "missing api key", "UNKNOWN_KEY", nil)})
	c, err := NewClient("", &Options{Endpoint: p.srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.GetLatestBlockhash(context.Background(), nil)
	if e := asError(t, err); e.Kind != KindAuth || e.HTTPStatus != 401 || len(p.seen()) != 1 || p.seen()[0].header.Get("X-Api-Key") != "" {
		t.Fatalf("%+v", e)
	}
}

func TestDecideRetryRows(t *testing.T) {
	mcs := uint64(105)
	data := func(s string) map[string]json.RawMessage {
		var m map[string]json.RawMessage
		_ = json.Unmarshal([]byte(s), &m)
		return m
	}
	for _, tc := range []struct {
		rc   retryContext
		want *retryPlan
	}{
		{retryContext{method: "sendTransaction", httpStatus: 503}, nil},
		{retryContext{method: "getSlot"}, standardPlan},
		{retryContext{method: "getSlot", httpStatus: 503, data: data(`{"retryable":false}`)}, nil},
		{retryContext{method: "getSlot", httpStatus: 429, code: -32010, hasCode: true}, &retryPlan{base: ms(100), cap: 5 * time.Second, maxAttempts: 6}},
		{retryContext{method: "getSlot", httpStatus: 429, code: -32005, hasCode: true}, standardPlan},
		{retryContext{method: "getSlot", httpStatus: 425}, &retryPlan{maxAttempts: 2}},
		{retryContext{method: "getSlot", httpStatus: 502, message: "response too large"}, nil},
		{retryContext{method: "getSlot", httpStatus: 502}, standardPlan},
		{retryContext{method: "getSlot", httpStatus: 503, message: "upstream RPC unavailable: x"}, &retryPlan{base: 2 * time.Second, cap: 8 * time.Second, maxAttempts: 4}},
		{retryContext{method: "getSlot", httpStatus: 503, message: "compat verbs not initialised"}, withAttempts(standardPlan, 3)},
		{retryContext{method: "getSlot", httpStatus: 504, message: "upstream_slow scan"}, nil},
		{retryContext{method: "getSlot", httpStatus: 504}, withAttempts(standardPlan, 2)},
		{retryContext{method: "getSlot", httpStatus: 200, code: -32005, hasCode: true, data: data(`{"reason":"X"}`)}, nil},
		{retryContext{method: "getSlot", httpStatus: 200, code: -32602, hasCode: true, message: "the walk read no history yet"}, standardPlan},
		{retryContext{method: "getSlot", httpStatus: 200, code: -32602, hasCode: true, message: "bad"}, nil},
		{retryContext{method: "getSlot", httpStatus: 200, code: -32016, hasCode: true, data: data(`{"contextSlot":100}`), minContextSlot: &mcs}, &retryPlan{base: ms(1100), cap: ms(1100), maxAttempts: 4}},
		{retryContext{method: "getSlot", httpStatus: 200, code: -32016, hasCode: true}, &retryPlan{base: ms(250), cap: ms(250), maxAttempts: 4}},
		{retryContext{method: "getSlot", httpStatus: 200, code: -32004, hasCode: true}, &retryPlan{base: time.Second, cap: 2 * time.Second, maxAttempts: 4}},
		{retryContext{method: "getSlot", httpStatus: 200, code: -32014, hasCode: true}, &retryPlan{base: time.Second, cap: time.Second, maxAttempts: 4}},
		{retryContext{method: "getSlot", httpStatus: 200, code: -32024, hasCode: true}, standardPlan},
		{retryContext{method: "getSlot", httpStatus: 200, code: -32011, hasCode: true, message: "das upstream timed out"}, standardPlan},
		{retryContext{method: "getSlot", httpStatus: 200, code: -32603, hasCode: true, message: "Internal error"}, withAttempts(standardPlan, 2)},
		{retryContext{method: "getSlot", httpStatus: 200, code: -32603, hasCode: true, message: "Internal error: more"}, nil},
		{retryContext{method: "getSlot", httpStatus: 200, code: -32010, hasCode: true, message: "the peer refused the copy"}, &retryPlan{base: 20 * time.Second, cap: 20 * time.Second, maxAttempts: 2}},
		{retryContext{method: "getSlot", httpStatus: 401}, nil},
		{retryContext{method: "getSlot", httpStatus: 402}, nil},
		{retryContext{method: "getSlot", httpStatus: 400}, nil},
	} {
		got := decideRetry(tc.rc)
		if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
			t.Errorf("%+v: got %+v, want %+v", tc.rc, got, tc.want)
		}
	}
}

func TestServerHintOrder(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h := map[string][]string{"Retry-After": {"Thu, 01 Jan 2026 00:00:09 GMT"}}
	if d, ok := serverHint(h, nil, now); !ok || d != 9*time.Second {
		t.Fatalf("date form: %v %v", d, ok)
	}
	if d, ok := serverHint(map[string][]string{"Retry-After": {"3"}}, map[string]json.RawMessage{"retryAfterMs": json.RawMessage("700")}, now); !ok || d != ms(700) {
		t.Fatalf("data wins over Retry-After: %v", d)
	}
	if d, ok := serverHint(map[string][]string{"Retry-After-Ms": {"1200"}}, map[string]json.RawMessage{"retryAfterMs": json.RawMessage("700")}, now); !ok || d != ms(1200) {
		t.Fatalf("retry-after-ms wins over data: %v", d)
	}
	if _, ok := serverHint(map[string][]string{"Retry-After": {"soon"}}, nil, now); ok {
		t.Fatal("an unreadable Retry-After is no hint")
	}
}

// A 3xx is never followed: the key header would go to the other host.
func TestARedirectIsNotFollowed(t *testing.T) {
	other := plant(t, reply{body: ok("1")})
	p := plant(t, reply{status: 302, body: "", header: map[string]string{"Location": other.srv.URL + "/"}})
	c, w := testClient(t, p, nil)
	_, err := c.GetSlot(context.Background(), nil)
	if e := asError(t, err); e.HTTPStatus != 302 || e.Kind != KindUpstream || !strings.Contains(e.Message, "redirect") {
		t.Fatalf("%+v", e)
	}
	_, err = c.Cq1Quote(context.Background(), cq1Params)
	if e := asError(t, err); e.HTTPStatus != 302 {
		t.Fatalf("%+v", e)
	}
	if len(other.seen()) != 0 || len(p.seen()) != 2 || len(w.list()) != 0 {
		t.Fatalf("the other host saw %d requests; %d sent, waits %v", len(other.seen()), len(p.seen()), w.list())
	}
}

// A hint above the cap goes back to the caller on the 200 error path too, and an absurd hint is no hint.
func TestHintsNeverHangOrOverflow(t *testing.T) {
	p := plant(t, reply{body: rpcErr(-32004, "Block not available for slot 5", "", map[string]any{"retryAfterMs": 60000})})
	c, w := testClient(t, p, nil)
	_, err := c.GetBlock(context.Background(), 5, nil)
	if e := asError(t, err); e.RetryAfter != time.Minute || e.Code != -32004 || len(p.seen()) != 1 || len(w.list()) != 0 {
		t.Fatalf("%d requests, waits %v, %+v", len(p.seen()), w.list(), e)
	}
	for _, h := range []map[string]string{{"Retry-After": "Fri, 31 Dec 9999 23:59:59 GMT"}, {"Retry-After-Ms": "99999999999999999999"}, {"Retry-After-Ms": "9223372036854775807"}, {"Retry-After": "9223372036854775807"}} {
		p := plant(t, reply{status: 503, body: rpcErr(-32005, "load shed", "LOAD_SHED", nil), header: h})
		c, w := testClient(t, p, nil)
		_, err := c.GetSlot(context.Background(), nil)
		e := asError(t, err)
		if len(p.seen()) != 1 || len(w.list()) != 0 || e.RetryAfter != maxServerHint || !e.Retryable {
			t.Errorf("%v: %d requests, waits %v, hint %v: a hint too long to wait goes back to the caller, clamped", h, len(p.seen()), w.list(), e.RetryAfter)
		}
	}
}

var capQuote = QuoteParams{SrcMint: "So11111111111111111111111111111111111111112", DstMint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", AmountIn: "1", SlippageBps: 50, SwapTag: Ptr[uint64](1)}

func TestAQuoteWithNoRouteUnderTheAccountCapIsARouteErrorSentOnce(t *testing.T) {
	p := plant(t, reply{body: rpcErr(-32055, "no route fits the account cap", "", map[string]any{"why": "accounts_over_cap"})})
	c, w := testClient(t, p, nil)
	_, err := c.Quote(context.Background(), capQuote)
	e := asError(t, err)
	if e.Kind != KindRoute || e.Code != -32055 || e.HTTPStatus != 200 || e.Retryable || len(p.seen()) != 1 || len(w.list()) != 0 {
		t.Fatalf("%d requests, waits %v, %+v", len(p.seen()), w.list(), e)
	}
}

func TestAQuoteSendsTheAccountCapAndTheIntegratorFeeByTheirLongNamesAndTypesTheFee(t *testing.T) {
	const sol, usdc, wallet = "So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "11111111111111111111111111111111"
	quote := ok(`{"srcAmountIn":"100000000","destAmountOut":"14250000","minAmountOut":"14178750","slippageBps":50,"swapMode":"ExactIn","inputMint":"` + sol +
		`","outputMint":"` + usdc + `","routePlan":[],"quoteId":"q","integratorFee":{"amount":"35625","feeBps":25,"mint":"` + usdc + `","account":"` + wallet + `"}}`)
	p := plant(t, reply{body: quote})
	c, _ := testClient(t, p, nil)
	plain := QuoteParams{SrcMint: sol, DstMint: usdc, AmountIn: "100000000", SlippageBps: 50}
	ask := plain
	ask.MaxAccounts, ask.IntegratorFeeBps, ask.IntegratorFeeAccount = Ptr[uint64](8), "25", wallet
	q, err := c.Quote(context.Background(), ask)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Quote(context.Background(), plain); err != nil {
		t.Fatal(err)
	}
	var sent []map[string]any
	for _, r := range p.seen() {
		var one struct{ Params map[string]any }
		if err := json.Unmarshal(r.body, &one); err != nil {
			t.Fatal(err)
		}
		sent = append(sent, one.Params)
	}
	base := map[string]any{"src_mint": sol, "dst_mint": usdc, "amount_in": "100000000", "slippage_bps": float64(50)}
	asked := map[string]any{"maxAccounts": float64(8), "integrator_fee_bps": "25", "integrator_fee_account": wallet}
	for k, v := range base {
		asked[k] = v
	}
	if !reflect.DeepEqual(sent, []map[string]any{asked, base}) {
		t.Fatalf("sent %v", sent)
	}
	if f := q.IntegratorFee; f == nil || *f != (QuoteIntegratorFee{Amount: "35625", FeeBps: 25, Mint: usdc, Account: wallet}) {
		t.Fatalf("integratorFee %+v", f)
	}
}
