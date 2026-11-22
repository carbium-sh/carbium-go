package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	carbium "github.com/carbium-sh/carbium-go"
)

var testKey = "c_serv_" + "socket000000000000000"

type wsRequest struct {
	ID     uint64          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

// fake is a local WebSocket server. Each connection runs script[n] (the last one repeats); by default it acks every
// subscribe with a new id.
type fake struct {
	srv     *httptest.Server
	mu      sync.Mutex
	conns   int
	hs      []http.Header
	urls    []string
	got     []wsRequest
	nextSub uint64
	refuse  int // answer the handshake with this status
	script  []func(c *websocket.Conn, f *fake)
}

func newFake(t *testing.T, script ...func(c *websocket.Conn, f *fake)) *fake {
	f := &fake{script: script, nextSub: 41}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.conns++
		n := f.conns
		f.hs = append(f.hs, r.Header.Clone())
		f.urls = append(f.urls, r.URL.String())
		refuse := f.refuse
		f.mu.Unlock()
		if refuse != 0 {
			http.Error(w, "refused", refuse)
			return
		}
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"carbium"}})
		if err != nil {
			return
		}
		defer c.CloseNow()
		var step func(*websocket.Conn, *fake)
		if len(f.script) > 0 {
			step = f.script[min(n, len(f.script))-1]
		}
		if step == nil {
			step = func(c *websocket.Conn, f *fake) { f.serve(c, nil) }
		}
		step(c, f)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fake) url() string { return "ws" + strings.TrimPrefix(f.srv.URL, "http") }

const (
	doAck     = iota // answer the request with a new id
	doNothing        // the step answered it
	doStop           // end the connection's loop
)

// serve answers every request with a new id, unless step says otherwise.
func (f *fake) serve(c *websocket.Conn, step func(c *websocket.Conn, req wsRequest) int) {
	for {
		_, data, err := c.Read(context.Background())
		if err != nil {
			return
		}
		var req wsRequest
		_ = json.Unmarshal(data, &req)
		f.mu.Lock()
		f.got = append(f.got, req)
		f.nextSub++
		id := f.nextSub
		f.mu.Unlock()
		if step != nil {
			switch step(c, req) {
			case doNothing:
				continue
			case doStop:
				return
			}
		}
		result := any(id)
		if strings.HasSuffix(req.Method, "Unsubscribe") {
			result = true
		}
		b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
		_ = c.Write(context.Background(), websocket.MessageText, b)
	}
}

func (f *fake) requests() []wsRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]wsRequest(nil), f.got...)
}

func (f *fake) connections() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.conns
}

func notify(c *websocket.Conn, method string, sub uint64, result string) {
	b := `{"jsonrpc":"2.0","method":"` + method + `","params":{"subscription":` + itoa(sub) + `,"result":` + result + `}}`
	_ = c.Write(context.Background(), websocket.MessageText, []byte(b))
}

func itoa(n uint64) string { b, _ := json.Marshal(n); return string(b) }

// ackThenClose acks the first subscribe, then closes with code and reason.
func ackThenClose(code websocket.StatusCode, reason string) func(*websocket.Conn, *fake) {
	return func(c *websocket.Conn, f *fake) {
		f.serve(c, func(c *websocket.Conn, req wsRequest) int {
			b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": 7})
			_ = c.Write(context.Background(), websocket.MessageText, b)
			_ = c.Close(code, reason)
			return doStop
		})
	}
}

type harness struct {
	mu     sync.Mutex
	delays []time.Duration
	fatal  chan *carbium.Error
	reseed chan bool
	errs   chan *carbium.Error
}

func socket(t *testing.T, f *fake, opts *Options) (*Socket, *harness) {
	t.Helper()
	h := &harness{fatal: make(chan *carbium.Error, 4), reseed: make(chan bool, 4), errs: make(chan *carbium.Error, 4)}
	if opts == nil {
		opts = &Options{}
	}
	opts.URL = f.url()
	opts.OnFatal = func(e *carbium.Error) { h.fatal <- e }
	opts.OnReseed = func(_ CloseInfo, gap bool) { h.reseed <- gap }
	opts.OnError = func(e *carbium.Error) { h.errs <- e }
	s, err := New(testKey, opts)
	if err != nil {
		t.Fatal(err)
	}
	s.random = func() float64 { return 0 }
	s.sleep = func(ctx context.Context, d time.Duration) error {
		h.mu.Lock()
		h.delays = append(h.delays, d)
		h.mu.Unlock()
		return ctx.Err()
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, h
}

func (h *harness) waits() []time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]time.Duration(nil), h.delays...)
}

func wait[T any](t *testing.T, ch chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("no %s within 5 s", what)
		var zero T
		return zero
	}
}

func ready(t *testing.T, sub *Subscription, err error) uint64 {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := sub.Ready(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestTheSubprotocolCarrierAckAndRoutingByServerID(t *testing.T) {
	f := newFake(t, func(c *websocket.Conn, f *fake) {
		f.serve(c, func(c *websocket.Conn, req wsRequest) int {
			b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": 42})
			_ = c.Write(context.Background(), websocket.MessageText, b)
			notify(c, "accountNotification", 99, `{"context":{"slot":1},"value":{"lamports":1,"owner":"x","data":["","base64"],"executable":false,"rentEpoch":0}}`)
			notify(c, "accountNotification", 42, `{"context":{"slot":5},"value":{"lamports":7,"owner":"x","data":["","base64"],"executable":false,"rentEpoch":0}}`)
			return doNothing
		})
	})
	s, _ := socket(t, f, &Options{Carrier: "subprotocol"})
	got := make(chan carbium.WithContext[carbium.AccountInfo], 2)
	sub, err := s.AccountSubscribe("Acct", &AccountConfig{Commitment: carbium.CommitmentConfirmed}, func(n carbium.WithContext[carbium.AccountInfo]) { got <- n })
	if id := ready(t, sub, err); id != 42 {
		t.Fatalf("server id %d", id)
	}
	n := wait(t, got, "notification")
	if n.Value.Lamports != 7 || n.Context.Slot != 5 {
		t.Fatalf("%+v: a notification for another server id was routed here", n)
	}
	hs := f.hs[0]
	if protos := strings.Split(strings.ReplaceAll(hs.Get("Sec-WebSocket-Protocol"), " ", ""), ","); !reflect.DeepEqual(protos, []string{"carbium", "api-key." + testKey}) || hs.Get("X-Api-Key") != "" || strings.Contains(f.urls[0], "c_serv_") {
		t.Fatalf("handshake %v %s", hs, f.urls[0])
	}
	if p := string(f.requests()[0].Params); p != `["Acct",{"commitment":"confirmed"}]` {
		t.Fatalf("params %s", p)
	}
}

// The default carrier is the x-api-key handshake header: a Go client is never a browser.
func TestTheDefaultCarrierIsTheXAPIKeyHeader(t *testing.T) {
	for name, opts := range map[string]*Options{"default": nil, "header": {Carrier: "header"}} {
		f := newFake(t)
		s, _ := socket(t, f, opts)
		sub, err := s.SlotSubscribe(func(SlotInfo) {})
		ready(t, sub, err)
		hs := f.hs[0]
		if hs.Get("X-Api-Key") != testKey || strings.Contains(hs.Get("Sec-WebSocket-Protocol"), "api-key") || strings.Contains(f.urls[0], "c_serv_") {
			t.Fatalf("%s: handshake %v", name, hs)
		}
	}
	var e *carbium.Error
	if _, err := New(testKey, &Options{Carrier: "url"}); !errors.As(err, &e) || e.Kind != carbium.KindRequest {
		t.Fatalf("an unknown carrier: %v", err)
	}
}

func TestClose1013CreditsPendingReconnectsAndBindsAgain(t *testing.T) {
	f := newFake(t, ackThenClose(1013, "credits_pending"), nil)
	s, h := socket(t, f, nil)
	sub, err := s.LogsSubscribe(LogsMentioning("Prog"), nil, func(carbium.WithContext[carbium.LogsNotification]) {})
	ready(t, sub, err)
	if gap := wait(t, h.reseed, "reseed"); gap {
		t.Fatal("a 1013 is not a gap")
	}
	reqs := f.requests()
	if len(reqs) != 2 || reqs[1].Method != "logsSubscribe" || string(reqs[1].Params) != string(reqs[0].Params) || string(reqs[0].Params) != `[{"mentions":["Prog"]}]` {
		t.Fatalf("%+v", reqs)
	}
	if w := h.waits(); len(w) != 1 || w[0] != time.Second {
		t.Fatalf("waits %v, want [1s]", w)
	}
}

func TestClose1012GapReconnectsAtOnceAndReseedsWithGap(t *testing.T) {
	f := newFake(t, ackThenClose(1012, "stream_gap_reseed_required"), nil)
	s, h := socket(t, f, nil)
	sub, err := s.RootSubscribe(func(uint64) {})
	ready(t, sub, err)
	if !wait(t, h.reseed, "reseed") {
		t.Fatal("gap must be true after stream_gap_reseed_required")
	}
	if w := h.waits(); len(w) != 1 || w[0] != 250*time.Millisecond {
		t.Fatalf("waits %v", w)
	}
}

func TestClose1008KeyRevokedIsFatal(t *testing.T) {
	f := newFake(t, ackThenClose(1008, "key_revoked"))
	s, h := socket(t, f, nil)
	sub, err := s.SlotSubscribe(func(SlotInfo) {})
	ready(t, sub, err)
	e := wait(t, h.fatal, "fatal")
	if e.Kind != carbium.KindAuth || e.Reason != "key_revoked" || s.State() != StateClosed {
		t.Fatalf("%+v %s", e, s.State())
	}
	time.Sleep(100 * time.Millisecond)
	if n := f.connections(); n != 1 {
		t.Fatalf("%d connections: a key refusal never reconnects", n)
	}
}

func TestClose1008CreditsExhaustedReconnectsOnceThenIsFatal(t *testing.T) {
	f := newFake(t, ackThenClose(1008, "credits_exhausted"))
	s, h := socket(t, f, nil)
	sub, err := s.SlotSubscribe(func(SlotInfo) {})
	ready(t, sub, err)
	e := wait(t, h.fatal, "fatal")
	if e.Kind != carbium.KindCredits || f.connections() != 2 {
		t.Fatalf("%+v after %d connections", e, f.connections())
	}
	if w := h.waits(); len(w) != 1 || w[0] != 5*time.Second {
		t.Fatalf("waits %v, want [5s]", w)
	}
}

func TestARefusedHandshakeIsFatalAtOnce(t *testing.T) {
	f := newFake(t)
	f.refuse = 401
	s, h := socket(t, f, nil)
	sub, err := s.SlotSubscribe(func(SlotInfo) {})
	if err != nil {
		t.Fatal(err)
	}
	e := wait(t, h.fatal, "fatal")
	if e.Kind != carbium.KindAuth || e.HTTPStatus != 401 || f.connections() != 1 {
		t.Fatalf("%+v after %d handshakes", e, f.connections())
	}
	if _, err := sub.Ready(context.Background()); err == nil {
		t.Fatal("the pending subscription must be refused")
	}
}

func TestOverloadClosesBackOffFurtherEachTime(t *testing.T) {
	f := newFake(t, ackThenClose(1013, "slow_consumer"), ackThenClose(1013, "slow_consumer"), ackThenClose(1013, "slow_consumer"), nil)
	s, h := socket(t, f, nil)
	sub, err := s.SlotSubscribe(func(SlotInfo) {})
	ready(t, sub, err)
	deadline := time.Now().Add(5 * time.Second)
	for f.connections() < 4 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if w := h.waits(); !reflect.DeepEqual(w, []time.Duration{time.Second, time.Second, 2 * time.Second}) {
		t.Fatalf("waits %v, want at least 1 s, doubling per close in a row", w)
	}
}

func TestABindRefusedWithRateLimitIsTriedAgainAndCreditsExhaustedIsFatal(t *testing.T) {
	calls := 0
	f := newFake(t, func(c *websocket.Conn, f *fake) {
		f.serve(c, func(c *websocket.Conn, req wsRequest) int {
			calls++
			switch calls {
			case 1:
				_ = c.Write(context.Background(), websocket.MessageText, []byte(`{"jsonrpc":"2.0","id":`+itoa(req.ID)+`,"error":{"code":-32005,"message":"rate limited","data":{"reason":"RATE_LIMITED"}}}`))
				return doNothing
			case 2:
				return doAck
			}
			_ = c.Write(context.Background(), websocket.MessageText, []byte(`{"jsonrpc":"2.0","id":`+itoa(req.ID)+`,"error":{"code":-32002,"message":"credits exhausted","data":{"reason":"CREDITS_EXHAUSTED"}}}`))
			return doNothing
		})
	})
	s, h := socket(t, f, nil)
	sub, err := s.SlotSubscribe(func(SlotInfo) {})
	if _, err := sub.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if w := h.waits(); len(w) != 1 || w[0] != time.Second {
		t.Fatalf("waits %v, want [1s]", w)
	}
	if _, err = s.RootSubscribe(func(uint64) {}); err != nil {
		t.Fatal(err)
	}
	if e := wait(t, h.fatal, "fatal"); e.Code != -32002 {
		t.Fatalf("%+v", e)
	}
}

func TestSignatureFiresOnceAndBlockAsksForVersionOne(t *testing.T) {
	f := newFake(t, func(c *websocket.Conn, f *fake) {
		f.serve(c, func(c *websocket.Conn, req wsRequest) int {
			if req.Method != "signatureSubscribe" {
				return doAck
			}
			b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": 77})
			_ = c.Write(context.Background(), websocket.MessageText, b)
			notify(c, "signatureNotification", 77, `{"context":{"slot":3},"value":{"err":null}}`)
			return doNothing
		})
	})
	s, _ := socket(t, f, nil)
	b, err := s.BlockSubscribe(BlockAll, nil, func(carbium.WithContext[carbium.BlockNotification]) {})
	ready(t, b, err)
	got := make(chan carbium.WithContext[carbium.SignatureNotification], 1)
	sig, err := s.SignatureSubscribe("Sig", nil, func(n carbium.WithContext[carbium.SignatureNotification]) { got <- n })
	ready(t, sig, err)
	if n := wait(t, got, "signature notification"); n.Value.Err != nil || n.Context.Slot != 3 {
		t.Fatalf("%+v", n)
	}
	time.Sleep(50 * time.Millisecond)
	if s.Size() != 1 {
		t.Fatalf("size %d: the signature subscription must drop itself", s.Size())
	}
	if p := string(f.requests()[0].Params); p != `["all",{"maxSupportedTransactionVersion":1}]` {
		t.Fatalf("blockSubscribe params %s", p)
	}
}

func TestAKeyInTheURLOrAnAccountKeyIsRefusedBeforeAnyConnection(t *testing.T) {
	var e *carbium.Error
	if _, err := New("c_acct_"+"Abc123Def", nil); !errors.As(err, &e) || e.Kind != carbium.KindAuth {
		t.Fatal(err)
	}
	if _, err := New(testKey, &Options{URL: "wss://services.carbium.sh/?api-key=" + testKey}); !errors.As(err, &e) || e.Kind != carbium.KindRequest || strings.Contains(err.Error(), testKey) {
		t.Fatal(err)
	}
	s, _ := New(testKey, nil)
	deref := reflect.ValueOf(s).Elem().Interface() // the socket by value, as a caller's %v of *s prints it
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		if strings.Contains(fmt.Sprintf(verb, s), testKey) || strings.Contains(fmt.Sprintf(verb, deref), testKey) {
			t.Fatalf("the key prints with %s", verb)
		}
	}
	if _, err := s.Subscribe("notAMethod", nil, func(json.RawMessage) {}); !errors.As(err, &e) || e.Kind != carbium.KindRequest {
		t.Fatal(err)
	}
}

// Every served subscription has a typed method, and each typed method sends its own name.
func TestTheEightTypedSubscriptionsSendTheServedNames(t *testing.T) {
	b, err := os.ReadFile("../spec/served.json")
	if err != nil {
		t.Fatal(err)
	}
	var served struct{ WS []string }
	_ = json.Unmarshal(b, &served)
	f := newFake(t)
	s, _ := socket(t, f, nil)
	subs := []func() (*Subscription, error){
		func() (*Subscription, error) {
			return s.AccountSubscribe("a", nil, func(carbium.WithContext[carbium.AccountInfo]) {})
		},
		func() (*Subscription, error) {
			return s.ProgramSubscribe("p", nil, func(carbium.WithContext[carbium.KeyedAccount]) {})
		},
		func() (*Subscription, error) {
			return s.LogsSubscribe(LogsAll, nil, func(carbium.WithContext[carbium.LogsNotification]) {})
		},
		func() (*Subscription, error) {
			return s.SignatureSubscribe("s", nil, func(carbium.WithContext[carbium.SignatureNotification]) {})
		},
		func() (*Subscription, error) { return s.SlotSubscribe(func(SlotInfo) {}) },
		func() (*Subscription, error) { return s.RootSubscribe(func(uint64) {}) },
		func() (*Subscription, error) { return s.SlotsUpdatesSubscribe(func(SlotsUpdate) {}) },
		func() (*Subscription, error) {
			return s.BlockSubscribe(BlockAll, nil, func(carbium.WithContext[carbium.BlockNotification]) {})
		},
	}
	for _, sub := range subs {
		x, err := sub()
		ready(t, x, err)
	}
	var sent []string
	for _, r := range f.requests() {
		sent = append(sent, r.Method)
	}
	sort.Strings(sent)
	sort.Strings(served.WS)
	if !reflect.DeepEqual(sent, served.WS) {
		t.Fatalf("sent %v, served %v", sent, served.WS)
	}
}

func TestUnsubscribingTheLastClosesTheSocket(t *testing.T) {
	f := newFake(t)
	s, _ := socket(t, f, nil)
	sub, err := s.SlotSubscribe(func(SlotInfo) {})
	id := ready(t, sub, err)
	if err := sub.Unsubscribe(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for s.State() != StateIdle && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	reqs := f.requests()
	if s.State() != StateIdle || len(reqs) != 2 || reqs[1].Method != "slotUnsubscribe" || string(reqs[1].Params) != "["+itoa(id)+"]" {
		t.Fatalf("state %s, requests %+v", s.State(), reqs)
	}
}

func TestAHandshakeRedirectIsNotFollowed(t *testing.T) {
	other := newFake(t)
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.srv.URL+"/", http.StatusFound)
	}))
	t.Cleanup(redirect.Close)
	s, err := New(testKey, &Options{URL: "ws" + strings.TrimPrefix(redirect.URL, "http")})
	if err != nil {
		t.Fatal(err)
	}
	s.sleep = func(ctx context.Context, d time.Duration) error { return errors.New("stop") }
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.SlotSubscribe(func(SlotInfo) {}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if n := other.connections(); n != 0 {
		t.Fatalf("the other host saw %d handshakes", n)
	}
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within 5 s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (f *fake) count(method string) int {
	n := 0
	for _, r := range f.requests() {
		if r.Method == method {
			n++
		}
	}
	return n
}

// Unsubscribe from inside a notification callback, on the socket's reader, returns at once and the socket goes on.
func TestUnsubscribeFromACallbackDoesNotDeadlock(t *testing.T) {
	f := newFake(t, func(c *websocket.Conn, f *fake) {
		f.serve(c, func(c *websocket.Conn, req wsRequest) int {
			if req.Method != "slotSubscribe" {
				return doAck
			}
			b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": 5})
			_ = c.Write(context.Background(), websocket.MessageText, b)
			notify(c, "slotNotification", 5, `{"slot":1,"parent":0,"root":0}`)
			return doNothing
		})
	})
	s, _ := socket(t, f, nil)
	root, err := s.RootSubscribe(func(uint64) {})
	ready(t, root, err)
	done := make(chan error, 1)
	var sub *Subscription
	ready2 := make(chan struct{})
	sub, err = s.SlotSubscribe(func(SlotInfo) {
		<-ready2
		done <- sub.Unsubscribe(context.Background())
	})
	close(ready2)
	if err != nil {
		t.Fatal(err)
	}
	if err := wait(t, done, "Unsubscribe to return from the callback"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the unsubscribe frame", func() bool { return f.count("slotUnsubscribe") == 1 })
	if s.Size() != 1 || s.State() != StateOpen {
		t.Fatalf("size %d state %s", s.Size(), s.State())
	}
}

// A done context on Unsubscribe never closes the connection the other subscriptions share.
func TestADoneContextOnUnsubscribeKeepsTheConnection(t *testing.T) {
	f := newFake(t)
	s, _ := socket(t, f, nil)
	a, err := s.SlotSubscribe(func(SlotInfo) {})
	ready(t, a, err)
	b, err := s.RootSubscribe(func(uint64) {})
	ready(t, b, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.Unsubscribe(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the unsubscribe frame", func() bool { return f.count("slotUnsubscribe") == 1 })
	time.Sleep(100 * time.Millisecond)
	if f.connections() != 1 || s.State() != StateOpen || s.Size() != 1 {
		t.Fatalf("%d connections, state %s, size %d: the connection must stay", f.connections(), s.State(), s.Size())
	}
}

// A Subscribe that lands between the open and the rebind is bound once, and never leaves a server subscription
// nobody holds.
func TestASubscribeDuringTheRebindIsBoundOnce(t *testing.T) {
	f := newFake(t, ackThenClose(1012, "restart"), nil)
	var s *Socket
	var once sync.Once
	var extra *Subscription
	gotExtra := make(chan struct{})
	opts := &Options{OnState: func(st State, _ CloseInfo) {
		if st == StateOpen && s != nil && f.connections() == 2 {
			once.Do(func() {
				extra, _ = s.RootSubscribe(func(uint64) {})
				close(gotExtra)
			})
		}
	}}
	s, _ = socket(t, f, opts)
	first, err := s.SlotSubscribe(func(SlotInfo) {})
	ready(t, first, err)
	<-gotExtra
	ready(t, extra, nil)
	time.Sleep(200 * time.Millisecond)
	if live := f.count("rootSubscribe") - f.count("rootUnsubscribe"); live != 1 {
		t.Fatalf("%d live root subscriptions on the server, want 1 (%+v)", live, f.requests())
	}
}

// In Go a key refusal always shows its status, so handshakes that fail with none keep backing off.
func TestHandshakesWithNoStatusKeepBackingOff(t *testing.T) {
	f := newFake(t)
	f.srv.Close()
	s, h := socket(t, f, nil)
	calls := 0
	stop := make(chan struct{})
	s.sleep = func(ctx context.Context, d time.Duration) error {
		calls++
		if calls == 8 {
			close(stop)
			return errors.New("stop")
		}
		return nil
	}
	if _, err := s.SlotSubscribe(func(SlotInfo) {}); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-h.fatal:
		t.Fatalf("gave up after failed handshakes: %v", e)
	case <-stop:
	case <-time.After(5 * time.Second):
		t.Fatal("the socket neither backed off eight times nor gave up")
	}
	select {
	case e := <-h.fatal:
		t.Fatalf("gave up after failed handshakes: %v", e)
	case <-time.After(100 * time.Millisecond):
	}
}

// Removing the last subscription while the socket waits to reconnect goes idle instead of opening an empty socket.
func TestTheLastUnsubscribeDuringAReconnectGoesIdle(t *testing.T) {
	f := newFake(t, ackThenClose(1012, "restart"), nil)
	// The socket's goroutine can reach backoff before SlotSubscribe's result is assigned here, so the subscription
	// is handed to the callback over a channel, never through a variable this goroutine is still writing.
	handed := make(chan *Subscription, 1)
	var once sync.Once
	opts := &Options{OnState: func(st State, _ CloseInfo) {
		if st == StateBackoff {
			once.Do(func() { _ = (<-handed).Unsubscribe(context.Background()) })
		}
	}}
	s, _ := socket(t, f, opts)
	sub, err := s.SlotSubscribe(func(SlotInfo) {})
	handed <- sub
	ready(t, sub, err)
	waitFor(t, "idle", func() bool { return s.State() == StateIdle })
	time.Sleep(100 * time.Millisecond)
	if n := f.connections(); n != 1 {
		t.Fatalf("%d connections: an empty socket must not reconnect", n)
	}
}
