package carbium

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// A made-up key, joined here so no file holds one whole.
var testKey = "c_serv_" + "Test1234Key"

// reply is one scripted answer of the planted server.
type reply struct {
	status int
	body   string
	header map[string]string
	delay  time.Duration
	answer func(body []byte) string // builds the body from the request, for batches
}

type seenRequest struct {
	at     time.Time
	verb   string
	path   string
	header http.Header
	body   []byte
}

func (s seenRequest) method() string {
	var one struct{ Method string }
	if json.Unmarshal(s.body, &one) == nil {
		return one.Method
	}
	return ""
}

// planted is a local HTTP server that answers from a script and records what it was sent.
type planted struct {
	srv    *httptest.Server
	mu     sync.Mutex
	script []reply
	got    []seenRequest
}

func plant(t *testing.T, script ...reply) *planted {
	t.Helper()
	p := &planted{script: script}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		p.mu.Lock()
		p.got = append(p.got, seenRequest{at: time.Now(), verb: r.Method, path: r.URL.RequestURI(), header: r.Header.Clone(), body: body})
		n := len(p.got)
		step := reply{status: 200, body: `{"jsonrpc":"2.0","id":1,"result":null}`}
		if len(p.script) > 0 {
			step = p.script[min(n, len(p.script))-1]
		}
		p.mu.Unlock()
		if step.delay > 0 {
			select {
			case <-time.After(step.delay):
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-Id", fmt.Sprintf("rid-%d", n))
		for k, v := range step.header {
			w.Header().Set(k, v)
		}
		status := step.status
		if status == 0 {
			status = 200
		}
		w.WriteHeader(status)
		if step.answer != nil {
			_, _ = io.WriteString(w, step.answer(body))
			return
		}
		_, _ = io.WriteString(w, step.body)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *planted) seen() []seenRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]seenRequest(nil), p.got...)
}

func (p *planted) gaps() []time.Duration {
	got := p.seen()
	var out []time.Duration
	for i := 1; i < len(got); i++ {
		out = append(out, got[i].at.Sub(got[i-1].at))
	}
	return out
}

func ok(result string) string { return `{"jsonrpc":"2.0","id":1,"result":` + result + `}` }

func rpcErr(code int, message, reason string, data map[string]any) string {
	if data == nil {
		data = map[string]any{}
	}
	if reason != "" {
		data["reason"] = reason
	}
	e := map[string]any{"code": code, "message": message}
	if len(data) > 0 {
		e["data"] = data
	}
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "error": e})
	return string(b)
}

// waits records the waits the client asked for instead of sleeping, with no jitter.
type waits struct {
	mu sync.Mutex
	d  []time.Duration
}

func (w *waits) list() []time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]time.Duration(nil), w.d...)
}

// testClient is a client on the planted server whose waits are recorded, not slept.
func testClient(t *testing.T, p *planted, opts *Options) (*Client, *waits) {
	t.Helper()
	if opts == nil {
		opts = &Options{}
	}
	opts.Endpoint = p.srv.URL
	c, err := NewClient(testKey, opts)
	if err != nil {
		t.Fatal(err)
	}
	w := &waits{}
	c.c.sleep = func(ctx context.Context, d time.Duration) error {
		w.mu.Lock()
		w.d = append(w.d, d)
		w.mu.Unlock()
		return ctx.Err()
	}
	c.c.jitter = func() time.Duration { return 0 }
	return c, w
}

func asError(t *testing.T, err error) *Error {
	t.Helper()
	e, isErr := err.(*Error)
	if !isErr {
		t.Fatalf("want *carbium.Error, got %T: %v", err, err)
	}
	return e
}

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

func sameWaits(got []time.Duration, want ...time.Duration) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
