package account

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	carbium "github.com/carbium-sh/carbium-go"
)

// Made-up keys, joined here so no file holds one whole.
var (
	acctKey = "c_acct_" + "clients000000000000000"
	servKey = "c_serv_" + "clients000000000000000"
)

type seen struct {
	verb, path, auth string
	body             []byte
}

type answer struct {
	status int
	body   string
	header map[string]string
}

type server struct {
	srv    *httptest.Server
	mu     sync.Mutex
	script []answer
	got    []seen
}

func serve(t *testing.T, script ...answer) *server {
	s := &server{script: script}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.got = append(s.got, seen{r.Method, r.URL.RequestURI(), r.Header.Get("Authorization"), b})
		n := len(s.got)
		step := answer{status: 200, body: "null"}
		if len(s.script) > 0 {
			step = s.script[min(n, len(s.script))-1]
		}
		s.mu.Unlock()
		w.Header().Set("X-Request-Id", fmt.Sprintf("rid-%d", n))
		w.Header().Set("Content-Type", "application/json")
		for k, v := range step.header {
			w.Header().Set(k, v)
		}
		w.WriteHeader(step.status)
		_, _ = io.WriteString(w, step.body)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *server) seen() []seen {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]seen(nil), s.got...)
}

// tsRoutes are the routes the TypeScript SDK's account module calls (account.ts), by client: the oracle this
// package mirrors.
var tsRoutes = map[string][]string{
	"Client": {"GET /v1/keys", "POST /v1/keys", "POST /v1/keys/{id}/rotate", "POST /v1/keys/{id}/revoke", "DELETE /v1/keys/{id}",
		"POST /v1/keys/{id}/name", "PATCH /v1/keys/{id}/scope", "PATCH /v1/keys/{id}/caps", "PATCH /v1/keys/{id}/allowlist",
		"PATCH /v1/keys/{id}/limits", "GET /v1/webhooks", "GET /v1/webhooks/{id}", "GET /v1/webhooks/{id}/deliveries",
		"POST /v1/webhooks", "PATCH /v1/webhooks/{id}", "DELETE /v1/webhooks/{id}", "POST /v1/webhooks/{id}/rotate",
		"POST /v1/webhooks/{id}/test", "GET /v1/usage", "GET /v1/keys/usage", "GET /v1/keys/load", "GET /v1/usage/hourly",
		"GET /v1/usage/latency", "GET /v1/account", "GET /v1/balance", "GET /v1/invoices", "GET /v1/payments", "GET /v1/modules",
		"GET /v1/billing/crypto-intent/{id}", "POST /v1/modules/{module}", "GET /v1/api-key", "GET /v1/account/api-keys/{id}/activity"},
	"Session": {"POST /v1/account/api-keys", "GET /v1/account/api-keys", "DELETE /v1/account/api-keys/{id}", "GET /v1/keys",
		"POST /v1/keys/{id}/secret", "POST /v1/modules/{module}", "DELETE /v1/modules/{module}", "POST /v1/account/link-wallet",
		"POST /v1/account/link-wallet", "POST /v1/logout"},
	"Public": {"GET /v1/terms", "GET /v1/plans", "GET /v1/signup/challenge", "POST /v1/login/pin", "POST /v1/login/pin/verify",
		"POST /v1/auth/wallet", "POST /v1/auth/wallet"},
}

// Every call of the package is one the TypeScript module makes and nothing else, and each call sends its own route.
func TestEveryCallMirrorsTheTypeScriptModuleAndSendsItsRoute(t *testing.T) {
	got := map[string][]string{}
	for _, r := range Routes() {
		got[r.Client] = append(got[r.Client], r.Verb+" "+r.Path)
	}
	for client, want := range tsRoutes {
		w, g := append([]string(nil), want...), append([]string(nil), got[client]...)
		sort.Strings(w)
		sort.Strings(g)
		if !reflect.DeepEqual(w, g) {
			t.Errorf("%s routes differ from the TypeScript SDK:\n got %v\nwant %v", client, g, w)
		}
	}
	s := serve(t)
	opts := &Options{BaseURL: s.srv.URL}
	c, err := New(acctKey, opts)
	if err != nil {
		t.Fatal(err)
	}
	sess, err := NewSession(carbium.Secret("session-token-for-tests"), opts)
	if err != nil {
		t.Fatal(err)
	}
	public := map[string]func() error{
		"Terms":           func() error { _, err := Terms(context.Background(), opts); return err },
		"Plans":           func() error { _, err := Plans(context.Background(), opts); return err },
		"SignupChallenge": func() error { _, err := SignupChallenge(context.Background(), opts); return err },
		"PinStart": func() error {
			_, err := PinStart(context.Background(), SignupPinStartReq{Email: "a@example.com"}, opts)
			return err
		},
		"PinVerify": func() error { _, err := PinVerify(context.Background(), SignupPinVerifyReq{}, opts); return err },
		"WalletChallenge": func() error {
			_, err := WalletChallenge(context.Background(), WalletLoginReq{Wallet: "W"}, opts)
			return err
		},
		"WalletVerify": func() error {
			_, err := WalletVerify(context.Background(), WalletLoginReq{Wallet: "W"}, opts)
			return err
		},
	}
	for _, r := range Routes() {
		before := len(s.seen())
		var callErr error
		switch r.Client {
		case "Public":
			call, ok := public[r.Name]
			if !ok {
				t.Errorf("no test call for %s", r.Name)
				continue
			}
			callErr = call()
		default:
			var recv reflect.Value
			switch {
			case r.Client == "Session":
				recv = reflect.ValueOf(sess)
			case r.Group == "":
				recv = reflect.ValueOf(c)
			default:
				recv = reflect.ValueOf(c).Elem().FieldByName(r.Group)
			}
			m := recv.MethodByName(r.Name)
			if !m.IsValid() {
				t.Errorf("%s.%s.%s does not exist", r.Client, r.Group, r.Name)
				continue
			}
			args := []reflect.Value{reflect.ValueOf(context.Background())}
			for i := 1; i < m.Type().NumIn(); i++ {
				switch m.Type().In(i).Kind() {
				case reflect.Int64:
					args = append(args, reflect.ValueOf(int64(7)))
				case reflect.String:
					args = append(args, reflect.ValueOf("quote"))
				default:
					args = append(args, reflect.Zero(m.Type().In(i)))
				}
			}
			out := m.Call(args)
			callErr, _ = out[len(out)-1].Interface().(error)
		}
		if callErr != nil {
			t.Errorf("%s %s: %v", r.Client, r.Name, callErr)
			continue
		}
		sent := s.seen()[before:]
		want := strings.NewReplacer("{id}", "7", "{module}", "quote").Replace(r.Verb + " " + r.Path)
		if len(sent) != 1 || sent[0].verb+" "+strings.SplitN(sent[0].path, "?", 2)[0] != want {
			t.Errorf("%s %s sent %+v, want %s", r.Client, r.Name, sent, want)
			continue
		}
		switch r.Client {
		case "Client":
			if sent[0].auth != "Bearer "+acctKey {
				t.Errorf("%s: the account key must go in Authorization", r.Name)
			}
		case "Session":
			if sent[0].auth != "Bearer session-token-for-tests" {
				t.Errorf("%s: the session token must go in Authorization", r.Name)
			}
		case "Public":
			if sent[0].auth != "" {
				t.Errorf("%s: a sign-in step sends no credential", r.Name)
			}
		}
	}
	if len(Routes()) != 49 {
		t.Errorf("%d calls, want 32 + 10 + 7", len(Routes()))
	}
}

// Case 22, account part: the Bearer header, JSON errors with Retry-After, plain-text errors and the test call.
func TestErrorsCarryTheCodeTheWaitAndTheText(t *testing.T) {
	s := serve(t,
		answer{status: 200, body: `[{"key_id":1,"prefix":"c_serv_ab","status":"active","scope":["rpc"]}]`},
		answer{status: 429, body: `{"error":"rate_limited","message":"too many calls: wait Retry-After seconds"}`, header: map[string]string{"Retry-After": "37"}},
		answer{status: 415, body: "Expected request with `Content-Type: application/json`", header: map[string]string{"Content-Type": "text/plain"}},
		answer{status: 200, body: `{"event_id":"evt_test_1","outcome":"delivered","http_status":200,"reason":"","latency_ms":90,"refusal":null,"ts":1}`},
		answer{status: 200, body: `{"key_id":9,"prefix":"c_serv_ne","raw_key":"` + servKey + `","name":null}`},
	)
	c, err := New(acctKey, &Options{BaseURL: s.srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	keys, err := c.Keys.List(ctx)
	if err != nil || len(keys) != 1 || keys[0].KeyID != 1 || keys[0].Scope[0] != "rpc" {
		t.Fatal(keys, err)
	}
	_, err = c.Account.Balance(ctx)
	var e *carbium.Error
	if !errors.As(err, &e) || e.Kind != carbium.KindAccount || e.HTTPStatus != 429 || e.ErrorCode != "rate_limited" || e.RetryAfter.Seconds() != 37 || e.RequestID != "rid-2" || e.Retryable {
		t.Fatalf("%+v", err)
	}
	_, err = c.Keys.Create(ctx, KeysCreateReq{Name: carbium.Ptr("x")})
	if !errors.As(err, &e) || e.HTTPStatus != 415 || !strings.Contains(e.Message, "Content-Type") {
		t.Fatalf("%+v", err)
	}
	r, err := c.Webhooks.Test(ctx, 7)
	if err != nil || r.Outcome != "delivered" {
		t.Fatal(r, err)
	}
	created, err := c.Keys.Create(ctx, KeysCreateReq{})
	if err != nil || created.RawKey.Reveal() != servKey {
		t.Fatal(created, err)
	}
	sess, _ := NewSession(carbium.Secret(servKey), nil)
	for _, shown := range []string{fmt.Sprintf("%v", created), fmt.Sprintf("%+v", *created), fmt.Sprintf("%#v", created), mustJSON(t, created), fmt.Sprint(c), fmt.Sprintf("%+v", c),
		fmt.Sprintf("%v", *c), fmt.Sprintf("%+v", *c), fmt.Sprintf("%#v", *c), fmt.Sprintf("%v", *sess), fmt.Sprintf("%+v", *sess), fmt.Sprintf("%+v", sess)} {
		if strings.Contains(shown, servKey) || strings.Contains(shown, acctKey) {
			t.Fatalf("a key shows: %s", shown)
		}
	}
	if sent := s.seen(); len(sent) != 5 || string(sent[2].body) != `{"name":"x"}` || sent[4].verb != "POST" {
		t.Fatalf("%+v", sent)
	}
}

func mustJSON(t *testing.T, v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestAServiceKeyAndAKeyInTheURLAreRefused(t *testing.T) {
	var e *carbium.Error
	if _, err := New(servKey, nil); !errors.As(err, &e) || e.Kind != carbium.KindAuth || strings.Contains(err.Error(), servKey) {
		t.Fatal(err)
	}
	if _, err := New(acctKey, &Options{BaseURL: "https://carbium.sh/?api_key=" + acctKey}); !errors.As(err, &e) || e.Kind != carbium.KindRequest || strings.Contains(err.Error(), acctKey) {
		t.Fatal(err)
	}
}

func TestWalletStepsSendTheirStep(t *testing.T) {
	s := serve(t, answer{status: 200, body: `{"nonce_token":"n","message":"sign me"}`}, answer{status: 200, body: `{"session_token":"tok","account":{"account_id":1,"email":null,"wallet":"W","status":"active","created_at":"t","tier":"FREE"}}`})
	opts := &Options{BaseURL: s.srv.URL}
	ch, err := WalletChallenge(context.Background(), WalletLoginReq{Wallet: "W"}, opts)
	if err != nil || ch.Message != "sign me" {
		t.Fatal(ch, err)
	}
	sess, err := WalletVerify(context.Background(), WalletLoginReq{Wallet: "W", NonceToken: carbium.Ptr("n"), Signature: carbium.Ptr("sig")}, opts)
	if err != nil || sess.SessionToken.Reveal() != "tok" || sess.Account.AccountID != 1 {
		t.Fatal(sess, err)
	}
	sent := s.seen()
	if string(sent[0].body) != `{"step":"challenge","wallet":"W"}` || !strings.HasPrefix(string(sent[1].body), `{"step":"verify","wallet":"W","nonce_token":"n","signature":"sig"`) {
		t.Fatalf("%s | %s", sent[0].body, sent[1].body)
	}
}

func TestARedirectIsNotFollowed(t *testing.T) {
	other := serve(t)
	s := serve(t, answer{status: 302, body: "", header: map[string]string{"Location": other.srv.URL + "/v1/keys"}})
	c, _ := New(acctKey, &Options{BaseURL: s.srv.URL})
	_, err := c.Keys.List(context.Background())
	var e *carbium.Error
	if !errors.As(err, &e) || e.HTTPStatus != 302 || len(other.seen()) != 0 {
		t.Fatalf("%+v; the other host saw %d requests", err, len(other.seen()))
	}
}

// Every field the description marks nullable or not required decodes from null or absence.
func TestNullableFieldsDecodeFromNull(t *testing.T) {
	s := serve(t,
		answer{status: 200, body: `[{"key_id":1,"prefix":null,"name":null,"tier":"FREE","scope":["rpc","ws","grpc","quote","das"],"status":"active","per_method_caps":[],"max_connections":null,"max_inflight":null,"ip_allow":[],"created_at":null,"rotated_at":null,"revoked_at":null}]`},
		answer{status: 200, body: `{"max_connections":null,"max_inflight":null}`},
		answer{status: 200, body: `{"name":null}`},
		answer{status: 200, body: `{"webhook_id":2,"key_id":null,"name":null,"url":"https://h.example","status":"active","status_reason":null,"tx_status":"all","address_count":0,"secret_hint":"x","has_auth_header":false,"consecutive_failures":0,"last_success_at":null,"last_failure_at":null,"created_at":"t","updated_at":"t"}`},
		answer{status: 200, body: `[{"hour":"h","key_id":null,"method_class":"scan","calls":1,"p50_ms":null,"p95_ms":null,"p99_ms":null}]`},
		answer{status: 200, body: `[{"period_start":null,"period_end":null,"granted":1,"used":0,"current":true}]`},
		answer{status: 200, body: `[{"ts":1,"action":"a","request_id":null,"detail":null}]`},
		answer{status: 200, body: `{"api_key_id":3,"name":null,"prefix":"c_acct_ab","access":"read","scopes":[],"ip_allow":[],"created_at":"t","expires_at":null,"last_used_at":null}`},
		answer{status: 200, body: `[{"module":"quote","label":"Quote","blurb":"b","available":true,"enabled":false,"plan":null,"plans":[],"status":null,"cancel_at_period_end":null,"period_start":null,"period_end":null,"auto_renew":false,"contract":false,"linked":false}]`},
	)
	c, _ := New(acctKey, &Options{BaseURL: s.srv.URL})
	ctx := context.Background()
	keys, err := c.Keys.List(ctx)
	if err != nil || keys[0].Prefix != nil || keys[0].CreatedAt != nil || keys[0].MaxConnections != nil || keys[0].MaxInflight != nil || len(keys[0].Scope) != 5 {
		t.Fatalf("%+v %v", keys, err)
	}
	if l, err := c.Keys.SetLimits(ctx, 1, KeysLimitsReq{}); err != nil || l.MaxConnections != nil || l.MaxInflight != nil {
		t.Fatal(l, err)
	}
	if r, err := c.Keys.Rename(ctx, 1, KeysRenameReq{}); err != nil || r.Name != nil {
		t.Fatal(r, err)
	}
	if w, err := c.Webhooks.Get(ctx, 2); err != nil || w.KeyID != nil {
		t.Fatal(w, err)
	}
	if rows, err := c.Usage.Latency(ctx, UsageLatencyQuery{}); err != nil || rows[0].P50Ms != nil || rows[0].P95Ms != nil || rows[0].P99Ms != nil || rows[0].KeyID != nil {
		t.Fatal(rows, err)
	}
	if inv, err := c.Account.Invoices(ctx); err != nil || inv[0].PeriodStart != nil || inv[0].PeriodEnd != nil {
		t.Fatal(inv, err)
	}
	if act, err := c.Activity(ctx, 3, ActivityQuery{}); err != nil || act[0].RequestID != nil {
		t.Fatal(act, err)
	}
	if self, err := c.Self(ctx); err != nil || self.Name != nil {
		t.Fatal(self, err)
	}
	if mods, err := c.Account.Modules(ctx); err != nil || len(mods) != 1 || mods[0].Plan != nil {
		t.Fatal(mods, err)
	}
}

// A request can clear a text field with "", and a required list is never sent as null.
func TestRequestsSendEmptyStringsAndEmptyLists(t *testing.T) {
	s := serve(t, answer{status: 200, body: "null"})
	c, _ := New(acctKey, &Options{BaseURL: s.srv.URL})
	ctx := context.Background()
	if _, err := c.Webhooks.Update(ctx, 2, WebhooksUpdateReq{Name: carbium.Ptr(""), AuthHeader: carbium.Ptr("")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Keys.SetAllowlist(ctx, 1, KeysAllowlistReq{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Webhooks.Create(ctx, WebhooksCreateReq{KeyID: 1, URL: "https://h.example"}); err != nil {
		t.Fatal(err)
	}
	sent := s.seen()
	for i, want := range []string{`{"name":"","auth_header":""}`, `{"ip_allow":[]}`, `{"key_id":1,"url":"https://h.example","addresses":[]}`} {
		if string(sent[i].body) != want {
			t.Errorf("request %d sent %s, want %s", i, sent[i].body, want)
		}
	}
}
