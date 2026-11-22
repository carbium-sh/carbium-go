package dca

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	carbium "github.com/carbium-sh/carbium-go"
	"github.com/carbium-sh/carbium-go/internal/planted"
)

func TestEveryServedRouteHasACallAndNothingElseDoes(t *testing.T) {
	served := planted.Served(t, "../spec/served.json", "dca")
	isServed := map[string]bool{}
	for _, r := range served {
		isServed[r] = true
		if routes[r] == "" {
			t.Errorf("served but has no call: %s", r)
		}
	}
	for r := range routes {
		if !isServed[r] {
			t.Errorf("has a call but is not served: %s", r)
		}
	}
	s := planted.Start(t, planted.Answer{Body: `{"ok":true}`})
	c, err := New(&Options{BaseURL: s.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	one := U64(1)
	calls := map[string]func() error{
		"Quote": func() error {
			_, err := c.Quote(ctx, QuoteRequest{InputMint: "a", OutputMint: "b", Amount: 5})
			return err
		},
		"Positions": func() error { _, err := c.Positions(ctx, "o", ""); return err },
		"History":   func() error { _, err := c.History(ctx, "o", ""); return err },
		"Position":  func() error { _, err := c.Position(ctx, "P", nil); return err },
		"Health":    func() error { _, err := c.Health(ctx); return err },
		"Open": func() error {
			_, err := c.Open(ctx, OpenRequest{Owner: "o", InputMint: "a", OutputMint: "b", Cycles: 2, AmountPerCycle: &one, Frequency: "daily"})
			return err
		},
		"Update":   func() error { _, err := c.Update(ctx, UpdateRequest{Owner: "o", Position: "p"}); return err },
		"Withdraw": func() error { _, err := c.Withdraw(ctx, WithdrawRequest{Owner: "o", Position: "p"}); return err },
		"Close":    func() error { _, err := c.Close(ctx, CloseRequest{Owner: "o", Position: "p"}); return err },
		"Send":     func() error { _, err := c.Send(ctx, SendRequest{TxnBase64: "AQ=="}); return err },
	}
	for route, name := range routes {
		call, ok := calls[name]
		if !ok {
			t.Errorf("no test call for %s", name)
			continue
		}
		before := len(s.Seen())
		if err := call(); err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		got := s.Seen()[before:]
		want := strings.Replace(route, "{address}", "P", 1)
		if len(got) != 1 || got[0].Verb+" "+strings.SplitN(got[0].Path, "?", 2)[0] != want {
			t.Errorf("%s sent %+v, want %s", name, got, want)
		}
	}
}

// Case 22, DCA part: ok:false bodies are dca errors with their code, u64 amounts travel as strings, the key goes in
// x-api-key and never prints.
func TestRefusalsAmountsAndTheKey(t *testing.T) {
	s := planted.Start(t,
		planted.Answer{Status: 400, Body: `{"ok":false,"err":"same_mint","detail":"inputMint and outputMint are the same"}`},
		planted.Answer{Body: `{"ok":true,"txn_base64":"AQ==","blockhash":"h","last_valid_block_height":5,"position":"p","cost":{},"summary":{"kind":"open"},"authority":"a","escrowAccount":"e","index":0,"plan":{}}`},
		planted.Answer{Status: 429, Body: `{"ok":false,"err":"rate_limited","detail":"slow down"}`, Header: map[string]string{"Retry-After": "3"}},
		planted.Answer{Body: `{"ok":false,"err":"door_busy","detail":"try again"}`},
		planted.Answer{Body: `{"ok":true,"keeper":"k"}`},
	)
	dcaKey := "dca-test-key-" + "0001"
	c, err := New(&Options{BaseURL: s.URL, APIKey: dcaKey})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ten, big := U64(10), U64(12345678901234567890)
	var e *carbium.Error
	_, err = c.Open(ctx, OpenRequest{Owner: "o", InputMint: "m", OutputMint: "m", Cycles: 2, AmountPerCycle: &ten, Frequency: "daily"})
	if !errors.As(err, &e) || e.Kind != carbium.KindDCA || e.ErrorCode != "same_mint" || e.HTTPStatus != 400 || e.Retryable {
		t.Fatalf("%+v", err)
	}
	interval := uint64(3600)
	built, err := c.Open(ctx, OpenRequest{Owner: "o", InputMint: "a", OutputMint: "b", Cycles: 2, TotalAmount: &big, IntervalSecs: &interval})
	if err != nil || built.TxnBase64 != "AQ==" || built.LastValidBlockHeight != 5 {
		t.Fatal(built, err)
	}
	sent := s.Seen()[1]
	if !strings.Contains(string(sent.Body), `"totalAmount":"12345678901234567890"`) || sent.Header.Get("X-Api-Key") != dcaKey || sent.Path != "/open" {
		t.Fatalf("%s %v", sent.Body, sent.Header)
	}
	_, err = c.Quote(ctx, QuoteRequest{InputMint: "a", OutputMint: "b", Amount: 5})
	if !errors.As(err, &e) || e.ErrorCode != "rate_limited" || !e.Retryable || e.RetryAfter != 3*time.Second {
		t.Fatalf("%+v", err)
	}
	if got := s.Seen()[2].Path; got != "/quote?amount=5&inputMint=a&outputMint=b" {
		t.Fatalf("query %s", got)
	}
	_, err = c.Health(ctx)
	if !errors.As(err, &e) || e.ErrorCode != "door_busy" || !e.Retryable || e.HTTPStatus != 200 {
		t.Fatalf("an ok:false at 200 is an error: %+v", err)
	}
	if h, err := c.Health(ctx); err != nil || h.Keeper != "k" {
		t.Fatal(h, err)
	}
	if _, err := c.Open(ctx, OpenRequest{Owner: "o", AmountPerCycle: &ten, TotalAmount: &ten, Frequency: "daily"}); err == nil {
		t.Fatal("both amounts must be refused before sending")
	}
	if len(s.Seen()) != 5 {
		t.Fatalf("%d requests", len(s.Seen()))
	}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		if strings.Contains(fmt.Sprintf(verb, c), dcaKey) || strings.Contains(fmt.Sprintf(verb, *c), dcaKey) {
			t.Fatalf("the key prints with %s", verb)
		}
	}
	b, _ := UpdateRequest{Owner: "o", Position: "p", ClearMaxPrice: true}.MarshalJSON()
	if string(b) != `{"maxPrice":null,"owner":"o","position":"p"}` {
		t.Fatalf("%s", b)
	}
}

func TestARedirectIsNotFollowed(t *testing.T) {
	other := planted.Start(t)
	s := planted.Start(t, planted.Answer{Status: 302, Header: map[string]string{"Location": other.URL + "/health"}})
	c, _ := New(&Options{BaseURL: s.URL, APIKey: "dca-test-key-" + "0001"})
	_, err := c.Health(context.Background())
	var e *carbium.Error
	if !errors.As(err, &e) || e.HTTPStatus != 302 || len(other.Seen()) != 0 {
		t.Fatalf("%+v; the other host saw %d requests", err, len(other.Seen()))
	}
}

// Answers in the shapes the DCA API document gives: every u64 a decimal string.
func TestAnswersInTheDocumentsShapes(t *testing.T) {
	s := planted.Start(t,
		planted.Answer{Body: `{"ok":true,"txn_base64":"AQ==","blockhash":"H","last_valid_block_height":"312000555","position":"P","cost":{"feeLamports":5000,"lamportsLocked":"2039280"},"summary":{"kind":"withdraw","owner":"O","feePayer":"O","instructions":["withdraw"],"amount":"1000000"}}`},
		planted.Answer{Body: `{"ok":true,"out":"7041234","minOut":"7006028","rateE6":"70412","feeBps":10,"inDecimals":6,"outDecimals":9,"valueUsdc":"100000000","limits":{"minAmountPerCycle":"5000000","minValueUsdc":"5000000","minIntervalSecs":3600,"maxCycles":1000,"maxActive":10},"cost":{"requiredLamports":"4200000","heldLamports":"9000000"}}`},
		planted.Answer{Body: `{"ok":true,"positions":[{"address":"A","owner":"O","inMint":"I","outMint":"M","crank":"K","inProgram":"T","outProgram":"T","index":0,"maxSlippageBps":50,"cyclesTotal":10,"cyclesDone":2,"intervalSecs":86400,"amountPerCycle":"1000000","minOutE6":"0","cumIn":"2000000","cumOut":"14000000","nextTs":"1763856000","active":true,"escrow":"8000000","middleDebt":null,"status":"active","statusCode":"scheduled","statusDetail":null,"lastTry":{"at":1763769600,"cycle":2,"result":"filled","code":"filled","detail":null,"signature":"S"},"openedSlot":380000000,"openedSignature":"OS","served":true,"notServed":null}],"next":null}`},
	)
	c, _ := New(&Options{BaseURL: s.URL})
	ctx := context.Background()
	b, err := c.Withdraw(ctx, WithdrawRequest{Owner: "O", Position: "P"})
	if err != nil || b.LastValidBlockHeight != 312000555 {
		t.Fatalf("build answer: %+v %v", b, err)
	}
	q, err := c.Quote(ctx, QuoteRequest{InputMint: "I", OutputMint: "M", Amount: 100000000})
	if err != nil || q.ValueUsdc != 100000000 || q.Limits.MinValueUsdc != 5000000 || q.Cost.RequiredLamports != 4200000 || q.FeeBps != 10 {
		t.Fatalf("quote: %+v %v", q, err)
	}
	ps, err := c.Positions(ctx, "O", "")
	if err != nil || len(ps.Positions) != 1 || ps.Positions[0].CumOut != "14000000" || *ps.Positions[0].OpenedSlot != 380000000 {
		t.Fatalf("positions: %+v %v", ps, err)
	}
}

// The live shapes of 2026-11-22: quote amount, a null minAmountPerCycle, owner on lists, null owner balances and
// signatures, a numeric history openedSlot, an optional withdraw amount, and ok:false refusals at HTTP 200.
func TestTheLiveShapes(t *testing.T) {
	s := planted.Start(t,
		planted.Answer{Body: `{"ok":true,"out":"35000000","minOut":"34825000","rateE6":"7000","feeBps":10,"inDecimals":6,"outDecimals":9,"amount":"5000000","valueUsdc":"5000000","limits":{"minAmountPerCycle":null,"minValueUsdc":"5000000","minIntervalSecs":3600,"maxCycles":1000,"maxActive":10}}`},
		planted.Answer{Body: `{"ok":true,"owner":"O","positions":[],"next":null}`},
		planted.Answer{Body: `{"ok":true,"owner":"O","orders":[{"address":"A","inMint":"I","outMint":"M","cyclesDone":2,"cyclesTotal":2,"amountPerCycle":"1","intervalSecs":60,"cumIn":"2","cumOut":"3","minOutE6":"0","openedSlot":380000000,"openedSignature":null,"openedTs":null,"closedSlot":380000100,"closedSignature":null,"closedTs":null,"lastTry":null,"ended":"finished"}],"next":null}`},
		planted.Answer{Body: `{"ok":true,"address":"A","owner":"O","ownerIn":null,"ownerOut":null,"tracked":true,"fills":[],"openedSignature":null,"status":"active"}`},
		planted.Answer{Body: `{"ok":true,"address":"A","closed":true,"closedSlot":9,"openedSlot":8,"openedSignature":null,"status":"closed","statusCode":"closed","statusDetail":null,"lastTry":null,"fills":[]}`},
		planted.Answer{Body: `{"ok":true,"txn_base64":"AQ==","blockhash":"H","last_valid_block_height":"312000555","position":"P","cost":{},"summary":{}}`},
		planted.Answer{Status: 200, Body: `{"ok":false,"err":"rate_limited_open","detail":"all callers at the base rate"}`},
		planted.Answer{Status: 502, Body: ``},
	)
	c, _ := New(&Options{BaseURL: s.URL})
	ctx := context.Background()
	q, err := c.Quote(ctx, QuoteRequest{InputMint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", OutputMint: "So11111111111111111111111111111111111111112", Amount: 5000000})
	if err != nil || q.Amount != 5000000 || q.Limits.MinAmountPerCycle != nil || q.ValueUsdc != 5000000 {
		t.Fatalf("quote: %+v %v", q, err)
	}
	if ps, err := c.Positions(ctx, "O", ""); err != nil || ps.Owner != "O" {
		t.Fatalf("positions: %+v %v", ps, err)
	}
	if h, err := c.History(ctx, "O", ""); err != nil || h.Owner != "O" || h.Orders[0].OpenedSlot != 380000000 || h.Orders[0].OpenedSignature != nil {
		t.Fatalf("history: %+v %v", h, err)
	}
	if p, err := c.Position(ctx, "A", nil); err != nil || p.OwnerIn != nil || p.OwnerOut != nil || p.OpenedSignature != nil {
		t.Fatalf("position: %+v %v", p, err)
	}
	if p, err := c.Position(ctx, "A", nil); err != nil || !p.Closed || p.OpenedSignature != nil {
		t.Fatalf("closed record: %+v %v", p, err)
	}
	if _, err := c.Withdraw(ctx, WithdrawRequest{Owner: "O", Position: "P"}); err != nil {
		t.Fatal(err)
	}
	if body := string(s.Seen()[5].Body); body != `{"owner":"O","position":"P"}` {
		t.Fatalf("withdraw sent %s: amount is optional", body)
	}
	var e *carbium.Error
	if _, err := c.Health(ctx); !errors.As(err, &e) || e.ErrorCode != "rate_limited_open" || !e.Retryable || e.HTTPStatus != 200 {
		t.Fatalf("an ok:false refusal at HTTP 200: %+v", err)
	}
	if _, err := c.Health(ctx); !errors.As(err, &e) || e.Message != "HTTP 502" || !e.Retryable {
		t.Fatalf("an empty error body: %+v", err)
	}
}

// Send carries exactly the documented fields. Every SendRequest field is set, so one the API does not document would
// show in the body.
func TestSendCarriesOnlyTheDocumentedFields(t *testing.T) {
	s := planted.Start(t, planted.Answer{Body: `{"ok":true,"kind":"open","signature":"S","sent":true,"position":"P"}`})
	c, err := New(&Options{BaseURL: s.URL})
	if err != nil {
		t.Fatal(err)
	}
	var req SendRequest
	v := reflect.ValueOf(&req).Elem()
	for i := range v.NumField() {
		switch f := v.Field(i); f.Kind() {
		case reflect.String:
			f.SetString("AQ==")
		case reflect.Bool:
			f.SetBool(true)
		default:
			t.Fatalf("SendRequest.%s: no test value for a %s", v.Type().Field(i).Name, f.Kind())
		}
	}
	sent, err := c.Send(context.Background(), req)
	if err != nil || sent.Kind != "open" || sent.Signature != "S" || !sent.Sent || sent.Position != "P" {
		t.Fatal(sent, err)
	}
	seen := s.Seen()
	if len(seen) != 1 {
		t.Fatalf("%d sends, want 1", len(seen))
	}
	var body map[string]any
	if json.Unmarshal(seen[0].Body, &body) != nil || !reflect.DeepEqual(body, map[string]any{"txn_base64": "AQ==", "built_txn_base64": "AQ=="}) {
		t.Fatalf("send carried %s, want exactly txn_base64 and built_txn_base64", seen[0].Body)
	}
}
