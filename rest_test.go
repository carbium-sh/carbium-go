package carbium

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"
)

const cq1Answer = `{"srcAmountIn":"100000000","destAmountOut":"14213990","destAmountOutMin":"14143120","swapMode":"ExactIn","slippage":"50",
"priceImpactPct":"0.0001","routePlan":[{"swapInfo":{"ammKey":"AMM","label":"Whirlpool","inputMint":"So11111111111111111111111111111111111111112",
"outputMint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","inAmount":"100000000","outAmount":"14213990","feeAmount":"300","feeMint":"So11111111111111111111111111111111111111112"},
"percent":"100","bps":"10000","addedLater":"x"}],"feeMint":"So11111111111111111111111111111111111111112","feeBps":"0","signatureFeeLamports":"5000",
"prioritizationFeeLamports":"0","rentFeeLamports":"0","inputMint":"So11111111111111111111111111111111111111112",
"outputMint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","swapType":"aggregator","router":"cq1","requestId":"r-1","priceImpact":"0.01","newField":{"a":1}}`

var cq1Params = Cq1QuoteParams{SrcMint: "So11111111111111111111111111111111111111112", DstMint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", AmountIn: 100000000, SlippageBps: 50}

func TestCq1QuoteSendsThePathQueryAndKeyHeader(t *testing.T) {
	p := plant(t, reply{body: cq1Answer})
	c, _ := testClient(t, p, nil)
	q, err := c.Cq1Quote(context.Background(), cq1Params)
	if err != nil {
		t.Fatal(err)
	}
	if q.DestAmountOut != "14213990" || len(q.RoutePlan) != 1 || q.RoutePlan[0].SwapInfo.FeeAmount != "300" || q.RoutePlan[0].Bps != "10000" || q.Router != "cq1" || q.RequestID != "r-1" {
		t.Fatalf("%+v", q)
	}
	got := p.seen()[0]
	u, _ := url.Parse(got.path)
	want := url.Values{"src_mint": {cq1Params.SrcMint}, "dst_mint": {cq1Params.DstMint}, "amount_in": {"100000000"}, "slippage_bps": {"50"}}
	if got.verb != "GET" || u.Path != "/cq1/quote" || u.Query().Encode() != want.Encode() {
		t.Fatalf("sent %s %s", got.verb, got.path)
	}
	if got.header.Get("X-Api-Key") != testKey || strings.Contains(got.path, "c_serv_") || len(got.body) != 0 {
		t.Fatalf("the key must go in the header only: %s %v", got.path, got.header)
	}
}

func TestCq1QuoteAParameterFaultIsAPlainTextRequestError(t *testing.T) {
	text := "Failed to deserialize query string: missing field `dst_mint` near " + testKey + "Leaked"
	p := plant(t, reply{status: 400, body: text, header: map[string]string{"Content-Type": "text/plain; charset=utf-8"}})
	c, w := testClient(t, p, nil)
	_, err := c.Cq1Quote(context.Background(), cq1Params)
	e := asError(t, err)
	if e.Kind != KindRequest || e.HTTPStatus != 400 || len(p.seen()) != 1 || len(w.list()) != 0 {
		t.Fatalf("%d requests, %+v", len(p.seen()), e)
	}
	if !strings.HasPrefix(e.Message, "Failed to deserialize query string: missing field `dst_mint`") || strings.Contains(e.Message, testKey) || !strings.Contains(e.Message, "c_serv_…") {
		t.Fatalf("message %q", e.Message)
	}
}

func TestCq1QuoteRefusalsMapAsTheQuoteMethod(t *testing.T) {
	p := plant(t,
		reply{status: 429, body: rpcErr(-32005, "rate limit exceeded", "RATE_LIMITED", nil), header: map[string]string{"Retry-After": "1"}},
		reply{body: cq1Answer},
		reply{status: 401, body: rpcErr(-32001, "unknown api key", "UNKNOWN_KEY", nil)},
		reply{status: 403, body: rpcErr(-32050, "module not enabled", "MODULE_NOT_ENABLED", nil)},
	)
	c, w := testClient(t, p, nil)
	if _, err := c.Cq1Quote(context.Background(), cq1Params); err != nil {
		t.Fatal(err)
	}
	if len(p.seen()) != 2 || !sameWaits(w.list(), time.Second) {
		t.Fatalf("%d requests, waits %v: one 429 is waited out and retried once", len(p.seen()), w.list())
	}
	_, err := c.Cq1Quote(context.Background(), cq1Params)
	if e := asError(t, err); e.Kind != KindAuth || e.Code != -32001 || e.Reason != "UNKNOWN_KEY" || e.HTTPStatus != 401 {
		t.Fatalf("%+v", e)
	}
	_, err = c.Cq1Quote(context.Background(), cq1Params)
	if e := asError(t, err); e.Kind != KindAuth || e.Code != -32050 || e.HTTPStatus != 403 {
		t.Fatalf("%+v", e)
	}
	if n := len(p.seen()); n != 4 {
		t.Fatalf("%d requests, want 4: refusals are sent once", n)
	}
}

func TestCq1QuoteAPairWithNoRouteIsARouteErrorSentOnce(t *testing.T) {
	p := plant(t, reply{status: 422, body: "No valid routes found", header: map[string]string{"Content-Type": "text/plain"}})
	c, w := testClient(t, p, nil)
	_, err := c.Cq1Quote(context.Background(), cq1Params)
	e := asError(t, err)
	if e.Kind != KindRoute || e.HTTPStatus != 422 || e.Retryable || !strings.HasPrefix(e.Message, "No valid routes found") || len(p.seen()) != 1 || len(w.list()) != 0 {
		t.Fatalf("%d requests, %+v", len(p.seen()), e)
	}
}
