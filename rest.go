package carbium

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// restRoutes holds every served REST route on the services endpoint and the method on *Client that calls it.
var restRoutes = map[string]string{"GET /cq1/quote": "Cq1Quote"}

// RESTRoutes lists every served REST route on the services endpoint, sorted.
func RESTRoutes() []string {
	out := make([]string, 0, len(restRoutes))
	for r := range restRoutes {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

// Cq1QuoteParams are the four query parameters of GET /cq1/quote, all required.
type Cq1QuoteParams struct {
	// SrcMint and DstMint are base58 mint addresses.
	SrcMint string
	DstMint string
	// AmountIn is in the source mint's base units, sent as decimal digits.
	AmountIn uint64
	// SlippageBps is the slippage in basis points.
	SlippageBps uint64
}

// Cq1Quote is the answer of GET /cq1/quote: one JSON object, every value a string. Keys not listed here are
// dropped, so a field the route adds later does not break the call.
type Cq1Quote struct {
	SrcAmountIn               string        `json:"srcAmountIn"`
	DestAmountOut             string        `json:"destAmountOut"`
	DestAmountOutMin          string        `json:"destAmountOutMin"`
	SwapMode                  string        `json:"swapMode"`
	Slippage                  string        `json:"slippage"`
	PriceImpactPct            string        `json:"priceImpactPct"`
	RoutePlan                 []Cq1RouteLeg `json:"routePlan"`
	FeeMint                   string        `json:"feeMint"`
	FeeBps                    string        `json:"feeBps"`
	SignatureFeeLamports      string        `json:"signatureFeeLamports"`
	PrioritizationFeeLamports string        `json:"prioritizationFeeLamports"`
	RentFeeLamports           string        `json:"rentFeeLamports"`
	InputMint                 string        `json:"inputMint"`
	OutputMint                string        `json:"outputMint"`
	SwapType                  string        `json:"swapType"`
	Router                    string        `json:"router"`
	RequestID                 string        `json:"requestId"`
	PriceImpact               string        `json:"priceImpact"`
}

// Cq1RouteLeg is one leg of a Cq1Quote route.
type Cq1RouteLeg struct {
	SwapInfo Cq1SwapInfo `json:"swapInfo"`
	Percent  string      `json:"percent"`
	Bps      string      `json:"bps"`
}

// Cq1SwapInfo is the pool a Cq1RouteLeg trades through.
type Cq1SwapInfo struct {
	AmmKey     string `json:"ammKey"`
	Label      string `json:"label"`
	InputMint  string `json:"inputMint"`
	OutputMint string `json:"outputMint"`
	InAmount   string `json:"inAmount"`
	OutAmount  string `json:"outAmount"`
	FeeAmount  string `json:"feeAmount"`
	FeeMint    string `json:"feeMint"`
}

// Cq1Quote calls GET /cq1/quote, the REST form of the quote. It is admitted as the quote method (same scope, plan
// and rate) and follows the same retry rules. A parameter fault comes back as kind request with the service's
// text as the message; a pair with no route (HTTP 422) is kind route. Neither is retried.
func (cl *Client) Cq1Quote(ctx context.Context, params Cq1QuoteParams) (*Cq1Quote, error) {
	const route = "GET /cq1/quote"
	if err := cl.check(route); err != nil {
		return nil, err
	}
	if params.SrcMint == "" || params.DstMint == "" {
		return nil, NewError(ErrorInit{Kind: KindRequest, Method: route, Message: "src_mint and dst_mint are both required"})
	}
	q := url.Values{}
	q.Set("src_mint", params.SrcMint)
	q.Set("dst_mint", params.DstMint)
	q.Set("amount_in", strconv.FormatUint(params.AmountIn, 10))
	q.Set("slippage_bps", strconv.FormatUint(params.SlippageBps, 10))
	target := strings.TrimRight(cl.c.endpoint, "/") + "/cq1/quote?" + q.Encode()
	a, err := cl.c.send(ctx, http.MethodGet, target, nil, route, modeREST, nil)
	if err != nil {
		var e *Error
		if errors.As(err, &e) && e.HTTPStatus == http.StatusUnprocessableEntity {
			e.Kind = KindRoute // a pair with no route: quote another pair or amount
		}
		return nil, err
	}
	var out Cq1Quote
	if err := json.Unmarshal(a.body, &out); err != nil {
		return nil, NewError(ErrorInit{Kind: KindUpstream, Method: route, HTTPStatus: a.status, Message: "the " + route + " answer does not fit its type: " + err.Error(),
			RequestID: a.header.Get("X-Request-Id"), Cause: err})
	}
	return &out, nil
}
