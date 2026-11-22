// Package dca calls the DCA API on the dex (dex.carbium.io/api/dca). Reads are keyless. The four builders return
// one unsigned transaction for the owner's own wallet to sign; Send relays bytes the owner already signed, once.
// Nothing here signs, holds a private key or repeats a call. Server side only: a browser page is refused with
// forbidden_origin.
package dca

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	carbium "github.com/carbium-sh/carbium-go"
	"github.com/carbium-sh/carbium-go/internal/httpx"
)

// DefaultBaseURL is where the DCA API answers.
const DefaultBaseURL = "https://dex.carbium.io/api/dca"

// routes holds every served route and the method that calls it.
var routes = map[string]string{
	"GET /quote": "Quote", "GET /positions": "Positions", "GET /history": "History", "GET /position/{address}": "Position",
	"GET /health": "Health", "POST /open": "Open", "POST /update": "Update", "POST /withdraw": "Withdraw", "POST /close": "Close",
	"POST /send": "Send",
}

// Routes lists every route this package calls, with the method on *Client that calls it.
func Routes() map[string]string {
	out := make(map[string]string, len(routes))
	for k, v := range routes {
		out[k] = v
	}
	return out
}

// Options tune a Client. The zero value is the default.
type Options struct {
	// BaseURL defaults to https://dex.carbium.io/api/dca.
	BaseURL string
	// APIKey is optional: a DCA API key made in the dex account menu raises the rate limits. It is not a Carbium
	// service or account key. It is sent in x-api-key and never printed.
	APIKey string
	// HTTPClient defaults to a client with no timeout of its own.
	HTTPClient *http.Client
	// Timeout bounds each call. Default 30 s.
	Timeout time.Duration
}

// Client calls the DCA API.
type Client struct {
	d   *httpx.Doer
	key *carbium.Secret // behind a pointer, so a dereferenced client never prints it
}

// New makes a Client.
func New(opts *Options) (*Client, error) {
	if opts == nil {
		opts = &Options{}
	}
	d, err := httpx.New(opts.BaseURL, DefaultBaseURL, opts.HTTPClient, opts.Timeout, carbium.KindDCA)
	if err != nil {
		return nil, err
	}
	key := carbium.Secret(opts.APIKey)
	if key != "" {
		d.Auth = func(h http.Header) { h.Set("X-Api-Key", key.Reveal()) }
	}
	return &Client{d: d, key: &key}, nil
}

// Format prints the client with its key as a placeholder.
func (c *Client) Format(f fmt.State, verb rune) {
	fmt.Fprintf(f, "dca.Client{base: %s, key: %s}", c.d.Base, *c.key)
}

// U64 is an unsigned 64-bit amount. It is sent as a decimal string and read from a string or a number.
type U64 uint64

func (u U64) MarshalJSON() ([]byte, error) {
	return []byte(`"` + strconv.FormatUint(uint64(u), 10) + `"`), nil
}

func (u *U64) UnmarshalJSON(b []byte) error {
	s := string(b)
	if s == "null" {
		return nil
	}
	if len(s) >= 2 && s[0] == '"' {
		s = s[1 : len(s)-1]
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return fmt.Errorf("dca: %q is not a u64", s)
	}
	*u = U64(n)
	return nil
}

// QuoteRequest asks for the price of one buy.
type QuoteRequest struct {
	InputMint   string
	OutputMint  string
	Amount      U64
	SlippageBps *uint32
	Owner       string
}

// OpenRequest builds an open. Set exactly one of AmountPerCycle and TotalAmount, and exactly one of IntervalSecs
// and Frequency (hourly, daily, weekly, biweekly or monthly).
type OpenRequest struct {
	Owner          string  `json:"owner"`
	InputMint      string  `json:"inputMint"`
	OutputMint     string  `json:"outputMint"`
	Cycles         uint32  `json:"cycles"`
	AmountPerCycle *U64    `json:"amountPerCycle,omitempty"`
	TotalAmount    *U64    `json:"totalAmount,omitempty"`
	IntervalSecs   *uint64 `json:"intervalSecs,omitempty"`
	Frequency      string  `json:"frequency,omitempty"`
	// MaxSlippageBps defaults to 50, cap 300.
	MaxSlippageBps *uint32 `json:"maxSlippageBps,omitempty"`
	// StartAt is 0 or a Unix time up to 30 days ahead.
	StartAt *int64 `json:"startAt,omitempty"`
	// Index is 0 to 255, for several positions on one pair.
	Index *uint8 `json:"index,omitempty"`
	// MinOutE6 is a floor on the output per 1e6 of input; MaxPrice a maximum price. At most one of them.
	MinOutE6 *U64 `json:"minOutE6,omitempty"`
	MaxPrice *U64 `json:"maxPrice,omitempty"`
}

// UpdateRequest builds a pause, resume or new floor. ClearMaxPrice sends maxPrice null, which removes it.
type UpdateRequest struct {
	Owner          string
	Position       string
	Paused         *bool
	MinOutE6       *U64
	MaxPrice       *U64
	ClearMaxPrice  bool
	MaxSlippageBps *uint32
}

func (r UpdateRequest) MarshalJSON() ([]byte, error) {
	m := map[string]any{"owner": r.Owner, "position": r.Position}
	if r.Paused != nil {
		m["paused"] = *r.Paused
	}
	if r.MinOutE6 != nil {
		m["minOutE6"] = *r.MinOutE6
	}
	if r.ClearMaxPrice {
		m["maxPrice"] = nil
	} else if r.MaxPrice != nil {
		m["maxPrice"] = *r.MaxPrice
	}
	if r.MaxSlippageBps != nil {
		m["maxSlippageBps"] = *r.MaxSlippageBps
	}
	return json.Marshal(m)
}

// WithdrawRequest builds a withdraw; Amount 0 withdraws the whole escrow.
type WithdrawRequest struct {
	Owner    string `json:"owner"`
	Position string `json:"position"`
	Amount   *U64   `json:"amount,omitempty"` // 0 or nil withdraws the whole escrow
}

// CloseRequest builds a close.
type CloseRequest struct {
	Owner    string `json:"owner"`
	Position string `json:"position"`
}

// SendRequest relays a transaction this API built, once the owner's own wallet signed it.
type SendRequest struct {
	// TxnBase64 is the signed transaction, base64.
	TxnBase64 string `json:"txn_base64"`
	// BuiltTxnBase64 is the build answer's txn_base64, when the wallet changed the message.
	BuiltTxnBase64 string `json:"built_txn_base64,omitempty"`
}

// Build is a built, unsigned v0 transaction for the owner to sign and send.
type Build struct {
	TxnBase64            string                     `json:"txn_base64"`
	Blockhash            string                     `json:"blockhash"`
	LastValidBlockHeight U64                        `json:"last_valid_block_height"`
	Position             string                     `json:"position"`
	Cost                 map[string]json.RawMessage `json:"cost"`
	Summary              map[string]json.RawMessage `json:"summary"`
}

// OpenBuild is the open's Build with the new position's accounts.
type OpenBuild struct {
	Build
	Authority     string                     `json:"authority"`
	EscrowAccount string                     `json:"escrowAccount"`
	Index         uint8                      `json:"index"`
	Plan          map[string]json.RawMessage `json:"plan"`
}

// Sent is the answer of Send.
type Sent struct {
	Kind      string `json:"kind"`
	Signature string `json:"signature"`
	Sent      bool   `json:"sent"`
	Position  string `json:"position"`
}

// Quote is the price of one buy, the fee rate and the owner's limits.
type Quote struct {
	Out         string `json:"out"`
	MinOut      string `json:"minOut"`
	RateE6      string `json:"rateE6"`
	FeeBps      uint32 `json:"feeBps"`
	InDecimals  uint8  `json:"inDecimals"`
	OutDecimals uint8  `json:"outDecimals"`
	Amount      U64    `json:"amount"`
	ValueUsdc   U64    `json:"valueUsdc"` // USDC base units
	Limits      struct {
		MinAmountPerCycle *string `json:"minAmountPerCycle"`
		MinValueUsdc      U64     `json:"minValueUsdc"`
		MinIntervalSecs   uint64  `json:"minIntervalSecs"`
		MaxCycles         uint32  `json:"maxCycles"`
		MaxActive         uint32  `json:"maxActive"`
	} `json:"limits"`
	Cost *struct {
		RequiredLamports U64 `json:"requiredLamports"`
		HeldLamports     U64 `json:"heldLamports"`
	} `json:"cost,omitempty"`
}

// Buy is one fill or miss of a position.
type Buy struct {
	Signature  string  `json:"signature"`
	Cycle      uint32  `json:"cycle"`
	InAmount   string  `json:"inAmount"`
	OutAmount  string  `json:"outAmount"`
	Ts         string  `json:"ts"` // Unix seconds, as a decimal string
	Slot       *uint64 `json:"slot"`
	OpenedSlot uint64  `json:"openedSlot"`
	Verdict    string  `json:"verdict"` // filled or missed
	Detail     *string `json:"detail"`
}

// Position is one open position. Status is active, waiting, paused, not_served, empty, finished or closed.
type Position struct {
	Address         string          `json:"address"`
	Owner           string          `json:"owner"`
	InMint          string          `json:"inMint"`
	OutMint         string          `json:"outMint"`
	Crank           string          `json:"crank"`
	InProgram       string          `json:"inProgram"`
	OutProgram      string          `json:"outProgram"`
	Index           uint8           `json:"index"`
	MaxSlippageBps  uint32          `json:"maxSlippageBps"`
	CyclesTotal     uint32          `json:"cyclesTotal"`
	CyclesDone      uint32          `json:"cyclesDone"`
	IntervalSecs    uint64          `json:"intervalSecs"`
	AmountPerCycle  string          `json:"amountPerCycle"`
	MinOutE6        string          `json:"minOutE6"`
	CumIn           string          `json:"cumIn"`
	CumOut          string          `json:"cumOut"`
	NextTs          string          `json:"nextTs"`
	Active          bool            `json:"active"`
	Escrow          *string         `json:"escrow"`
	MiddleDebt      json.RawMessage `json:"middleDebt"`
	Status          string          `json:"status"`
	StatusCode      string          `json:"statusCode"`
	StatusDetail    *string         `json:"statusDetail"`
	LastTry         json.RawMessage `json:"lastTry"`
	OpenedSlot      *uint64         `json:"openedSlot"`
	OpenedSignature *string         `json:"openedSignature"`
	Served          *bool           `json:"served"`
	NotServed       *string         `json:"notServed"`
}

// PositionDetail is one position with its fills, or its closed record when Closed is true.
type PositionDetail struct {
	Position
	OwnerIn    *string `json:"ownerIn"`
	OwnerOut   *string `json:"ownerOut"`
	Tracked    bool    `json:"tracked"`
	Fills      []Buy   `json:"fills"`
	Closed     bool    `json:"closed"`
	ClosedSlot *uint64 `json:"closedSlot,omitempty"`
}

// Positions is a page of open positions; pass Next back as after for the rest.
type Positions struct {
	Owner     string     `json:"owner"`
	Positions []Position `json:"positions"`
	Next      *string    `json:"next"`
}

// HistoryOrder is an ended DCA as /history lists it. Ended is finished, emptied or closed_early.
type HistoryOrder struct {
	Address         string          `json:"address"`
	InMint          string          `json:"inMint"`
	OutMint         string          `json:"outMint"`
	CyclesDone      uint32          `json:"cyclesDone"`
	CyclesTotal     uint32          `json:"cyclesTotal"`
	AmountPerCycle  string          `json:"amountPerCycle"`
	IntervalSecs    uint64          `json:"intervalSecs"`
	CumIn           string          `json:"cumIn"`
	CumOut          string          `json:"cumOut"`
	MinOutE6        string          `json:"minOutE6"`
	OpenedSlot      uint64          `json:"openedSlot"`
	OpenedSignature *string         `json:"openedSignature"`
	OpenedTs        *string         `json:"openedTs"`
	ClosedSlot      uint64          `json:"closedSlot"`
	ClosedSignature *string         `json:"closedSignature"`
	ClosedTs        *string         `json:"closedTs"`
	LastTry         json.RawMessage `json:"lastTry"`
	Ended           string          `json:"ended"`
}

// History is a page of ended DCAs, newest close first.
type History struct {
	Owner  string         `json:"owner"`
	Orders []HistoryOrder `json:"orders"`
	Next   *string        `json:"next"`
}

// Health is the keeper's state.
type Health struct {
	OK     bool   `json:"ok"`
	Keeper string `json:"keeper"`
}

func call[T any](ctx context.Context, c *Client, method, verb, path string, q url.Values, body any) (*T, error) {
	a, err := c.d.Do(ctx, verb, path, q, body, 0, method)
	if err != nil {
		return nil, err
	}
	obj := a.Object()
	refused := obj == nil
	if raw, ok := obj["ok"]; ok && string(raw) == "false" {
		refused = true
	}
	if a.Status >= 400 || refused {
		return nil, dcaError(a, obj, method)
	}
	var out T
	if err := json.Unmarshal(a.Body, &out); err != nil {
		return nil, carbium.NewError(carbium.ErrorInit{Kind: carbium.KindDCA, Method: method, HTTPStatus: a.Status, Message: "the answer does not fit its type: " + err.Error(), Cause: err})
	}
	return &out, nil
}

func dcaError(a *httpx.Answer, obj map[string]json.RawMessage, method string) error {
	code, _ := httpx.Str(obj, "err")
	msg, ok := httpx.Str(obj, "detail")
	if !ok {
		msg = code
	}
	if msg == "" {
		msg = a.Text(200)
	}
	if msg == "" {
		msg = fmt.Sprintf("HTTP %d", a.Status)
	}
	s := a.Status
	in := carbium.ErrorInit{Kind: carbium.KindDCA, Method: method, HTTPStatus: s, ErrorCode: code, Message: msg,
		Retryable: s == 429 || s == 502 || s == 503 || s == 504 || retryableCodes[code]}
	if obj != nil {
		in.Data = a.Body
	}
	if hint, ok := carbium.RetryAfterHeader(a.Header); ok {
		in.RetryAfter = hint
	}
	return carbium.NewError(in)
}

// retryableCodes are retryable whatever the status: a refusal can arrive with HTTP 200 and ok false.
var retryableCodes = map[string]bool{"rate_limited": true, "rate_limited_open": true, "rpc_unavailable": true, "quote_unavailable": true, "door_busy": true}

func refuse(method, message string) error {
	return carbium.NewError(carbium.ErrorInit{Kind: carbium.KindDCA, Method: method, Message: message})
}

// Quote calls GET /quote: the price of one buy, the fee rate and the owner's limits.
func (c *Client) Quote(ctx context.Context, req QuoteRequest) (*Quote, error) {
	q := url.Values{"inputMint": {req.InputMint}, "outputMint": {req.OutputMint}, "amount": {strconv.FormatUint(uint64(req.Amount), 10)}}
	if req.SlippageBps != nil {
		q.Set("slippageBps", strconv.FormatUint(uint64(*req.SlippageBps), 10))
	}
	if req.Owner != "" {
		q.Set("owner", req.Owner)
	}
	return call[Quote](ctx, c, "quote", http.MethodGet, "/quote", q, nil)
}

// Positions calls GET /positions: the owner's open positions, at most 100; pass Next back as after.
func (c *Client) Positions(ctx context.Context, owner, after string) (*Positions, error) {
	q := url.Values{"owner": {owner}}
	if after != "" {
		q.Set("after", after)
	}
	return call[Positions](ctx, c, "positions", http.MethodGet, "/positions", q, nil)
}

// History calls GET /history: the owner's ended DCAs, newest close first.
func (c *Client) History(ctx context.Context, owner, after string) (*History, error) {
	q := url.Values{"owner": {owner}}
	if after != "" {
		q.Set("after", after)
	}
	return call[History](ctx, c, "history", http.MethodGet, "/history", q, nil)
}

// Position calls GET /position/{address}: one position with its fills, or its closed record. openedSlot may be nil.
func (c *Client) Position(ctx context.Context, address string, openedSlot *uint64) (*PositionDetail, error) {
	var q url.Values
	if openedSlot != nil {
		q = url.Values{"openedSlot": {strconv.FormatUint(*openedSlot, 10)}}
	}
	return call[PositionDetail](ctx, c, "position", http.MethodGet, "/position/"+url.PathEscape(address), q, nil)
}

// Health calls GET /health: the keeper's state. Never metered.
func (c *Client) Health(ctx context.Context) (*Health, error) {
	return call[Health](ctx, c, "health", http.MethodGet, "/health", nil, nil)
}

// Open calls POST /open: builds the unsigned open transaction.
func (c *Client) Open(ctx context.Context, req OpenRequest) (*OpenBuild, error) {
	if (req.AmountPerCycle == nil) == (req.TotalAmount == nil) {
		return nil, refuse("open", "set exactly one of AmountPerCycle and TotalAmount")
	}
	if (req.IntervalSecs == nil) == (req.Frequency == "") {
		return nil, refuse("open", "set exactly one of IntervalSecs and Frequency")
	}
	if req.MinOutE6 != nil && req.MaxPrice != nil {
		return nil, refuse("open", "MinOutE6 and MaxPrice exclude each other")
	}
	return call[OpenBuild](ctx, c, "open", http.MethodPost, "/open", nil, req)
}

// Update calls POST /update: builds the unsigned pause, resume or new-floor transaction.
func (c *Client) Update(ctx context.Context, req UpdateRequest) (*Build, error) {
	return call[Build](ctx, c, "update", http.MethodPost, "/update", nil, req)
}

// Withdraw calls POST /withdraw: builds the unsigned withdraw transaction.
func (c *Client) Withdraw(ctx context.Context, req WithdrawRequest) (*Build, error) {
	return call[Build](ctx, c, "withdraw", http.MethodPost, "/withdraw", nil, req)
}

// Close calls POST /close: builds the unsigned close transaction.
func (c *Client) Close(ctx context.Context, req CloseRequest) (*Build, error) {
	return call[Build](ctx, c, "close", http.MethodPost, "/close", nil, req)
}

// Send calls POST /send: relays a transaction this API built, once the owner's own wallet signed it. It is sent
// once and never retried.
func (c *Client) Send(ctx context.Context, req SendRequest) (*Sent, error) {
	return call[Sent](ctx, c, "send", http.MethodPost, "/send", nil, req)
}
