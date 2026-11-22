// Package tokens calls the four public routes of the token metadata API at tokens.carbium.io: search, lookup,
// batch and the image address. It is keyless and allows 300 requests a minute per client.
package tokens

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

// DefaultBaseURL is where the token API answers.
const DefaultBaseURL = "https://tokens.carbium.io"

// BatchMaxMints is the most mints in one Batch call, checked before it is sent.
const BatchMaxMints = 500

// routes holds every served route and the method that calls it.
var routes = map[string]string{"GET /tokens": "Search", "GET /tokens/:mint": "Lookup", "POST /tokens/batch": "Batch", "GET /img/:mint": "ImageURL"}

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
	// BaseURL defaults to https://tokens.carbium.io.
	BaseURL string
	// HTTPClient defaults to a client with no timeout of its own.
	HTTPClient *http.Client
	// Timeout bounds each call. Default 30 s.
	Timeout time.Duration
}

// Client calls the token API.
type Client struct{ d *httpx.Doer }

// New makes a Client.
func New(opts *Options) (*Client, error) {
	if opts == nil {
		opts = &Options{}
	}
	d, err := httpx.New(opts.BaseURL, DefaultBaseURL, opts.HTTPClient, opts.Timeout, carbium.KindTokens)
	if err != nil {
		return nil, err
	}
	return &Client{d: d}, nil
}

// Token is a token as the registry serves it. Times are Unix milliseconds; Supply is a decimal string.
type Token struct {
	Mint                 string                     `json:"mint"`
	Symbol               string                     `json:"symbol"` // "" until the mint is resolved
	Name                 string                     `json:"name"`
	Decimals             uint8                      `json:"decimals"`
	LogoURL              *string                    `json:"logo_url"`
	LogoStatus           string                     `json:"logo_status"` // ok, broken, broken_perm, missing or unchecked; another value is kept
	IsToken22            bool                       `json:"is_token22"`
	DailyVolumeUSD       *float64                   `json:"daily_volume_usd"`
	FirstSeenAt          int64                      `json:"first_seen_at"`
	LastSeenAt           int64                      `json:"last_seen_at"`
	Supply               *string                    `json:"supply"`
	MintAuthority        *string                    `json:"mint_authority"`
	FreezeAuthority      *string                    `json:"freeze_authority"`
	MetaplexURI          *string                    `json:"metaplex_uri"`
	UpdateAuthority      *string                    `json:"update_authority"`
	SellerFeeBasisPoints *uint16                    `json:"seller_fee_basis_points"`
	TokenStandard        *int                       `json:"token_standard"` // Metaplex TokenStandard
	Description          *string                    `json:"description"`
	ExternalURL          *string                    `json:"external_url"`
	Extensions           map[string]json.RawMessage `json:"extensions"`
	Tags                 []json.RawMessage          `json:"tags"`
}

// SearchOptions page a search. Limit is 1 to 200 (default 50); Page starts at 1.
type SearchOptions struct {
	Limit          int
	Page           int
	IncludeToken22 *bool
}

// SearchResult is one page of a search.
type SearchResult struct {
	Items []Token `json:"items"`
	Page  int     `json:"page"`
	Limit int     `json:"limit"`
	Total *int64  `json:"total"`
}

// BatchResult holds the known tokens and the mints the registry does not hold yet (queued for enrichment).
type BatchResult struct {
	Items   []Token  `json:"items"`
	Unknown []string `json:"unknown"`
}

func tokensError(a *httpx.Answer, method string) error {
	obj := a.Object()
	code, _ := httpx.Str(obj, "error")
	msg, ok := httpx.Str(obj, "message")
	if !ok {
		msg = code
	}
	if msg == "" {
		msg = a.Text(200)
	}
	if msg == "" {
		msg = fmt.Sprintf("HTTP %d", a.Status)
	}
	in := carbium.ErrorInit{Kind: carbium.KindTokens, Method: method, HTTPStatus: a.Status, ErrorCode: code, Message: msg, Retryable: a.Status == 429 || a.Status == 503}
	if obj != nil {
		in.Data = a.Body
	}
	if hint, ok := carbium.RetryAfterHeader(a.Header); ok {
		in.RetryAfter = hint
	} else if raw, ok := obj["retryAfterSec"]; ok {
		if sec, err := strconv.ParseFloat(string(raw), 64); err == nil && sec >= 0 {
			in.RetryAfter = time.Duration(sec * float64(time.Second))
		}
	}
	return carbium.NewError(in)
}

func get[T any](ctx context.Context, c *Client, method, path string, q url.Values, body any) (*T, error) {
	verb := http.MethodGet
	if body != nil {
		verb = http.MethodPost
	}
	a, err := c.d.Do(ctx, verb, path, q, body, 0, method)
	if err != nil {
		return nil, err
	}
	if a.Status >= 400 {
		return nil, tokensError(a, method)
	}
	var out T
	if err := json.Unmarshal(a.Body, &out); err != nil {
		return nil, carbium.NewError(carbium.ErrorInit{Kind: carbium.KindTokens, Method: method, HTTPStatus: a.Status, Message: "the answer does not fit its type: " + err.Error(), Cause: err})
	}
	return &out, nil
}

// Lookup calls GET /tokens/:mint. It returns nil, nil when the mint is not indexed yet (404).
func (c *Client) Lookup(ctx context.Context, mint string) (*Token, error) {
	a, err := c.d.Do(ctx, http.MethodGet, "/tokens/"+url.PathEscape(mint), nil, nil, 0, "lookup")
	if err != nil {
		return nil, err
	}
	if a.Status == http.StatusNotFound {
		return nil, nil
	}
	if a.Status >= 400 {
		return nil, tokensError(a, "lookup")
	}
	var t Token
	if err := json.Unmarshal(a.Body, &t); err != nil {
		return nil, carbium.NewError(carbium.ErrorInit{Kind: carbium.KindTokens, Method: "lookup", HTTPStatus: a.Status, Message: "the answer does not fit its type: " + err.Error(), Cause: err})
	}
	return &t, nil
}

// Search calls GET /tokens?q=: a ranked search, or the top listing by volume when q is empty.
func (c *Client) Search(ctx context.Context, q string, opts *SearchOptions) (*SearchResult, error) {
	v := url.Values{"q": {q}}
	if opts != nil {
		if opts.Limit > 0 {
			v.Set("limit", strconv.Itoa(opts.Limit))
		}
		if opts.Page > 0 {
			v.Set("page", strconv.Itoa(opts.Page))
		}
		if opts.IncludeToken22 != nil {
			v.Set("include_token22", strconv.FormatBool(*opts.IncludeToken22))
		}
	}
	return get[SearchResult](ctx, c, "search", "/tokens", v, nil)
}

// Batch calls POST /tokens/batch with 1 to 500 mints. Unknown mints come back in Unknown.
func (c *Client) Batch(ctx context.Context, mints []string) (*BatchResult, error) {
	if len(mints) == 0 || len(mints) > BatchMaxMints {
		return nil, carbium.NewError(carbium.ErrorInit{Kind: carbium.KindTokens, Method: "batch", Message: fmt.Sprintf("batch takes 1 to %d mints, not %d", BatchMaxMints, len(mints))})
	}
	return get[BatchResult](ctx, c, "batch", "/tokens/batch", nil, map[string]any{"mints": mints})
}

// ImageURL is the URL of GET /img/:mint, for an image tag. It makes no request; the route always answers 200, with
// an SVG fallback when no logo is known.
func (c *Client) ImageURL(mint string) string { return c.d.Base + "/img/" + url.PathEscape(mint) }
