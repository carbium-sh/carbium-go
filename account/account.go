// Package account runs a Carbium account by API on carbium.sh: service keys and their policies, webhooks, usage
// and the account's records, with an account API key (c_acct_…, made on the dashboard; keep it in
// CARBIUM_ACCOUNT_KEY). Session holds a dashboard session for the calls an account key cannot make; the
// package-level functions are the sign-in steps, which need no credential.
//
// No call costs credits. The account API allows 600 calls per 10 minutes per key. Nothing here is retried, because
// creates are not safe to repeat: a 429 comes back with Error.RetryAfter set. Payments, plan changes and closing the
// account stay on the dashboard. Nothing here holds a private key: a wallet sign-in passes the signature the user's
// own wallet made.
package account

//go:generate go run ../internal/genaccount ../spec/account-openapi.json account_gen.go

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"time"

	carbium "github.com/carbium-sh/carbium-go"
	"github.com/carbium-sh/carbium-go/internal/httpx"
)

// DefaultBaseURL is where the account API answers.
const DefaultBaseURL = "https://carbium.sh"

// slowCall is the least wait of a call the service answers only after trying a delivery.
const slowCall = 25 * time.Second

// Options tune a client. The zero value is the default.
type Options struct {
	// BaseURL defaults to https://carbium.sh.
	BaseURL string
	// HTTPClient defaults to a client with no timeout of its own; Timeout bounds each call.
	HTTPClient *http.Client
	// Timeout bounds each call. Default 30 s.
	Timeout time.Duration
}

// Route is one call of this package and the route it uses.
type Route struct {
	Client, Group, Name, Verb, Path, Scope string
}

// Routes lists every call of this package.
func Routes() []Route { return append([]Route(nil), routes...) }

var accountKey = regexp.MustCompile(`^c_acct_[A-Za-z0-9]+$`)

// Client calls the account API with an account API key. It never prints its key.
type Client struct {
	Keys     *Keys
	Webhooks *Webhooks
	Usage    *Usage
	Account  *Records
	Modules  *Modules
	d        *httpx.Doer
	key      *carbium.Secret // behind a pointer, so a dereferenced client never prints it
}

// New makes a Client for an account API key (c_acct_…). A service key (c_serv_…) is refused: it is for
// services.carbium.sh.
func New(apiKey string, opts *Options) (*Client, error) {
	if !accountKey.MatchString(apiKey) {
		return nil, carbium.NewError(carbium.ErrorInit{Kind: carbium.KindAuth, Message: "the key must be an account API key (c_acct_…) from the dashboard; a service key (c_serv_) is for services.carbium.sh"})
	}
	d, err := doer(opts)
	if err != nil {
		return nil, err
	}
	key := carbium.Secret(apiKey)
	d.Auth = func(h http.Header) { h.Set("Authorization", "Bearer "+key.Reveal()) }
	c := &Client{d: d, key: &key}
	c.wire()
	return c, nil
}

// Format prints the client with its key as a placeholder.
func (c *Client) Format(f fmt.State, verb rune) {
	fmt.Fprintf(f, "account.Client{base: %s, key: %s}", c.d.Base, *c.key)
}

// Session is a dashboard session on carbium.sh, made by an e-mailed code or a wallet signature and held in memory
// only. It can make account API keys and show a service key's value once. It never prints its token.
type Session struct {
	d     *httpx.Doer
	token *carbium.Secret // behind a pointer, so a dereferenced session never prints it
}

// NewSession makes a Session from a session token, as PinVerify or WalletVerify answer it.
func NewSession(token carbium.Secret, opts *Options) (*Session, error) {
	if token == "" {
		return nil, carbium.NewError(carbium.ErrorInit{Kind: carbium.KindAuth, Message: "a session token is required"})
	}
	d, err := doer(opts)
	if err != nil {
		return nil, err
	}
	d.Auth = func(h http.Header) { h.Set("Authorization", "Bearer "+token.Reveal()) }
	return &Session{d: d, token: &token}, nil
}

// Format prints the session with its token as a placeholder.
func (s *Session) Format(f fmt.State, verb rune) {
	fmt.Fprintf(f, "account.Session{base: %s, token: %s}", s.d.Base, *s.token)
}

func doer(opts *Options) (*httpx.Doer, error) {
	if opts == nil {
		opts = &Options{}
	}
	return httpx.New(opts.BaseURL, DefaultBaseURL, opts.HTTPClient, opts.Timeout, carbium.KindAccount)
}

func public(opts *Options) (*httpx.Doer, error) { return doer(opts) }

// send makes one call and decodes its answer. An answer of 400 or above is an Error of kind account whose
// ErrorCode is the body's error string.
func send[T any](ctx context.Context, d *httpx.Doer, method, verb, path string, query url.Values, body any, timeout time.Duration) (T, error) {
	var out T
	a, err := d.Do(ctx, verb, path, query, body, timeout, method)
	if err != nil {
		return out, err
	}
	if a.Status >= 400 {
		return out, accountError(a, method)
	}
	if len(a.Body) == 0 || a.Status == http.StatusNoContent {
		return out, nil
	}
	if err := json.Unmarshal(a.Body, &out); err != nil {
		return out, carbium.NewError(carbium.ErrorInit{Kind: carbium.KindAccount, Method: method, HTTPStatus: a.Status, Message: "the answer does not fit its type: " + err.Error(),
			RequestID: a.Header.Get("X-Request-Id"), Cause: err})
	}
	return out, nil
}

func accountError(a *httpx.Answer, method string) error {
	obj := a.Object()
	code, _ := httpx.Str(obj, "error")
	msg, ok := httpx.Str(obj, "message")
	if !ok {
		msg = a.Text(200)
	}
	if msg == "" {
		msg = fmt.Sprintf("HTTP %d", a.Status)
	}
	in := carbium.ErrorInit{Kind: carbium.KindAccount, Method: method, HTTPStatus: a.Status, ErrorCode: code, Message: msg, Retryable: a.Status == http.StatusServiceUnavailable,
		RequestID: a.Header.Get("X-Request-Id")}
	if obj != nil {
		in.Data = a.Body
	}
	if hint, ok := carbium.RetryAfterHeader(a.Header); ok {
		in.RetryAfter = hint
	}
	return carbium.NewError(in)
}
