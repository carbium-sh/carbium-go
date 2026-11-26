package carbium

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// Version is this module's version, sent in the solana-client header.
const Version = "0.1.1"

// DefaultEndpoint is where the client sends calls unless Options.Endpoint says otherwise.
const DefaultEndpoint = "https://services.carbium.sh"

// Wire limits of services.carbium.sh, checked before a request is sent.
const (
	// MaxBodyBytes is the largest request body; a bigger one is refused with 413.
	MaxBodyBytes = 51200
	// MaxBatch is the most calls in one JSON-RPC batch.
	MaxBatch = 100
	// MaxPermitPerBatch is the most methods holding a node slot (MethodSpec.Permit) in one batch.
	MaxPermitPerBatch = 2
)

// Options tune a Client. The zero value is the default.
type Options struct {
	// Endpoint defaults to https://services.carbium.sh. A key in the URL is refused: it goes in the x-api-key header.
	Endpoint string
	// HTTPClient defaults to a client with no timeout of its own; Timeout bounds each attempt. The client uses a
	// copy that never follows a redirect.
	HTTPClient *http.Client
	// Timeout bounds each attempt. Default 100 s, above the service's own 95 s gateway deadline.
	Timeout time.Duration
	// MaxAttempts caps the calls made for one request, the first included. 0 keeps each retry rule's own count
	// (4 for most, 6 for a scan index behind); 1 turns retries off. It can only lower a rule's count.
	MaxAttempts int
	// MaxHint is the longest server wait hint the client waits for; a longer one is returned at once in
	// Error.RetryAfter. Default 30 s.
	MaxHint time.Duration
}

// Client calls services.carbium.sh: every served JSON-RPC method (Solana RPC, DAS and the quote plane), with the
// published retry rules. It is safe for concurrent use. Make it with NewClient: the zero value refuses every call. It never prints its key: %v, %+v and %#v show a placeholder.
type Client struct {
	c *core
}

type core struct {
	endpoint    string
	key         *secret
	http        *http.Client
	timeout     time.Duration
	maxAttempts int
	maxHint     time.Duration
	nextID      atomic.Uint64

	// Swappable for tests: the wait between attempts, its jitter and the clock.
	sleep  func(context.Context, time.Duration) error
	jitter func() time.Duration
	now    func() time.Time
}

// secret holds an API key and never prints it.
type secret struct{ v string }

func (s *secret) String() string {
	if s == nil || s.v == "" {
		return "none"
	}
	if i := strings.Index(s.v, "_"); i >= 0 {
		if j := strings.Index(s.v[i+1:], "_"); j >= 0 {
			return s.v[:i+j+2] + "…"
		}
	}
	return "…"
}

func (s *secret) GoString() string              { return s.String() }
func (s *secret) Format(f fmt.State, verb rune) { fmt.Fprint(f, s.String()) }
func (s *secret) MarshalJSON() ([]byte, error)  { return json.Marshal(s.String()) }
func (s *secret) MarshalText() ([]byte, error)  { return []byte(s.String()), nil }

var (
	serviceKey = regexp.MustCompile(`^c_serv_[A-Za-z0-9]+$`)
	keyInURL   = regexp.MustCompile(`(?i)(api[-_]?key|apikey|token)=|c_serv_|c_acct_`)
)

// CheckEndpoint refuses an endpoint that carries a key or is not an http, https, ws or wss URL. The sub-packages
// use it; the endpoint itself is never echoed, since it may hold a key.
func CheckEndpoint(endpoint string, schemes ...string) error {
	if keyInURL.MatchString(endpoint) {
		return NewError(ErrorInit{Kind: KindRequest, Message: "put the key in the key argument, not in the URL: a key in a URL ends up in logs"})
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || !contains(schemes, u.Scheme) {
		return NewError(ErrorInit{Kind: KindRequest, Message: "the endpoint must be an absolute " + strings.Join(schemes, " or ") + " URL"})
	}
	if u.User != nil {
		return NewError(ErrorInit{Kind: KindRequest, Message: "the endpoint must not carry a user or password"})
	}
	return nil
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// NewClient makes a Client for a service key (c_serv_…, shown once on the dashboard). An empty key makes a keyless
// client: the service answers its twelve public methods, 30 calls a minute per IP address, and refuses every other
// call with 401. An account key (c_acct_…) is refused: it is for package account.
func NewClient(apiKey string, opts *Options) (*Client, error) {
	if opts == nil {
		opts = &Options{}
	}
	if apiKey != "" && !serviceKey.MatchString(apiKey) {
		return nil, NewError(ErrorInit{Kind: KindAuth, Message: "the key must be the full service key shown once on the dashboard (it starts with c_serv_); an account key (c_acct_) is for package account"})
	}
	endpoint := opts.Endpoint
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}
	if err := CheckEndpoint(endpoint, "https", "http"); err != nil {
		return nil, err
	}
	c := &core{endpoint: endpoint, key: &secret{apiKey}, http: opts.HTTPClient, timeout: opts.Timeout, maxAttempts: opts.MaxAttempts, maxHint: opts.MaxHint,
		sleep: sleepContext, jitter: func() time.Duration { return time.Duration(rand.IntN(251)) * time.Millisecond }, now: time.Now}
	c.http = NoRedirects(c.http)
	if c.timeout <= 0 {
		c.timeout = 100 * time.Second
	}
	if c.maxHint <= 0 {
		c.maxHint = 30 * time.Second
	}
	if c.maxAttempts < 0 {
		c.maxAttempts = 0
	}
	return &Client{c: c}, nil
}

// Endpoint is the URL the client sends calls to.
func (cl *Client) Endpoint() string {
	if cl == nil || cl.c == nil {
		return ""
	}
	return cl.c.endpoint
}

// Format prints the client with every verb, with the key as a placeholder.
func (cl *Client) Format(f fmt.State, verb rune) {
	if cl == nil || cl.c == nil {
		fmt.Fprint(f, "carbium.Client(nil)")
		return
	}
	fmt.Fprintf(f, "carbium.Client{endpoint: %s, key: %s}", cl.c.endpoint, cl.c.key.String())
}

func (cl *Client) String() string { return fmt.Sprint(cl) }

func (cl *Client) GoString() string { return fmt.Sprint(cl) }

// NoRedirects returns a copy of hc (a new client when nil) that never follows a redirect: a 3xx comes back as the
// answer, so a key header never reaches another host. Every client in this module uses it.
func NoRedirects(hc *http.Client) *http.Client {
	c := &http.Client{}
	if hc != nil {
		*c = *hc
	}
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return c
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return context.Cause(ctx)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.C:
		return nil
	}
}

// MethodSpec describes one served JSON-RPC method.
type MethodSpec struct {
	// Plane is the part of the service that answers: "rpc", "das" or "quote".
	Plane string
	// ByName is true when params are one object, false when they are a positional array.
	ByName bool
	// Batch says whether the method may share a batch: "any", "das" (only with other DAS methods) or "never".
	Batch string
	// Permit is true for a method that holds a node slot; a batch holds at most MaxPermitPerBatch of them.
	Permit bool
	// GoName is the typed method on *Client.
	GoName string
}

// LookupMethod returns the spec of a served method; ok is false for any name the services do not serve.
func LookupMethod(name string) (MethodSpec, bool) {
	m, ok := methodTable[name]
	return m, ok
}

// Methods lists every served JSON-RPC method, sorted.
func Methods() []string {
	out := make([]string, 0, len(methodTable))
	for name := range methodTable {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Subscriptions lists every served WebSocket subscription, sorted.
func Subscriptions() []string { return append([]string(nil), subscriptionNames...) }

// WithContext is an answer that carries the slot it was read at.
type WithContext[T any] struct {
	Context Context `json:"context"`
	Value   T       `json:"value"`
}

// Ptr returns a pointer to v, for the optional numbers and flags of a config.
func Ptr[T any](v T) *T { return &v }

// positional builds positional params without the trailing ones the caller left out. A nil in the middle is sent
// as null. Nothing left means no params at all.
func positional(args ...any) any {
	n := len(args)
	for n > 0 && args[n-1] == nil {
		n--
	}
	if n == 0 {
		return nil
	}
	return args[:n]
}

// opt passes an optional pointer param: nil stays an untyped nil, so positional can drop it.
func opt[T any](p *T) any {
	if p == nil {
		return nil
	}
	return p
}

func optSlice[T any](s []T) any {
	if s == nil {
		return nil
	}
	return s
}
