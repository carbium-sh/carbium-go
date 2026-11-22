// Package stream is Carbon Stream (Yellowstone gRPC) on services.carbium.sh:9443: Subscribe and the seven unary
// calls the services serve, with Carbium's rules applied. Fields the service refuses are caught before any call,
// SubscribeWithReconnect follows the published reconnect table, pings inside the stream every 10 s, re-seeds on
// OUT_OF_RANGE and resumes from the last slot when the retained floor allows.
//
// The key needs the grpc scope and a plan with streams. It travels as x-token metadata, never in a URL, and every
// format verb hides it. This module stands alone, so gRPC stays out of github.com/carbium-sh/carbium-go.
package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/carbium-sh/carbium-go/stream/geyserpb"
)

// DefaultEndpoint is where Connect dials unless Options.Endpoint says otherwise.
const DefaultEndpoint = "services.carbium.sh:9443"

const (
	service      = "/geyser.Geyser/"
	pingEvery    = 10 * time.Second
	maxRecvBytes = 64 << 20
)

// Error is the one error type of this module. Kind is "auth" (a key of the wrong shape), "request" (refused before
// any call) or "stream" (the service ended a call); Code is the gRPC status. The message never holds a key.
type Error struct {
	Kind       string
	Code       codes.Code
	Message    string
	Retryable  bool
	RetryAfter time.Duration
	cause      error
}

func (e *Error) Error() string { return "carbium stream: " + e.Kind + ": " + e.Message }

// Unwrap returns the gRPC error the service sent.
func (e *Error) Unwrap() error { return e.cause }

var secret = regexp.MustCompile(`\b(c_serv_|c_acct_|whsec_)[A-Za-z0-9+/=_-]{5,}|((?i)api[-_]?key|token)=[^&\s"'<>]+`)

func redact(s string) string { return secret.ReplaceAllString(s, "${1}…") }

func newError(kind string, code codes.Code, msg string, cause error) *Error {
	return &Error{Kind: kind, Code: code, Message: redact(msg), cause: cause}
}

// Options tune Connect. The zero value is the default.
type Options struct {
	// Endpoint defaults to services.carbium.sh:9443 (host:port). A key in it is refused.
	Endpoint string
	// MaxRecvMsgSize defaults to 64 MiB, the published minimum.
	MaxRecvMsgSize int
	// DialOptions are added after the module's own (TLS with the system roots), so they can replace them.
	DialOptions []grpc.DialOption
}

// Client holds one gRPC connection. It never prints its key.
type Client struct {
	conn   *grpc.ClientConn
	key    *string
	closed context.Context // done once Close ran: every reconnecting stream ends with it
	close  context.CancelFunc

	// Swappable for tests.
	sleep     func(context.Context, time.Duration) error
	random    func() float64
	pingEvery time.Duration
}

var serviceKey = regexp.MustCompile(`^c_serv_[A-Za-z0-9]+$`)

// Connect makes a Client for a service key (c_serv_…) with the grpc scope. It dials lazily, on the first call.
func Connect(apiKey string, opts *Options) (*Client, error) {
	if opts == nil {
		opts = &Options{}
	}
	if !serviceKey.MatchString(apiKey) {
		return nil, newError("auth", codes.Unauthenticated, "the key must be the full service key (c_serv_…) with the grpc scope", nil)
	}
	endpoint := opts.Endpoint
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}
	if strings.Contains(endpoint, "c_serv_") || strings.Contains(endpoint, "c_acct_") || regexp.MustCompile(`(?i)(api[-_]?key|token)=`).MatchString(endpoint) {
		return nil, newError("request", codes.InvalidArgument, "put the key in the key argument, not in the endpoint", nil)
	}
	size := opts.MaxRecvMsgSize
	if size <= 0 {
		size = maxRecvBytes
	}
	key := apiKey
	withKey := func(ctx context.Context) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "x-token", key)
	}
	dial := append([]grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(size)),
		// Chained, so a caller's own interceptor wraps this one and the key is still sent.
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, o ...grpc.CallOption) error {
			return invoker(withKey(ctx), method, req, reply, cc, o...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, o ...grpc.CallOption) (grpc.ClientStream, error) {
			return streamer(withKey(ctx), desc, cc, method, o...)
		}),
	}, opts.DialOptions...)
	conn, err := grpc.NewClient(endpoint, dial...)
	if err != nil {
		return nil, newError("request", codes.InvalidArgument, err.Error(), err)
	}
	closed, closeFn := context.WithCancel(context.Background())
	return &Client{conn: conn, key: &key, closed: closed, close: closeFn, sleep: sleepContext, random: rand.Float64, pingEvery: pingEvery}, nil
}

// Format prints the client with its key as a placeholder.
func (c *Client) Format(f fmt.State, verb rune) {
	fmt.Fprintf(f, "stream.Client{target: %s, key: c_serv_…}", c.conn.Target())
}

// Close closes the connection.
// Close closes the connection and ends every reconnecting stream of the client.
func (c *Client) Close() error {
	c.close()
	return c.conn.Close()
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (c *Client) invoke(ctx context.Context, method string, req, out proto.Message) error {
	var trailer metadata.MD
	if err := c.conn.Invoke(ctx, service+method, req, out, grpc.Trailer(&trailer)); err != nil {
		info := withHint(Classify(err), trailer)
		e := newError("stream", info.Code, info.Message, err)
		e.Retryable, e.RetryAfter = info.Action != ActionStop, info.RetryAfter
		return e
	}
	return nil
}

// The seven unary calls, one credit each.

// GetSlot calls GetSlot; commitment may be nil.
func (c *Client) GetSlot(ctx context.Context, commitment *geyserpb.CommitmentLevel) (*geyserpb.GetSlotResponse, error) {
	out := new(geyserpb.GetSlotResponse)
	return out, c.invoke(ctx, "GetSlot", &geyserpb.GetSlotRequest{Commitment: commitment}, out)
}

// GetBlockHeight calls GetBlockHeight; commitment may be nil.
func (c *Client) GetBlockHeight(ctx context.Context, commitment *geyserpb.CommitmentLevel) (*geyserpb.GetBlockHeightResponse, error) {
	out := new(geyserpb.GetBlockHeightResponse)
	return out, c.invoke(ctx, "GetBlockHeight", &geyserpb.GetBlockHeightRequest{Commitment: commitment}, out)
}

// GetLatestBlockhash calls GetLatestBlockhash; commitment may be nil.
func (c *Client) GetLatestBlockhash(ctx context.Context, commitment *geyserpb.CommitmentLevel) (*geyserpb.GetLatestBlockhashResponse, error) {
	out := new(geyserpb.GetLatestBlockhashResponse)
	return out, c.invoke(ctx, "GetLatestBlockhash", &geyserpb.GetLatestBlockhashRequest{Commitment: commitment}, out)
}

// IsBlockhashValid calls IsBlockhashValid; commitment may be nil.
func (c *Client) IsBlockhashValid(ctx context.Context, blockhash string, commitment *geyserpb.CommitmentLevel) (*geyserpb.IsBlockhashValidResponse, error) {
	out := new(geyserpb.IsBlockhashValidResponse)
	return out, c.invoke(ctx, "IsBlockhashValid", &geyserpb.IsBlockhashValidRequest{Blockhash: blockhash, Commitment: commitment}, out)
}

// GetVersion calls GetVersion.
func (c *Client) GetVersion(ctx context.Context) (*geyserpb.GetVersionResponse, error) {
	out := new(geyserpb.GetVersionResponse)
	return out, c.invoke(ctx, "GetVersion", &geyserpb.GetVersionRequest{}, out)
}

// Ping calls the unary Ping, which costs one credit; the in-stream ping of SubscribeWithReconnect costs nothing.
func (c *Client) Ping(ctx context.Context, count int32) (*geyserpb.PongResponse, error) {
	out := new(geyserpb.PongResponse)
	return out, c.invoke(ctx, "Ping", &geyserpb.PingRequest{Count: count}, out)
}

// SubscribeReplayInfo calls SubscribeReplayInfo: the first slot a FromSlot replay can start from.
func (c *Client) SubscribeReplayInfo(ctx context.Context) (*geyserpb.SubscribeReplayInfoResponse, error) {
	out := new(geyserpb.SubscribeReplayInfoResponse)
	return out, c.invoke(ctx, "SubscribeReplayInfo", &geyserpb.SubscribeReplayInfoRequest{}, out)
}

// Filters are the seven served filter kinds and the request-level fields of a subscription. Commitment nil means
// processed.
type Filters struct {
	Accounts           map[string]*geyserpb.SubscribeRequestFilterAccounts
	Slots              map[string]*geyserpb.SubscribeRequestFilterSlots
	Transactions       map[string]*geyserpb.SubscribeRequestFilterTransactions
	TransactionsStatus map[string]*geyserpb.SubscribeRequestFilterTransactions
	Blocks             map[string]*geyserpb.SubscribeRequestFilterBlocks
	BlocksMeta         map[string]*geyserpb.SubscribeRequestFilterBlocksMeta
	Entry              map[string]*geyserpb.SubscribeRequestFilterEntry
	Commitment         *geyserpb.CommitmentLevel
	AccountsDataSlice  []*geyserpb.SubscribeRequestAccountsDataSlice
	// FromSlot replays from this slot; processed commitment only, and only in the first request.
	FromSlot *uint64
}

func refused(what string) error {
	return newError("request", codes.InvalidArgument, what+" is refused by Carbon Stream: remove the field", nil)
}

// BuildRequest makes the complete first request, refusing locally what the service refuses: cuckoo filters,
// token_accounts, include_update_parent, a bare transaction filter, and FromSlot at any commitment but processed.
func BuildRequest(f Filters) (*geyserpb.SubscribeRequest, error) {
	for name, a := range f.Accounts {
		if a.GetCuckooAccountsFilter() != nil {
			return nil, refused("accounts." + name + ".cuckoo_accounts_filter")
		}
	}
	for kind, m := range map[string]map[string]*geyserpb.SubscribeRequestFilterTransactions{"transactions": f.Transactions, "transactions_status": f.TransactionsStatus} {
		for name, t := range m {
			if t == nil {
				return nil, newError("request", codes.InvalidArgument, kind+"."+name+" is a bare transaction filter: name account_include, account_required, account_exclude, signature, vote or failed", nil)
			}
			if t.TokenAccounts != nil {
				return nil, refused(kind + "." + name + ".token_accounts")
			}
			if t.GetCuckooAccountInclude() != nil {
				return nil, refused(kind + "." + name + ".cuckoo_account_include")
			}
			if t.Vote == nil && t.Failed == nil && t.Signature == nil && len(t.AccountInclude) == 0 && len(t.AccountExclude) == 0 && len(t.AccountRequired) == 0 {
				return nil, newError("request", codes.InvalidArgument, kind+"."+name+" is a bare transaction filter: name account_include, account_required, account_exclude, signature, vote or failed", nil)
			}
		}
	}
	for name, b := range f.Blocks {
		if b.GetCuckooAccountInclude() != nil {
			return nil, refused("blocks." + name + ".cuckoo_account_include")
		}
	}
	for name, e := range f.Entry {
		if e.GetIncludeUpdateParent() {
			return nil, refused("entry." + name + ".include_update_parent")
		}
	}
	if f.FromSlot != nil && f.Commitment != nil && *f.Commitment != geyserpb.CommitmentLevel_PROCESSED {
		return nil, newError("request", codes.InvalidArgument, "from_slot replay is available only at processed commitment", nil)
	}
	req := &geyserpb.SubscribeRequest{
		Accounts: f.Accounts, Slots: f.Slots, Transactions: f.Transactions, TransactionsStatus: f.TransactionsStatus,
		Blocks: f.Blocks, BlocksMeta: f.BlocksMeta, Entry: f.Entry, Commitment: f.Commitment,
		AccountsDataSlice: f.AccountsDataSlice, FromSlot: f.FromSlot,
	}
	// A deep copy: the caller may change its own maps later, and a running stream must keep the set it opened with.
	return proto.Clone(req).(*geyserpb.SubscribeRequest), nil
}

// Stream is one plain subscription, with no reconnect and no keepalive.
type Stream struct{ s grpc.ClientStream }

// Recv returns the next update.
func (s *Stream) Recv() (*geyserpb.SubscribeUpdate, error) {
	u := new(geyserpb.SubscribeUpdate)
	if err := s.s.RecvMsg(u); err != nil {
		return nil, err
	}
	return u, nil
}

// Send sends a request on the stream, such as a ping.
func (s *Stream) Send(r *geyserpb.SubscribeRequest) error { return s.s.SendMsg(r) }

// CloseSend ends the requests; the stream ends when the service closes it or ctx ends.
func (s *Stream) CloseSend() error { return s.s.CloseSend() }

var subscribeDesc = &grpc.StreamDesc{StreamName: "Subscribe", ServerStreams: true, ClientStreams: true}

func (c *Client) open(ctx context.Context, req *geyserpb.SubscribeRequest) (*Stream, error) {
	s, err := c.conn.NewStream(ctx, subscribeDesc, service+"Subscribe")
	if err != nil {
		return nil, err
	}
	if err := s.SendMsg(req); err != nil {
		return nil, err
	}
	return &Stream{s}, nil
}

// Subscribe opens one plain stream with the whole filter set in its first message: for short reads.
func (c *Client) Subscribe(ctx context.Context, f Filters) (*Stream, error) {
	req, err := BuildRequest(f)
	if err != nil {
		return nil, err
	}
	s, err := c.open(ctx, req)
	if err != nil {
		info := Classify(err)
		return nil, newError("stream", info.Code, info.Message, err)
	}
	return s, nil
}

// Action is what a stream error leads to.
type Action string

const (
	ActionStop   Action = "stop"
	ActionReseed Action = "reseed"
	ActionRetry  Action = "retry"
)

// ErrorInfo is a stream error read against the published table.
type ErrorInfo struct {
	Code       codes.Code
	Message    string
	Action     Action
	RetryAfter time.Duration
}

var creditsExhausted = regexp.MustCompile(`(?i)credits.exhausted`)

// Classify reads a gRPC status and decides stop, re-seed or retry: stop on UNAUTHENTICATED, PERMISSION_DENIED,
// INVALID_ARGUMENT, UNIMPLEMENTED and RESOURCE_EXHAUSTED for credits; re-seed on OUT_OF_RANGE; retry anything else
// after the table's wait (2 s, or 1 s for UNAVAILABLE "still loading").
func Classify(err error) ErrorInfo {
	info := ErrorInfo{Code: codes.Unknown, Message: "stream ended", Action: ActionRetry, RetryAfter: 2 * time.Second}
	if err == nil || errors.Is(err, io.EOF) {
		return info
	}
	if st, ok := status.FromError(err); ok {
		info.Code, info.Message = st.Code(), st.Message()
	} else {
		info.Message = err.Error()
	}
	switch info.Code {
	case codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument, codes.Unimplemented:
		info.Action = ActionStop
	case codes.OutOfRange:
		info.Action = ActionReseed
	case codes.ResourceExhausted:
		if creditsExhausted.MatchString(info.Message) {
			info.Action = ActionStop
		}
	case codes.Unavailable:
		if strings.Contains(strings.ToLower(info.Message), "still loading") {
			info.RetryAfter = time.Second
		}
	}
	return info
}

// Handlers receive what SubscribeWithReconnect sees.
type Handlers struct {
	// OnUpdate gets every update except pings and pongs. It must not block: a reader that takes no frame for 10 s
	// is cut.
	OnUpdate func(*geyserpb.SubscribeUpdate)
	// OnReseed runs after OUT_OF_RANGE, or when a resume cannot replay: reload state over RPC, then the stream
	// subscribes again. lastSlot is nil when no slot was seen.
	OnReseed func(ctx context.Context, lastSlot *uint64)
	// OnStatus sees each connect, retry, re-seed and stop.
	OnStatus func(Status)
}

// Status is one event of a reconnecting stream: Type is connected, retry, reseed or stopped.
type Status struct {
	Type     string
	Attempt  int
	FromSlot *uint64
	Delay    time.Duration
	Err      *Error
}

// Reconnecting is a stream that follows the reconnect table until Close or a stop.
type Reconnecting struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

// Close ends the stream; Done then closes.
func (r *Reconnecting) Close() { r.cancel() }

// Done closes when the stream has ended.
func (r *Reconnecting) Done() <-chan struct{} { return r.done }

// Err is nil after Close, or the *Error that stopped the stream. Read it after Done.
func (r *Reconnecting) Err() error { return r.err }

func slotOf(u *geyserpb.SubscribeUpdate) (uint64, bool) {
	switch {
	case u.GetSlot() != nil:
		return u.GetSlot().GetSlot(), true
	case u.GetTransaction() != nil:
		return u.GetTransaction().GetSlot(), true
	case u.GetTransactionStatus() != nil:
		return u.GetTransactionStatus().GetSlot(), true
	case u.GetAccount() != nil:
		return u.GetAccount().GetSlot(), true
	case u.GetBlockMeta() != nil:
		return u.GetBlockMeta().GetSlot(), true
	case u.GetBlock() != nil:
		return u.GetBlock().GetSlot(), true
	case u.GetEntry() != nil:
		return u.GetEntry().GetSlot(), true
	}
	return 0, false
}

// SubscribeWithReconnect keeps a stream with a fixed filter set alive by the published table: a stop status ends it,
// OUT_OF_RANGE re-seeds and subscribes again at once, anything else waits max(table, backoff) plus up to 1 s of
// jitter, the backoff doubling from 2 s to 30 s and resetting after data. After the wait, at processed commitment
// and with a slot seen, it reads SubscribeReplayInfo and resumes from the last slot when the floor allows, else
// re-seeds. It pings inside the stream every 10 s with at most one ping unanswered.
func (c *Client) SubscribeWithReconnect(ctx context.Context, f Filters, h Handlers) (*Reconnecting, error) {
	base, err := BuildRequest(f)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	stopOnClose := context.AfterFunc(c.closed, cancel) // a closed connection would answer Canceled forever
	r := &Reconnecting{cancel: cancel, done: make(chan struct{})}
	status := func(s Status) {
		if h.OnStatus != nil {
			h.OnStatus(s)
		}
	}
	reseed := func(last *uint64) {
		if h.OnReseed != nil {
			h.OnReseed(ctx, last)
		}
	}
	canResume := base.Commitment == nil || *base.Commitment == geyserpb.CommitmentLevel_PROCESSED
	go func() {
		defer close(r.done)
		defer stopOnClose()
		defer cancel()
		lastSlot := base.FromSlot
		req := base
		backoff := 2 * time.Second
		attempt := 0
		for ctx.Err() == nil {
			gotData, recvErr, trailer := c.runOnce(ctx, req, base, attempt, h, status, &lastSlot)
			if ctx.Err() != nil {
				break
			}
			info := withHint(Classify(recvErr), trailer)
			e := newError("stream", info.Code, info.Message, recvErr)
			e.Retryable, e.RetryAfter = info.Action != ActionStop, info.RetryAfter
			if info.Action == ActionStop {
				status(Status{Type: "stopped", Err: e})
				r.err = e
				return
			}
			if gotData {
				backoff, attempt = 2*time.Second, 0
			}
			attempt++
			if info.Action == ActionReseed {
				status(Status{Type: "reseed", Err: e, FromSlot: lastSlot})
				reseed(lastSlot)
				req = withoutFromSlot(base)
				continue
			}
			delay := max(info.RetryAfter, backoff) + time.Duration(c.random()*float64(time.Second))
			backoff = min(30*time.Second, backoff*2)
			status(Status{Type: "retry", Err: e, Delay: delay, Attempt: attempt})
			if c.sleep(ctx, delay) != nil {
				break
			}
			req = withoutFromSlot(base)
			if canResume && lastSlot != nil {
				if floor, err := c.SubscribeReplayInfo(ctx); err == nil {
					if *lastSlot >= floor.GetFirstAvailable() {
						from := *lastSlot
						req.FromSlot = &from
					} else {
						status(Status{Type: "reseed", FromSlot: lastSlot})
						reseed(lastSlot)
					}
				} // a failed read of the floor means: go live
			}
		}
		status(Status{Type: "stopped"})
	}()
	return r, nil
}

func withoutFromSlot(base *geyserpb.SubscribeRequest) *geyserpb.SubscribeRequest {
	r := proto.Clone(base).(*geyserpb.SubscribeRequest)
	r.FromSlot = nil
	return r
}

// runOnce runs one stream until it ends, and reports whether it delivered data and the error that ended it.
func (c *Client) runOnce(ctx context.Context, req, base *geyserpb.SubscribeRequest, attempt int, h Handlers, status func(Status), lastSlot **uint64) (bool, error, metadata.MD) {
	sctx, stop := context.WithCancel(ctx)
	defer stop()
	s, err := c.open(sctx, req)
	if err != nil {
		return false, err, nil
	}
	status(Status{Type: "connected", Attempt: attempt, FromSlot: req.FromSlot})
	var mu sync.Mutex
	pongPending := false
	pingID := int32(0)
	ticker := time.NewTicker(c.pingEvery)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-sctx.Done():
				return
			case <-ticker.C:
				mu.Lock()
				if pongPending {
					mu.Unlock()
					continue // the control queue holds 8 messages: never pile pings up
				}
				pongPending = true
				pingID++
				ping := withoutFromSlot(base)
				ping.Ping = &geyserpb.SubscribeRequestPing{Id: pingID}
				mu.Unlock()
				if err := s.Send(ping); err != nil {
					stop()
					return
				}
			}
		}
	}()
	gotData := false
	for {
		u, err := s.Recv()
		if err != nil {
			return gotData, err, s.s.Trailer()
		}
		if u.GetPong() != nil {
			mu.Lock()
			pongPending = false
			mu.Unlock()
			continue
		}
		if u.GetPing() != nil {
			continue
		}
		gotData = true
		if slot, ok := slotOf(u); ok {
			*lastSlot = &slot
		}
		if h.OnUpdate != nil {
			h.OnUpdate(u)
		}
	}
}

// withHint raises the table's wait to the service's own retry-after (retry-after-ms, else retry-after in seconds,
// from the trailers) when that is longer.
func withHint(info ErrorInfo, trailer metadata.MD) ErrorInfo {
	var hint time.Duration
	if v := trailer.Get("retry-after-ms"); len(v) > 0 {
		if n, err := strconv.ParseInt(strings.TrimSpace(v[0]), 10, 64); err == nil && n >= 0 && n < int64(time.Hour/time.Millisecond) {
			hint = time.Duration(n) * time.Millisecond
		}
	} else if v := trailer.Get("retry-after"); len(v) > 0 {
		if n, err := strconv.ParseInt(strings.TrimSpace(v[0]), 10, 64); err == nil && n >= 0 && n < int64(time.Hour/time.Second) {
			hint = time.Duration(n) * time.Second
		}
	}
	info.RetryAfter = max(info.RetryAfter, hint)
	return info
}
