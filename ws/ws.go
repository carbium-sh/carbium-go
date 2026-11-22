// Package ws holds JSON-RPC subscriptions over wss://services.carbium.sh with the published reconnect rules: every
// live subscription is bound again after a reconnect, server ids are remapped, and OnReseed tells the caller to
// read again what it may have missed. The server replays nothing after a disconnect.
//
// The key needs the websocket scope. It travels in the x-api-key handshake header by default, or in the
// Sec-WebSocket-Protocol header ("carbium, api-key.<key>") with Carrier "subprotocol"; never in the URL.
package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	carbium "github.com/carbium-sh/carbium-go"
)

// DefaultURL is where the socket connects unless Options.URL says otherwise.
const DefaultURL = "wss://services.carbium.sh"

const (
	bindBatch          = 32
	pingWatchdog       = 75 * time.Second
	readLimit          = 64 << 20
	carrierHeader      = "header"
	carrierSubprotocol = "subprotocol"
)

var retryReasons = map[string]bool{"RATE_LIMITED": true, "LOAD_SHED": true, "FREE_SUBNET_LIMITED": true, "FREE_TIER_BUSY": true, "CREDITS_PENDING": true}

// State is where a Socket is in its life.
type State string

const (
	StateIdle       State = "idle"
	StateConnecting State = "connecting"
	StateOpen       State = "open"
	StateBackoff    State = "backoff"
	StateClosed     State = "closed"
)

// CloseInfo is the code and reason of the last close.
type CloseInfo struct {
	Code   int
	Reason string
}

// Options tune a Socket. The zero value is the default.
type Options struct {
	// URL defaults to wss://services.carbium.sh. A key in the URL is refused.
	URL string
	// Carrier is how the key travels: "header" (x-api-key, the default) or "subprotocol" (Sec-WebSocket-Protocol,
	// the form browsers need).
	Carrier string
	// HTTPClient is used for the handshake.
	HTTPClient *http.Client
	// OnReseed runs after every reconnect, once every live subscription is bound again. Gap is true after close
	// 1012 stream_gap_reseed_required. Read again what you may have missed.
	OnReseed func(last CloseInfo, gap bool)
	// OnFatal runs once when the socket gives up: a key refusal, a second credits_exhausted, a refused handshake.
	// The same error also comes back from Ready of every subscription still waiting, so a caller that prints in both
	// prints it twice.
	OnFatal func(*carbium.Error)
	// OnError gets a subscription the server refused when it was bound again after a reconnect.
	OnError func(*carbium.Error)
	// OnState sees every state change.
	OnState func(State, CloseInfo)
}

// Socket holds WebSocket subscriptions on one connection. It is safe for concurrent use. Notification callbacks run
// on the socket's reader and must not block.
type Socket struct {
	url     string
	carrier string
	key     *carbium.Secret // behind a pointer, so a dereferenced socket never prints it
	opts    Options

	ctx    context.Context
	cancel context.CancelFunc

	mu        sync.Mutex
	state     State
	running   bool
	conn      *websocket.Conn
	gen       uint64
	subs      map[uint64]*Subscription
	byServer  map[uint64]*Subscription
	pending   map[uint64]pendingCall
	nextID    uint64
	nextLocal uint64
	fatal     *carbium.Error
	closed    bool
	lastClose CloseInfo

	attempt, overloadCloses          int
	creditsExhaustedOnce, everOpened bool

	// Swappable for tests.
	sleep    func(context.Context, time.Duration) error
	random   func() float64
	watchdog time.Duration
}

type reply struct {
	result json.RawMessage
	err    *carbium.Error
}

// pendingCall is a request waiting for its answer. A bind names its subscription, so the reader records the new
// server id before it reads the next message: a notification can follow its ack at once.
type pendingCall struct {
	ch  chan reply
	sub *Subscription
	gen uint64
}

var serviceKey = regexp.MustCompile(`^c_serv_[A-Za-z0-9]+$`)

// New makes a Socket for a service key (c_serv_…, with the websocket scope). It connects on the first Subscribe.
func New(apiKey string, opts *Options) (*Socket, error) {
	if opts == nil {
		opts = &Options{}
	}
	if apiKey != "" && !serviceKey.MatchString(apiKey) {
		return nil, carbium.NewError(carbium.ErrorInit{Kind: carbium.KindAuth, Message: "the key must be the full service key (c_serv_…) with the websocket scope"})
	}
	if opts.Carrier != "" && opts.Carrier != carrierHeader && opts.Carrier != carrierSubprotocol {
		return nil, carbium.NewError(carbium.ErrorInit{Kind: carbium.KindRequest, Message: `Carrier is "header" (the default) or "subprotocol"`})
	}
	u := opts.URL
	if u == "" {
		u = DefaultURL
	}
	u = strings.TrimRight(strings.Replace(strings.Replace(u, "https://", "wss://", 1), "http://", "ws://", 1), "/")
	if err := carbium.CheckEndpoint(u, "wss", "ws"); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	key := carbium.Secret(apiKey)
	return &Socket{url: u, carrier: opts.Carrier, key: &key, opts: *opts, ctx: ctx, cancel: cancel, state: StateIdle,
		subs: map[uint64]*Subscription{}, byServer: map[uint64]*Subscription{}, pending: map[uint64]pendingCall{},
		sleep: sleepContext, random: rand.Float64, watchdog: pingWatchdog}, nil
}

// Format prints the socket with its key as a placeholder.
func (s *Socket) Format(f fmt.State, verb rune) {
	fmt.Fprintf(f, "ws.Socket{url: %s, key: %s}", s.url, *s.key)
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

// State is the socket's current state.
func (s *Socket) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Size is how many subscriptions the socket holds.
func (s *Socket) Size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subs)
}

func (s *Socket) setState(st State, info CloseInfo) {
	s.mu.Lock()
	s.state = st
	s.mu.Unlock()
	if s.opts.OnState != nil {
		s.opts.OnState(st, info)
	}
}

func socketError(method, message string) *carbium.Error {
	return carbium.NewError(carbium.ErrorInit{Kind: carbium.KindSocket, Method: method, Message: message})
}

// Subscription is one subscription. Its server id changes on every reconnect; Ready returns the first one.
type Subscription struct {
	s        *Socket
	local    uint64
	method   string
	params   []any
	onNotify func(json.RawMessage)

	serverID   uint64
	acked      bool
	bindingGen uint64 // the connection a bind is under way on
	ready      chan struct{}
	readyID    uint64
	readyErr   error
	once       sync.Once
}

func (sub *Subscription) settle(id uint64, err error) {
	sub.once.Do(func() {
		sub.readyID, sub.readyErr = id, err
		close(sub.ready)
	})
}

// Ready waits for the server's first acknowledgement and returns its subscription id, or the refusal.
func (sub *Subscription) Ready(ctx context.Context) (uint64, error) {
	select {
	case <-sub.ready:
		return sub.readyID, sub.readyErr
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// Unsubscribe ends the subscription. Unsubscribing the last one closes the socket with 1000. It returns before its
// frame is necessarily written, so a program that exits at once may not have told the server; closing the socket
// ends its subscriptions either way.
func (sub *Subscription) Unsubscribe(ctx context.Context) error {
	s := sub.s
	s.mu.Lock()
	if s.subs[sub.local] != sub {
		s.mu.Unlock()
		return nil
	}
	delete(s.subs, sub.local)
	serverID, gen, open := sub.serverID, s.gen, s.state == StateOpen
	if serverID != 0 {
		delete(s.byServer, serverID)
	}
	acked := sub.acked
	last := len(s.subs) == 0
	conn := s.conn
	s.mu.Unlock()
	var send func()
	if serverID != 0 && open {
		// Sent without waiting for the answer, so a callback on the reader can unsubscribe; false or -32602 both
		// mean gone.
		send = s.frame(gen, strings.TrimSuffix(sub.method, "Subscribe")+"Unsubscribe", []any{serverID})
	}
	if !acked {
		sub.settle(0, socketError(sub.method, "unsubscribed before the acknowledgement"))
	}
	closeAfter := last && open && conn != nil
	if send != nil || closeAfter {
		go func() {
			if send != nil {
				send()
			}
			if closeAfter {
				_ = conn.Close(websocket.StatusNormalClosure, "no subscriptions")
			}
		}()
	}
	return nil
}

// frame prepares a request that is sent without waiting for its answer, on the socket's own context: a caller's
// done context must never close the connection the other subscriptions share. It returns nil when the connection
// is gone.
func (s *Socket) frame(gen uint64, method string, params []any) func() {
	s.mu.Lock()
	conn := s.conn
	if conn == nil || s.gen != gen {
		s.mu.Unlock()
		return nil
	}
	s.nextID++
	id := s.nextID
	s.mu.Unlock()
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	return func() { _ = conn.Write(s.ctx, websocket.MessageText, b) }
}

// notify sends a request in the background without waiting for its answer.
func (s *Socket) notify(gen uint64, method string, params []any) {
	if send := s.frame(gen, method, params); send != nil {
		go send()
	}
}

// Subscribe adds any served subscription by name with raw params; onNotify gets each notification's result. The
// typed methods call it.
func (s *Socket) Subscribe(method string, params []any, onNotify func(json.RawMessage)) (*Subscription, error) {
	served := false
	for _, name := range carbium.Subscriptions() {
		served = served || name == method
	}
	if !served {
		return nil, carbium.NewError(carbium.ErrorInit{Kind: carbium.KindRequest, Method: method, Message: method + " is not a served subscription"})
	}
	s.mu.Lock()
	if s.fatal != nil {
		err := s.fatal
		s.mu.Unlock()
		return nil, err
	}
	if s.closed {
		s.mu.Unlock()
		return nil, socketError(method, "the socket was closed")
	}
	s.nextLocal++
	sub := &Subscription{s: s, local: s.nextLocal, method: method, params: params, onNotify: onNotify, ready: make(chan struct{})}
	s.subs[sub.local] = sub
	open, gen, start := s.state == StateOpen, s.gen, !s.running
	if start {
		s.running = true
	}
	s.mu.Unlock()
	switch {
	case open:
		go s.bind(sub, gen)
	case start:
		go s.run()
	}
	return sub, nil
}

// Close closes the socket; no reconnect follows. Pending subscriptions are refused.
func (s *Socket) Close() error {
	s.mu.Lock()
	s.closed = true
	conn := s.conn
	var pending []*Subscription
	for _, sub := range s.subs {
		if !sub.acked {
			pending = append(pending, sub)
		}
	}
	s.subs, s.byServer = map[uint64]*Subscription{}, map[uint64]*Subscription{}
	s.mu.Unlock()
	err := socketError("", "the socket was closed")
	for _, sub := range pending {
		sub.settle(0, err)
	}
	if conn != nil {
		_ = conn.Close(websocket.StatusNormalClosure, "client close")
	}
	s.cancel()
	s.setState(StateClosed, CloseInfo{})
	return nil
}

func (s *Socket) die(err *carbium.Error) {
	s.mu.Lock()
	if s.fatal != nil {
		s.mu.Unlock()
		return
	}
	s.fatal = err
	var pending []*Subscription
	for _, sub := range s.subs {
		if !sub.acked {
			pending = append(pending, sub)
		}
	}
	s.subs, s.byServer = map[uint64]*Subscription{}, map[uint64]*Subscription{}
	conn := s.conn
	s.conn = nil
	s.mu.Unlock()
	for _, sub := range pending {
		sub.settle(0, err)
	}
	if conn != nil {
		_ = conn.Close(websocket.StatusNormalClosure, "fatal")
	}
	s.cancel()
	s.setState(StateClosed, CloseInfo{})
	if s.opts.OnFatal != nil {
		s.opts.OnFatal(err)
	}
}

func (s *Socket) done() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed || s.fatal != nil
}

// run owns the connection: it dials, reads, and decides each reconnect from the close code.
func (s *Socket) run() {
	defer func() {
		s.mu.Lock()
		s.running = false
		if s.state == StateIdle && !s.closed && s.fatal == nil && len(s.subs) > 0 {
			s.running = true // a subscription came in while the connection was ending
			go s.run()
		}
		s.mu.Unlock()
	}()
	for !s.done() {
		if s.idleIfEmpty() {
			return
		}
		s.setState(StateConnecting, CloseInfo{})
		opts := &websocket.DialOptions{HTTPClient: carbium.NoRedirects(s.opts.HTTPClient)}
		switch {
		case *s.key == "":
		case s.carrier == carrierSubprotocol:
			opts.Subprotocols = []string{"carbium", "api-key." + s.key.Reveal()}
		default:
			opts.HTTPHeader = http.Header{"X-Api-Key": {s.key.Reveal()}}
		}
		var watch *time.Timer
		var watchMu sync.Mutex
		opts.OnPingReceived = func(context.Context, []byte) bool {
			watchMu.Lock()
			if watch != nil {
				watch.Reset(s.watchdog)
			}
			watchMu.Unlock()
			return true
		}
		conn, resp, err := websocket.Dial(s.ctx, s.url, opts)
		if err != nil {
			if s.done() {
				return
			}
			status, retryAfter := 0, time.Duration(0)
			if resp != nil {
				status = resp.StatusCode
				if ra := resp.Header.Get("Retry-After"); status == 429 && ra != "" {
					if n, err := strconv.Atoi(ra); err == nil {
						retryAfter = time.Duration(n) * time.Second
					}
				}
			}
			if status == 401 || status == 403 {
				msg := "websocket handshake refused: unknown, suspended or revoked key, or no websocket scope"
				if status == 403 {
					msg = "websocket handshake refused: address not allowed or no websocket scope"
				}
				s.die(carbium.NewError(carbium.ErrorInit{Kind: carbium.KindAuth, HTTPStatus: status, Message: msg}))
				return
			}
			if !s.afterClose(CloseInfo{Code: 1006, Reason: "handshake failed"}, false, status, retryAfter) {
				return
			}
			continue
		}
		conn.SetReadLimit(readLimit)
		s.mu.Lock()
		if len(s.subs) == 0 {
			// the last subscription left while this connection opened
			s.mu.Unlock()
			_ = conn.Close(websocket.StatusNormalClosure, "no subscriptions")
			s.setState(StateIdle, CloseInfo{})
			return
		}
		s.conn = conn
		s.gen++
		gen := s.gen
		s.attempt = 0
		reconnect := s.everOpened
		s.everOpened = true
		last := s.lastClose
		s.mu.Unlock()
		s.setState(StateOpen, CloseInfo{})
		watchMu.Lock()
		watch = time.AfterFunc(s.watchdog, func() { _ = conn.CloseNow() })
		watchMu.Unlock()
		go func() {
			s.rebindAll(gen)
			s.mu.Lock()
			still := s.gen == gen && s.state == StateOpen
			s.mu.Unlock()
			if reconnect && still && s.opts.OnReseed != nil {
				s.opts.OnReseed(last, last.Code == 1012 && last.Reason == "stream_gap_reseed_required")
			}
		}()
		info := s.read(conn)
		watchMu.Lock()
		watch.Stop()
		watchMu.Unlock()
		s.mu.Lock()
		s.conn = nil
		s.lastClose = info
		pending := s.pending
		s.pending = map[uint64]pendingCall{}
		s.byServer = map[uint64]*Subscription{}
		for _, sub := range s.subs {
			sub.serverID = 0
		}
		empty := len(s.subs) == 0
		s.mu.Unlock()
		dropped := carbium.NewError(carbium.ErrorInit{Kind: carbium.KindSocket, Code: info.Code, Reason: info.Reason, Message: strings.TrimSpace(fmt.Sprintf("socket closed %d %s", info.Code, info.Reason))})
		for _, p := range pending {
			p.ch <- reply{err: dropped}
		}
		if s.done() {
			return
		}
		if empty {
			s.setState(StateIdle, info)
			return
		}
		if !s.afterClose(info, true, 0, 0) {
			return
		}
	}
}

// afterClose applies the close table: it waits the delay the close calls for and returns true to reconnect, or
// gives up and returns false.
// idleIfEmpty goes idle when no subscription is left, and reports whether it did.
func (s *Socket) idleIfEmpty() bool {
	s.mu.Lock()
	empty := len(s.subs) == 0
	s.mu.Unlock()
	if empty {
		s.setState(StateIdle, CloseInfo{})
	}
	return empty
}

func (s *Socket) afterClose(info CloseInfo, opened bool, status int, retryAfter time.Duration) bool {
	r := s.random()
	var delay time.Duration
	s.mu.Lock()
	var fatal *carbium.Error
	switch {
	case info.Code == 1008 && strings.HasPrefix(info.Reason, "key_"):
		fatal = carbium.NewError(carbium.ErrorInit{Kind: carbium.KindAuth, Code: 1008, Reason: info.Reason, Message: "the server closed the socket: " + info.Reason + "; the key must be fixed"})
	case info.Code == 1008 && info.Reason == "credits_exhausted":
		if s.creditsExhaustedOnce {
			fatal = carbium.NewError(carbium.ErrorInit{Kind: carbium.KindCredits, Code: 1008, Reason: info.Reason, Message: "credits exhausted: buy a plan or upgrade"})
		}
		s.creditsExhaustedOnce = true
		delay = 5 * time.Second
	case info.Code == 1008:
		fatal = carbium.NewError(carbium.ErrorInit{Kind: carbium.KindSocket, Code: 1008, Reason: info.Reason, Message: "the server closed the socket: " + info.Reason})
	case info.Code == 1013 && info.Reason == "credits_pending":
		delay = time.Second + time.Duration(r*float64(500*time.Millisecond))
	case info.Code == 1013:
		limit := min(30*time.Second, time.Second<<min(s.overloadCloses, 5))
		s.overloadCloses++
		delay = max(time.Second, limit/2+time.Duration(r*float64(limit/2)))
	case info.Code == 1012 || info.Code == 1001:
		delay = 250*time.Millisecond + time.Duration(r*float64(750*time.Millisecond))
	default:
		limit := min(30*time.Second, 500*time.Millisecond<<min(s.attempt, 6))
		s.attempt++
		delay = limit/2 + time.Duration(r*float64(limit/2))
		delay = max(delay, retryAfter)
	}
	s.mu.Unlock()
	if fatal != nil {
		s.die(fatal)
		return false
	}
	s.setState(StateBackoff, info)
	return s.sleep(s.ctx, delay) == nil && !s.done()
}

func (s *Socket) read(conn *websocket.Conn) CloseInfo {
	for {
		_, data, err := conn.Read(s.ctx)
		if err != nil {
			var ce websocket.CloseError
			if errors.As(err, &ce) {
				return CloseInfo{Code: int(ce.Code), Reason: ce.Reason}
			}
			return CloseInfo{Code: 1006, Reason: ""}
		}
		s.onMessage(data)
	}
}

func (s *Socket) onMessage(data []byte) {
	var msg struct {
		ID     *uint64         `json:"id"`
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
		Method string          `json:"method"`
		Params *struct {
			Subscription uint64          `json:"subscription"`
			Result       json.RawMessage `json:"result"`
		} `json:"params"`
	}
	if json.Unmarshal(data, &msg) != nil {
		return
	}
	if msg.ID != nil {
		s.mu.Lock()
		p, ok := s.pending[*msg.ID]
		delete(s.pending, *msg.ID)
		failed := len(msg.Error) > 0 && string(msg.Error) != "null"
		var id uint64
		first := false
		if ok && !failed && p.sub != nil && p.gen == s.gen && json.Unmarshal(msg.Result, &id) == nil && s.subs[p.sub.local] == p.sub && p.sub.serverID == 0 {
			p.sub.serverID = id
			s.byServer[id] = p.sub
			first = !p.sub.acked
			p.sub.acked = true
		}
		s.mu.Unlock()
		if first {
			p.sub.settle(id, nil)
		}
		if ok {
			if failed {
				p.ch <- reply{err: carbium.RPCError("", msg.Error)}
			} else {
				p.ch <- reply{result: msg.Result}
			}
		}
		return
	}
	if !strings.HasSuffix(msg.Method, "Notification") || msg.Params == nil {
		return
	}
	s.mu.Lock()
	sub := s.byServer[msg.Params.Subscription]
	if sub == nil {
		s.mu.Unlock()
		return
	}
	s.overloadCloses = 0 // a connection that delivers is not overloaded
	if msg.Method == "signatureNotification" {
		delete(s.byServer, msg.Params.Subscription)
		delete(s.subs, sub.local)
	}
	s.mu.Unlock()
	sub.onNotify(msg.Params.Result)
}

func (s *Socket) request(ctx context.Context, gen uint64, method string, params []any, sub *Subscription) (json.RawMessage, *carbium.Error) {
	s.mu.Lock()
	conn := s.conn
	if conn == nil || s.state != StateOpen || s.gen != gen {
		s.mu.Unlock()
		return nil, socketError(method, "the socket is not open")
	}
	s.nextID++
	id := s.nextID
	ch := make(chan reply, 1)
	s.pending[id] = pendingCall{ch: ch, sub: sub, gen: gen}
	s.mu.Unlock()
	body := map[string]any{"jsonrpc": "2.0", "id": id, "method": method}
	if len(params) > 0 {
		body["params"] = params
	}
	b, _ := json.Marshal(body)
	if err := conn.Write(s.ctx, websocket.MessageText, b); err != nil { // never the caller's context: a done one closes the connection
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
		return nil, carbium.NewError(carbium.ErrorInit{Kind: carbium.KindSocket, Method: method, Message: err.Error(), Cause: err})
	}
	select {
	case r := <-ch:
		if r.err != nil && r.err.Method == "" {
			r.err.Method = method
		}
		return r.result, r.err
	case <-ctx.Done():
		return nil, socketError(method, "the socket ended before the answer")
	}
}

func (s *Socket) bind(sub *Subscription, gen uint64) {
	s.mu.Lock()
	if sub.bindingGen == gen {
		s.mu.Unlock()
		return // a bind for this connection is already under way
	}
	sub.bindingGen = gen
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if sub.bindingGen == gen {
			sub.bindingGen = 0
		}
		s.mu.Unlock()
	}()
	for attempt := 1; ; attempt++ {
		s.mu.Lock()
		if s.gen != gen || sub.serverID != 0 || s.subs[sub.local] != sub {
			s.mu.Unlock()
			return // the connection changed, another bind won, or the caller left
		}
		s.mu.Unlock()
		res, err := s.request(s.ctx, gen, sub.method, sub.params, sub)
		if err != nil {
			if err.Kind == carbium.KindSocket {
				return // the close handler binds again on the next connection
			}
			if err.Code == -32005 && (err.Reason == "" || retryReasons[err.Reason]) && attempt < 4 {
				hint := err.RetryAfter
				if hint == 0 {
					hint = time.Second
				}
				wait := max(hint, min(4*time.Second, 250*time.Millisecond<<(attempt-1))) + time.Duration(s.random()*float64(251*time.Millisecond))
				if s.sleep(s.ctx, wait) != nil {
					return
				}
				continue
			}
			if err.Code == -32002 {
				s.die(err)
				return
			}
			s.mu.Lock()
			delete(s.subs, sub.local)
			acked := sub.acked
			s.mu.Unlock()
			if acked {
				if s.opts.OnError != nil {
					s.opts.OnError(err)
				}
			} else {
				sub.settle(0, err)
			}
			return
		}
		var id uint64
		if json.Unmarshal(res, &id) != nil {
			return
		}
		s.mu.Lock()
		registered, sameConn := sub.serverID == id, s.gen == gen
		s.mu.Unlock()
		if sameConn && !registered {
			// unsubscribed before the ack, or bound twice: free the server subscription nobody holds
			s.notify(gen, strings.TrimSuffix(sub.method, "Subscribe")+"Unsubscribe", []any{id})
		}
		return
	}
}

func (s *Socket) rebindAll(gen uint64) {
	s.mu.Lock()
	all := make([]*Subscription, 0, len(s.subs))
	for _, sub := range s.subs {
		all = append(all, sub)
	}
	s.mu.Unlock()
	for i := 0; i < len(all); i += bindBatch {
		if s.State() != StateOpen {
			return
		}
		var wg sync.WaitGroup
		for _, sub := range all[i:min(i+bindBatch, len(all))] {
			wg.Add(1)
			go func() { defer wg.Done(); s.bind(sub, gen) }()
		}
		wg.Wait()
	}
}
