package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/carbium-sh/carbium-go/stream/geyserpb"
)

var testKey = "c_serv_" + "stream000000000000000"

// fake is an in-memory Geyser server. Each Subscribe call runs script[n] (the last one repeats).
type fake struct {
	unaryErr  error // GetVersion answers it, with a retry-after-ms trailer of 3500
	mu        sync.Mutex
	tokens    []string
	unary     []string
	firsts    []*geyserpb.SubscribeRequest
	pings     []int32
	floor     uint64
	floorErr  error
	subscribe int
	script    []func(s grpc.ServerStream) error
}

func (f *fake) token(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)
	f.mu.Lock()
	f.tokens = append(f.tokens, strings.Join(md.Get("x-token"), ","))
	f.mu.Unlock()
}

func unary[Req, Resp proto.Message](f *fake, name string, newReq func() Req, answer func(Req) (Resp, error)) grpc.MethodDesc {
	return grpc.MethodDesc{MethodName: name, Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
		req := newReq()
		if err := dec(req); err != nil {
			return nil, err
		}
		f.token(ctx)
		f.mu.Lock()
		f.unary = append(f.unary, name)
		uerr := f.unaryErr
		f.mu.Unlock()
		if uerr != nil && name == "GetVersion" {
			_ = grpc.SetTrailer(ctx, metadata.Pairs("retry-after-ms", "3500"))
			return nil, uerr
		}
		return answer(req)
	}}
}

func start(t *testing.T, f *fake, extra ...grpc.DialOption) *Client {
	t.Helper()
	desc := grpc.ServiceDesc{ServiceName: "geyser.Geyser", HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			unary(f, "GetSlot", func() *geyserpb.GetSlotRequest { return new(geyserpb.GetSlotRequest) }, func(*geyserpb.GetSlotRequest) (*geyserpb.GetSlotResponse, error) {
				return &geyserpb.GetSlotResponse{Slot: 321}, nil
			}),
			unary(f, "GetBlockHeight", func() *geyserpb.GetBlockHeightRequest { return new(geyserpb.GetBlockHeightRequest) }, func(*geyserpb.GetBlockHeightRequest) (*geyserpb.GetBlockHeightResponse, error) {
				return &geyserpb.GetBlockHeightResponse{BlockHeight: 300}, nil
			}),
			unary(f, "GetLatestBlockhash", func() *geyserpb.GetLatestBlockhashRequest { return new(geyserpb.GetLatestBlockhashRequest) }, func(*geyserpb.GetLatestBlockhashRequest) (*geyserpb.GetLatestBlockhashResponse, error) {
				return &geyserpb.GetLatestBlockhashResponse{Blockhash: "H"}, nil
			}),
			unary(f, "IsBlockhashValid", func() *geyserpb.IsBlockhashValidRequest { return new(geyserpb.IsBlockhashValidRequest) }, func(r *geyserpb.IsBlockhashValidRequest) (*geyserpb.IsBlockhashValidResponse, error) {
				return &geyserpb.IsBlockhashValidResponse{Valid: r.Blockhash == "H"}, nil
			}),
			unary(f, "GetVersion", func() *geyserpb.GetVersionRequest { return new(geyserpb.GetVersionRequest) }, func(*geyserpb.GetVersionRequest) (*geyserpb.GetVersionResponse, error) {
				return &geyserpb.GetVersionResponse{Version: "v"}, nil
			}),
			unary(f, "Ping", func() *geyserpb.PingRequest { return new(geyserpb.PingRequest) }, func(r *geyserpb.PingRequest) (*geyserpb.PongResponse, error) {
				return &geyserpb.PongResponse{Count: r.Count}, nil
			}),
			unary(f, "SubscribeReplayInfo", func() *geyserpb.SubscribeReplayInfoRequest { return new(geyserpb.SubscribeReplayInfoRequest) }, func(*geyserpb.SubscribeReplayInfoRequest) (*geyserpb.SubscribeReplayInfoResponse, error) {
				f.mu.Lock()
				defer f.mu.Unlock()
				if f.floorErr != nil {
					return nil, f.floorErr
				}
				return &geyserpb.SubscribeReplayInfoResponse{FirstAvailable: proto.Uint64(f.floor)}, nil
			}),
		},
		Streams: []grpc.StreamDesc{{StreamName: "Subscribe", ServerStreams: true, ClientStreams: true, Handler: func(_ any, s grpc.ServerStream) error {
			f.token(s.Context())
			first := new(geyserpb.SubscribeRequest)
			if err := s.RecvMsg(first); err != nil {
				return err
			}
			f.mu.Lock()
			f.subscribe++
			n := f.subscribe
			f.firsts = append(f.firsts, first)
			var step func(grpc.ServerStream) error
			if len(f.script) > 0 {
				step = f.script[min(n, len(f.script))-1]
			}
			f.mu.Unlock()
			if step == nil {
				<-s.Context().Done()
				return nil
			}
			return step(s)
		}}},
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	srv.RegisterService(&desc, struct{}{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	c, err := Connect(testKey, &Options{Endpoint: "passthrough:///bufnet", DialOptions: append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, extra...)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func send(s grpc.ServerStream, u *geyserpb.SubscribeUpdate) { _ = s.SendMsg(u) }

func slotUpdate(slot uint64) *geyserpb.SubscribeUpdate {
	return &geyserpb.SubscribeUpdate{UpdateOneof: &geyserpb.SubscribeUpdate_Slot{Slot: &geyserpb.SubscribeUpdateSlot{Slot: slot}}}
}

// recorder replaces the sleep and the jitter, and collects the status events.
type recorder struct {
	all     int // every OnUpdate call
	mu      sync.Mutex
	delays  []time.Duration
	events  []Status
	reseeds []*uint64
	updates []uint64
}

func (r *recorder) hook(c *Client) Handlers {
	c.random = func() float64 { return 0 }
	c.sleep = func(ctx context.Context, d time.Duration) error {
		r.mu.Lock()
		r.delays = append(r.delays, d)
		r.mu.Unlock()
		return ctx.Err()
	}
	return Handlers{
		OnUpdate: func(u *geyserpb.SubscribeUpdate) {
			r.mu.Lock()
			r.all++
			if s, ok := slotOf(u); ok {
				r.updates = append(r.updates, s)
			}
			r.mu.Unlock()
		},
		OnReseed: func(_ context.Context, last *uint64) {
			r.mu.Lock()
			r.reseeds = append(r.reseeds, last)
			r.mu.Unlock()
		},
		OnStatus: func(s Status) {
			r.mu.Lock()
			r.events = append(r.events, s)
			r.mu.Unlock()
		},
	}
}

func (r *recorder) waits() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.delays...)
}

func waitDone(t *testing.T, rc *Reconnecting) {
	t.Helper()
	select {
	case <-rc.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the stream did not end within 5 s")
	}
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within 5 s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (f *fake) firstRequests() []*geyserpb.SubscribeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*geyserpb.SubscribeRequest(nil), f.firsts...)
}

func TestTheSevenUnaryCallsCarryTheKeyAsXToken(t *testing.T) {
	f := &fake{}
	c := start(t, f)
	ctx := context.Background()
	if r, err := c.GetSlot(ctx, nil); err != nil || r.Slot != 321 {
		t.Fatal(r, err)
	}
	if r, err := c.GetBlockHeight(ctx, nil); err != nil || r.BlockHeight != 300 {
		t.Fatal(r, err)
	}
	if r, err := c.GetLatestBlockhash(ctx, nil); err != nil || r.Blockhash != "H" {
		t.Fatal(r, err)
	}
	if r, err := c.IsBlockhashValid(ctx, "H", nil); err != nil || !r.Valid {
		t.Fatal(r, err)
	}
	if r, err := c.GetVersion(ctx); err != nil || r.Version != "v" {
		t.Fatal(r, err)
	}
	if r, err := c.Ping(ctx, 3); err != nil || r.Count != 3 {
		t.Fatal(r, err)
	}
	if _, err := c.SubscribeReplayInfo(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.tokens) != 7 {
		t.Fatalf("%d calls", len(f.tokens))
	}
	for _, tok := range f.tokens {
		if tok != testKey {
			t.Fatal("every call carries the key as x-token")
		}
	}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		if strings.Contains(fmt.Sprintf(verb, c), testKey) || strings.Contains(fmt.Sprintf(verb, reflect.ValueOf(c).Elem().Interface()), testKey) {
			t.Fatalf("the key prints with %s", verb)
		}
	}
}

// The served filters and unary calls, counted both ways against the module's copy of served.json.
func TestTheServedFiltersAndCallsAndNothingElse(t *testing.T) {
	b, err := os.ReadFile("testdata/served.json")
	if err != nil {
		t.Fatal(err)
	}
	var served struct {
		Stream struct {
			Filters []string `json:"filters"`
			Unary   []string `json:"unary"`
		} `json:"stream"`
	}
	if err := json.Unmarshal(b, &served); err != nil {
		t.Fatal(err)
	}
	var filters []string
	ft := reflect.TypeOf(Filters{})
	for i := 0; i < ft.NumField(); i++ {
		if ft.Field(i).Type.Kind() == reflect.Map {
			n := ft.Field(i).Name
			filters = append(filters, strings.ToLower(n[:1])+n[1:])
		}
	}
	var calls []string
	ct := reflect.TypeOf(&Client{})
	for i := 0; i < ct.NumMethod(); i++ {
		switch n := ct.Method(i).Name; n {
		case "Subscribe", "SubscribeWithReconnect", "Close", "Format":
		default:
			calls = append(calls, n)
		}
	}
	for _, l := range [][]string{filters, calls, served.Stream.Filters, served.Stream.Unary} {
		sort.Strings(l)
	}
	if !reflect.DeepEqual(filters, served.Stream.Filters) {
		t.Errorf("filters %v, served %v", filters, served.Stream.Filters)
	}
	if !reflect.DeepEqual(calls, served.Stream.Unary) {
		t.Errorf("unary calls %v, served %v", calls, served.Stream.Unary)
	}
}

func TestBuildRequestRefusesWhatTheServiceRefuses(t *testing.T) {
	processed, confirmed := geyserpb.CommitmentLevel_PROCESSED, geyserpb.CommitmentLevel_CONFIRMED
	slot := uint64(5)
	tf := geyserpb.TokenAccountExpansionControlFlag(1)
	for name, f := range map[string]Filters{
		"cuckoo accounts":       {Accounts: map[string]*geyserpb.SubscribeRequestFilterAccounts{"a": {CuckooAccountsFilter: &geyserpb.CuckooFilter{}}}},
		"token accounts":        {Transactions: map[string]*geyserpb.SubscribeRequestFilterTransactions{"t": {AccountInclude: []string{"P"}, TokenAccounts: &tf}}},
		"cuckoo transactions":   {TransactionsStatus: map[string]*geyserpb.SubscribeRequestFilterTransactions{"t": {AccountInclude: []string{"P"}, CuckooAccountInclude: &geyserpb.CuckooFilter{}}}},
		"bare transactions":     {Transactions: map[string]*geyserpb.SubscribeRequestFilterTransactions{"t": {}}},
		"cuckoo blocks":         {Blocks: map[string]*geyserpb.SubscribeRequestFilterBlocks{"b": {CuckooAccountInclude: &geyserpb.CuckooFilter{}}}},
		"include update parent": {Entry: map[string]*geyserpb.SubscribeRequestFilterEntry{"e": {IncludeUpdateParent: proto.Bool(true)}}},
		"from slot confirmed":   {Slots: map[string]*geyserpb.SubscribeRequestFilterSlots{"s": {}}, FromSlot: &slot, Commitment: &confirmed},
	} {
		var e *Error
		if _, err := BuildRequest(f); !errors.As(err, &e) || e.Kind != "request" {
			t.Errorf("%s: %v, want a request refusal", name, err)
		}
	}
	r, err := BuildRequest(Filters{Slots: map[string]*geyserpb.SubscribeRequestFilterSlots{"s": {}}, FromSlot: &slot, Commitment: &processed})
	if err != nil || r.GetFromSlot() != 5 || len(r.Slots) != 1 {
		t.Fatal(r, err)
	}
}

func TestTheFilterSetGoesFirstAndPingsAndPongsNeverReachOnUpdate(t *testing.T) {
	f := &fake{script: []func(grpc.ServerStream) error{func(s grpc.ServerStream) error {
		send(s, &geyserpb.SubscribeUpdate{UpdateOneof: &geyserpb.SubscribeUpdate_Ping{Ping: &geyserpb.SubscribeUpdatePing{}}})
		send(s, &geyserpb.SubscribeUpdate{UpdateOneof: &geyserpb.SubscribeUpdate_Pong{Pong: &geyserpb.SubscribeUpdatePong{Id: 1}}})
		send(s, slotUpdate(10))
		send(s, slotUpdate(11))
		<-s.Context().Done()
		return nil
	}}}
	c := start(t, f)
	r := &recorder{}
	h := r.hook(c)
	rc, err := c.SubscribeWithReconnect(context.Background(), Filters{
		Slots:        map[string]*geyserpb.SubscribeRequestFilterSlots{"s": {}},
		Transactions: map[string]*geyserpb.SubscribeRequestFilterTransactions{"pool": {AccountInclude: []string{"P"}, Vote: proto.Bool(false)}},
	}, h)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "two updates", func() bool { r.mu.Lock(); defer r.mu.Unlock(); return len(r.updates) == 2 })
	rc.Close()
	waitDone(t, rc)
	first := f.firstRequests()[0]
	if len(first.Slots) != 1 || first.Transactions["pool"].AccountInclude[0] != "P" || rc.Err() != nil {
		t.Fatalf("%v %v", first, rc.Err())
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.all != 2 || !reflect.DeepEqual(r.updates, []uint64{10, 11}) {
		t.Fatalf("OnUpdate ran %d times with %v: pings and pongs must never reach it", r.all, r.updates)
	}
	if f.tokens[0] != testKey {
		t.Fatal("the stream itself must carry the key as x-token")
	}
}

func TestStopStatusesEndTheStream(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{
		{status.Error(codes.Unauthenticated, "bad key"), codes.Unauthenticated},
		{status.Error(codes.PermissionDenied, "no grpc scope"), codes.PermissionDenied},
		{status.Error(codes.InvalidArgument, "bad filter"), codes.InvalidArgument},
		{status.Error(codes.Unimplemented, "no"), codes.Unimplemented},
		{status.Error(codes.ResourceExhausted, "credits exhausted"), codes.ResourceExhausted},
	} {
		f := &fake{script: []func(grpc.ServerStream) error{func(grpc.ServerStream) error { return tc.err }}}
		c := start(t, f)
		r := &recorder{}
		rc, err := c.SubscribeWithReconnect(context.Background(), Filters{Slots: map[string]*geyserpb.SubscribeRequestFilterSlots{"s": {}}}, r.hook(c))
		if err != nil {
			t.Fatal(err)
		}
		waitDone(t, rc)
		var e *Error
		if !errors.As(rc.Err(), &e) || e.Kind != "stream" || e.Code != tc.code || e.Retryable || f.subscribe != 1 {
			t.Errorf("%v: %v after %d subscribes", tc.code, rc.Err(), f.subscribe)
		}
	}
}

func TestResourceExhaustedOtherThanCreditsIsRetried(t *testing.T) {
	info := Classify(status.Error(codes.ResourceExhausted, "too many streams"))
	if info.Action != ActionRetry {
		t.Fatalf("%+v", info)
	}
	if info := Classify(status.Error(codes.Unavailable, "index still loading")); info.Action != ActionRetry || info.RetryAfter != time.Second {
		t.Fatalf("%+v", info)
	}
}

func TestOutOfRangeReseedsThenSubscribesLiveAtOnce(t *testing.T) {
	f := &fake{script: []func(grpc.ServerStream) error{
		func(s grpc.ServerStream) error {
			send(s, slotUpdate(50))
			return status.Error(codes.OutOfRange, "past the retained range")
		},
		nil,
	}}
	c := start(t, f)
	r := &recorder{}
	from := uint64(40)
	rc, err := c.SubscribeWithReconnect(context.Background(), Filters{Slots: map[string]*geyserpb.SubscribeRequestFilterSlots{"s": {}}, FromSlot: &from}, r.hook(c))
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "the second subscribe", func() bool { return len(f.firstRequests()) == 2 })
	rc.Close()
	waitDone(t, rc)
	firsts := f.firstRequests()
	if firsts[0].GetFromSlot() != 40 || firsts[1].FromSlot != nil || len(r.reseeds) != 1 || *r.reseeds[0] != 50 || len(r.waits()) != 0 {
		t.Fatalf("from %v then %v, reseeds %v, waits %v", firsts[0].FromSlot, firsts[1].FromSlot, r.reseeds, r.waits())
	}
}

func TestARetryWaitsTheTableThenResumesFromTheLastSlotOrReseeds(t *testing.T) {
	unavailable := func(slot uint64) func(grpc.ServerStream) error {
		return func(s grpc.ServerStream) error {
			send(s, slotUpdate(slot))
			return status.Error(codes.Unavailable, "restarting")
		}
	}
	f := &fake{floor: 90, script: []func(grpc.ServerStream) error{unavailable(100), unavailable(101), nil}}
	c := start(t, f)
	r := &recorder{}
	rc, err := c.SubscribeWithReconnect(context.Background(), Filters{Slots: map[string]*geyserpb.SubscribeRequestFilterSlots{"s": {}}}, r.hook(c))
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "the third subscribe", func() bool { return len(f.firstRequests()) == 3 })
	rc.Close()
	waitDone(t, rc)
	firsts := f.firstRequests()
	if firsts[1].GetFromSlot() != 100 || firsts[2].GetFromSlot() != 101 {
		t.Fatalf("resumed from %d and %d, want 100 and 101", firsts[1].GetFromSlot(), firsts[2].GetFromSlot())
	}
	if w := r.waits(); !reflect.DeepEqual(w, []time.Duration{2 * time.Second, 2 * time.Second}) {
		t.Fatalf("waits %v: at least the table's 2 s, and data resets the backoff", w)
	}

	f2 := &fake{floor: 500, script: []func(grpc.ServerStream) error{unavailable(100), nil}}
	c2 := start(t, f2)
	r2 := &recorder{}
	rc2, _ := c2.SubscribeWithReconnect(context.Background(), Filters{Slots: map[string]*geyserpb.SubscribeRequestFilterSlots{"s": {}}}, r2.hook(c2))
	eventually(t, "the second subscribe", func() bool { return len(f2.firstRequests()) == 2 })
	rc2.Close()
	waitDone(t, rc2)
	if f2.firstRequests()[1].FromSlot != nil || len(r2.reseeds) != 1 {
		t.Fatalf("below the floor it must re-seed and go live: %v, %d reseeds", f2.firstRequests()[1].FromSlot, len(r2.reseeds))
	}
}

func TestTheBackoffDoublesWithoutDataAndStopsAt30s(t *testing.T) {
	f := &fake{script: []func(grpc.ServerStream) error{func(grpc.ServerStream) error { return status.Error(codes.Unavailable, "down") }}}
	c := start(t, f)
	r := &recorder{}
	rc, _ := c.SubscribeWithReconnect(context.Background(), Filters{Slots: map[string]*geyserpb.SubscribeRequestFilterSlots{"s": {}}}, r.hook(c))
	eventually(t, "seven retries", func() bool { return len(r.waits()) >= 6 })
	rc.Close()
	waitDone(t, rc)
	want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	if w := r.waits()[:6]; !reflect.DeepEqual(w, want) {
		t.Fatalf("waits %v, want %v", w, want)
	}
}

func TestThePingGoesInTheStreamWithOneUnanswered(t *testing.T) {
	var pings []int32
	var mu sync.Mutex
	f := &fake{script: []func(grpc.ServerStream) error{func(s grpc.ServerStream) error {
		for {
			req := new(geyserpb.SubscribeRequest)
			if err := s.RecvMsg(req); err != nil {
				return nil
			}
			mu.Lock()
			pings = append(pings, req.GetPing().GetId())
			n := len(pings)
			mu.Unlock()
			if n == 1 {
				send(s, &geyserpb.SubscribeUpdate{UpdateOneof: &geyserpb.SubscribeUpdate_Pong{Pong: &geyserpb.SubscribeUpdatePong{Id: 1}}})
			}
		}
	}}}
	c := start(t, f)
	r := &recorder{}
	h := r.hook(c)
	c.pingEvery = 20 * time.Millisecond
	rc, _ := c.SubscribeWithReconnect(context.Background(), Filters{Slots: map[string]*geyserpb.SubscribeRequestFilterSlots{"s": {}}}, h)
	time.Sleep(300 * time.Millisecond)
	rc.Close()
	waitDone(t, rc)
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(pings, []int32{1, 2}) {
		t.Fatalf("pings %v: one ping, its pong, then one more left unanswered", pings)
	}
}

func TestAKeyOfTheWrongKindOrInTheEndpointIsRefused(t *testing.T) {
	var e *Error
	if _, err := Connect("c_acct_"+"Abc123Def", nil); !errors.As(err, &e) || e.Kind != "auth" {
		t.Fatal(err)
	}
	if _, err := Connect(testKey, &Options{Endpoint: "services.carbium.sh:9443/" + testKey}); !errors.As(err, &e) || e.Kind != "request" || strings.Contains(err.Error(), testKey) {
		t.Fatal(err)
	}
}

func slotsOnly() Filters {
	return Filters{Slots: map[string]*geyserpb.SubscribeRequestFilterSlots{"s": {}}}
}

func TestATransactionsStatusStreamResumesFromItsLastSlot(t *testing.T) {
	status1 := func(s grpc.ServerStream) error {
		send(s, &geyserpb.SubscribeUpdate{UpdateOneof: &geyserpb.SubscribeUpdate_TransactionStatus{TransactionStatus: &geyserpb.SubscribeUpdateTransactionStatus{Slot: 200}}})
		return status.Error(codes.Unavailable, "restarting")
	}
	f := &fake{floor: 100, script: []func(grpc.ServerStream) error{status1, nil}}
	c := start(t, f)
	r := &recorder{}
	rc, err := c.SubscribeWithReconnect(context.Background(), Filters{TransactionsStatus: map[string]*geyserpb.SubscribeRequestFilterTransactions{"t": {AccountInclude: []string{"P"}}}}, r.hook(c))
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "the second subscribe", func() bool { return len(f.firstRequests()) == 2 })
	rc.Close()
	waitDone(t, rc)
	if got := f.firstRequests()[1].GetFromSlot(); got != 200 {
		t.Fatalf("resumed from %d, want 200: a transaction status frame carries its slot", got)
	}
}

func TestACallersInterceptorStillSendsTheKey(t *testing.T) {
	var unaryRan, streamRan bool
	f := &fake{script: []func(grpc.ServerStream) error{func(s grpc.ServerStream) error { send(s, slotUpdate(1)); <-s.Context().Done(); return nil }}}
	c := start(t, f,
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, o ...grpc.CallOption) error {
			unaryRan = true
			return invoker(ctx, method, req, reply, cc, o...)
		}),
		grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, o ...grpc.CallOption) (grpc.ClientStream, error) {
			streamRan = true
			return streamer(ctx, desc, cc, method, o...)
		}))
	if _, err := c.GetSlot(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := c.Subscribe(ctx, slotsOnly())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Recv(); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !unaryRan || !streamRan || len(f.tokens) != 2 || f.tokens[0] != testKey || f.tokens[1] != testKey {
		t.Fatalf("interceptors ran %v %v; tokens seen %d: the key must still go on both", unaryRan, streamRan, len(f.tokens))
	}
}

func reconnectGoroutines() bool {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Contains(string(buf[:n]), "stream.(*Client).SubscribeWithReconnect")
}

func TestClosingTheClientEndsAReconnectingStream(t *testing.T) {
	f := &fake{script: []func(grpc.ServerStream) error{func(grpc.ServerStream) error { return status.Error(codes.Unavailable, "down") }}}
	c := start(t, f)
	r := &recorder{}
	rc, err := c.SubscribeWithReconnect(context.Background(), slotsOnly(), r.hook(c))
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "a retry", func() bool { return len(r.waits()) >= 1 })
	_ = c.Close()
	waitDone(t, rc)
	eventually(t, "no reconnect goroutine left", func() bool { return !reconnectGoroutines() })
}

func TestTheCallersMapsCannotChangeARunningStream(t *testing.T) {
	again := func(s grpc.ServerStream) error {
		send(s, slotUpdate(5))
		time.Sleep(50 * time.Millisecond)
		return status.Error(codes.Unavailable, "restarting")
	}
	f := &fake{script: []func(grpc.ServerStream) error{again, nil}}
	c := start(t, f)
	r := &recorder{}
	h := r.hook(c)
	c.pingEvery = time.Millisecond
	filters := slotsOnly()
	rc, err := c.SubscribeWithReconnect(context.Background(), filters, h)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		filters.Slots[fmt.Sprintf("added%d", i)] = &geyserpb.SubscribeRequestFilterSlots{}
		time.Sleep(time.Millisecond)
	}
	eventually(t, "the second subscribe", func() bool { return len(f.firstRequests()) == 2 })
	rc.Close()
	waitDone(t, rc)
	if got := f.firstRequests()[1].Slots; len(got) != 1 {
		t.Fatalf("the resubscribe sent %d slot filters, want the 1 the stream opened with", len(got))
	}
}

func TestARetryAfterTrailerIsHonoured(t *testing.T) {
	f := &fake{script: []func(grpc.ServerStream) error{func(s grpc.ServerStream) error {
		s.SetTrailer(metadata.Pairs("retry-after", "7"))
		return status.Error(codes.Unavailable, "draining")
	}, nil}}
	c := start(t, f)
	r := &recorder{}
	rc, _ := c.SubscribeWithReconnect(context.Background(), slotsOnly(), r.hook(c))
	eventually(t, "the second subscribe", func() bool { return len(f.firstRequests()) == 2 })
	rc.Close()
	waitDone(t, rc)
	if w := r.waits(); len(w) != 1 || w[0] != 7*time.Second {
		t.Fatalf("waits %v, want the trailer's 7 s over the table's 2 s", w)
	}
	f.unaryErr = status.Error(codes.Unavailable, "busy")
	_, err := c.GetVersion(context.Background())
	var e *Error
	if !errors.As(err, &e) || e.RetryAfter != 3500*time.Millisecond || !e.Retryable {
		t.Fatalf("%+v", err)
	}
}

func TestNilFilterValuesAreRefusedOrReadSafely(t *testing.T) {
	var e *Error
	if _, err := BuildRequest(Filters{Transactions: map[string]*geyserpb.SubscribeRequestFilterTransactions{"t": nil}}); !errors.As(err, &e) || e.Kind != "request" {
		t.Fatalf("a nil transaction filter: %v", err)
	}
	if _, err := BuildRequest(Filters{TransactionsStatus: map[string]*geyserpb.SubscribeRequestFilterTransactions{"t": nil}}); !errors.As(err, &e) || e.Kind != "request" {
		t.Fatalf("a nil transaction status filter: %v", err)
	}
	if _, err := BuildRequest(Filters{Entry: map[string]*geyserpb.SubscribeRequestFilterEntry{"e": nil}, Accounts: map[string]*geyserpb.SubscribeRequestFilterAccounts{"a": nil}, Blocks: map[string]*geyserpb.SubscribeRequestFilterBlocks{"b": nil}}); err != nil {
		t.Fatalf("nil entry, account and block filters read as empty: %v", err)
	}
}

func TestNoResumeAtConfirmedOrFinalized(t *testing.T) {
	for _, level := range []geyserpb.CommitmentLevel{geyserpb.CommitmentLevel_CONFIRMED, geyserpb.CommitmentLevel_FINALIZED} {
		f := &fake{script: []func(grpc.ServerStream) error{func(s grpc.ServerStream) error { send(s, slotUpdate(9)); return status.Error(codes.Unavailable, "x") }, nil}}
		c := start(t, f)
		r := &recorder{}
		filters := slotsOnly()
		filters.Commitment = &level
		rc, _ := c.SubscribeWithReconnect(context.Background(), filters, r.hook(c))
		eventually(t, "the second subscribe", func() bool { return len(f.firstRequests()) == 2 })
		rc.Close()
		waitDone(t, rc)
		f.mu.Lock()
		unary := append([]string(nil), f.unary...)
		f.mu.Unlock()
		if f.firstRequests()[1].FromSlot != nil || len(unary) != 0 {
			t.Fatalf("%v: from_slot %v, unary calls %v: no resume and no replay read off processed", level, f.firstRequests()[1].FromSlot, unary)
		}
	}
}

func TestTheJitterStaysUnderASecond(t *testing.T) {
	f := &fake{script: []func(grpc.ServerStream) error{func(grpc.ServerStream) error { return status.Error(codes.Unavailable, "x") }, nil}}
	c := start(t, f)
	r := &recorder{}
	h := r.hook(c)
	c.random = func() float64 { return 0.999999 }
	rc, _ := c.SubscribeWithReconnect(context.Background(), slotsOnly(), h)
	eventually(t, "a retry", func() bool { return len(r.waits()) >= 1 })
	rc.Close()
	waitDone(t, rc)
	if w := r.waits()[0]; w < 2*time.Second || w >= 3*time.Second {
		t.Fatalf("wait %v, want the 2 s table plus less than 1 s of jitter", w)
	}
}

func TestPlainSubscribeSendsTheFiltersAndReads(t *testing.T) {
	f := &fake{script: []func(grpc.ServerStream) error{func(s grpc.ServerStream) error { send(s, slotUpdate(42)); <-s.Context().Done(); return nil }}}
	c := start(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := c.Subscribe(ctx, slotsOnly())
	if err != nil {
		t.Fatal(err)
	}
	u, err := sub.Recv()
	if err != nil || u.GetSlot().GetSlot() != 42 || len(f.firstRequests()[0].Slots) != 1 {
		t.Fatalf("%v %v", u, err)
	}
	if _, err := c.Subscribe(ctx, Filters{Transactions: map[string]*geyserpb.SubscribeRequestFilterTransactions{"t": {}}}); err == nil {
		t.Fatal("a refused filter set must fail before any call")
	}
}
