package carbium

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
)

// Answers in the shapes the service sends decode into the generated types, a u64 rentEpoch included.
func TestRecordedAnswersDecode(t *testing.T) {
	p := plant(t,
		reply{body: ok(`{"context":{"slot":9,"apiVersion":"3.1.0"},"value":{"lamports":5,"owner":"11111111111111111111111111111111","data":["AQ==","base64"],"executable":false,"rentEpoch":18446744073709551615,"space":0}}`)},
		reply{body: ok(`{"context":{"slot":9},"value":{"lamports":5,"owner":"T","data":{"program":"spl-token","parsed":{"type":"mint"},"space":82},"executable":false,"rentEpoch":0}}`)},
		reply{body: ok(`[{"pubkey":"A","account":{"lamports":1,"owner":"P","data":["","base64"],"executable":false,"rentEpoch":0}}]`)},
		reply{body: ok(`{"context":{"slot":3},"value":[{"pubkey":"A","account":{"lamports":1,"owner":"P","data":["","base64"],"executable":false,"rentEpoch":0}}]}`)},
		reply{body: ok(`{"slot":7,"blockTime":null,"version":0,"transaction":["AQ==","base64"],"meta":{"err":{"InstructionError":[0,{"Custom":1}]},"fee":5000,"preBalances":[1],"postBalances":[2],"rewards":[{"pubkey":"V","lamports":-12,"postBalance":3,"rewardType":"Rent"}],"status":{"Err":{"InstructionError":[0,{"Custom":1}]}}}}`)},
		reply{body: ok(`{"context":{"slot":1},"value":[null,{"slot":4,"confirmations":null,"err":null,"confirmationStatus":"finalized","status":{"Ok":null}}]}`)},
		reply{body: ok(`{"current":[{"votePubkey":"V","nodePubkey":"N","activatedStake":42,"epochVoteAccount":true,"commission":10,"lastVote":8,"epochCredits":[[1,64,0],[2,128,64]],"rootSlot":7}],"delinquent":[]}`)},
	)
	c, _ := testClient(t, p, nil)
	ctx := context.Background()

	a, err := c.GetAccountInfo(ctx, "x", nil)
	if err != nil || a.Value == nil || a.Value.RentEpoch != math.MaxUint64 || a.Value.Data.Data != "AQ==" || a.Value.Data.Encoding != AccountDataEncodingBase64 || a.Context.Slot != 9 {
		t.Fatalf("%+v %v", a, err)
	}
	parsed, err := c.GetAccountInfo(ctx, "x", &AccountInfoConfig{Encoding: EncodingJSONParsed})
	if err != nil || parsed.Value.Data.Parsed == nil || parsed.Value.Data.Parsed.Program != "spl-token" || parsed.Value.Data.Parsed.Space != 82 {
		t.Fatalf("%+v %v", parsed, err)
	}
	plain, err := c.GetProgramAccounts(ctx, "p", nil)
	if err != nil || plain.Context != nil || len(plain.Value) != 1 {
		t.Fatalf("%+v %v", plain, err)
	}
	withCtx, err := c.GetProgramAccounts(ctx, "p", &ProgramAccountsConfig{WithContext: Ptr(true)})
	if err != nil || withCtx.Context == nil || withCtx.Context.Slot != 3 || len(withCtx.Value) != 1 {
		t.Fatalf("%+v %v", withCtx, err)
	}
	tx, err := c.GetTransaction(ctx, "sig", nil)
	if err != nil || tx.Version != TxVersion0 || tx.BlockTime != nil || tx.Meta.Err == nil || tx.Meta.Status.OK() || tx.Meta.Rewards[0].Lamports != -12 {
		t.Fatalf("%+v %v", tx, err)
	}
	st, err := c.GetSignatureStatuses(ctx, []string{"a", "b"}, nil)
	if err != nil || st.Value[0] != nil || st.Value[1].Err != nil || !st.Value[1].Status.OK() || *st.Value[1].ConfirmationStatus != CommitmentFinalized {
		t.Fatalf("%+v %v", st, err)
	}
	votes, err := c.GetVoteAccounts(ctx, nil)
	if err != nil || votes.Current[0].EpochCredits[1].Credits != 128 || votes.Current[0].EpochCredits[1].PreviousCredits != 64 {
		t.Fatalf("%+v %v", votes, err)
	}
}

func TestUnionsWriteTheShapeTheServiceTakes(t *testing.T) {
	for _, tc := range []struct {
		v    any
		want string
	}{
		{DASGrouping{Key: "collection", Value: "C"}, `["collection","C"]`},
		{TxVersionLegacy, `"legacy"`},
		{TxVersion1, `1`},
		{TxStatus{}, `{"Ok":null}`},
		{TokenAccountsFilter{Mint: "M"}, `{"mint":"M"}`},
		{AccountInfoConfig{Commitment: CommitmentConfirmed, MinContextSlot: Ptr[uint64](0)}, `{"commitment":"confirmed","minContextSlot":0}`},
		{QuoteParams{SrcMint: "a", DstMint: "b", AmountIn: "100000000", SlippageBps: 50, Dex: []string{"x"}}, `{"src_mint":"a","dst_mint":"b","amount_in":"100000000","slippage_bps":50,"dex":["x"]}`},
	} {
		b, err := json.Marshal(tc.v)
		if err != nil || string(b) != tc.want {
			t.Errorf("%T: %s %v, want %s", tc.v, b, err, tc.want)
		}
	}
	var g DASGrouping
	if json.Unmarshal([]byte(`{"group_key":"collection","group_value":"C"}`), &g) != nil || g != (DASGrouping{"collection", "C"}) {
		t.Fatal(g)
	}
	var e TransactionError
	if json.Unmarshal([]byte(`"AccountInUse"`), &e) != nil || e.String() != `"AccountInUse"` {
		t.Fatal(e)
	}
}

// One wrong spec type must not make a call unusable: extra fields are dropped, an unknown enum value is kept, a
// shape mismatch names the method and the field path, and the raw answer stays readable through Call.
func TestDecodingRules(t *testing.T) {
	p := plant(t,
		reply{body: ok(`{"context":{"slot":1,"brandNew":{"x":1}},"value":{"lamports":5,"owner":"o","data":["","base64"],"executable":false,"rentEpoch":0,"extra":"kept out"}}`)},
		reply{body: ok(`{"context":{"slot":1},"value":[{"slot":4,"confirmations":null,"err":null,"confirmationStatus":"optimistic"}]}`)},
		reply{body: ok(`{"context":{"slot":1},"value":{"lamports":"five","owner":"o","data":["","base64"],"executable":false,"rentEpoch":0}}`)},
		reply{body: ok(`{"context":{"slot":1},"value":{"lamports":"five"}}`)},
	)
	c, _ := testClient(t, p, nil)
	ctx := context.Background()
	if a, err := c.GetAccountInfo(ctx, "x", nil); err != nil || a.Value.Lamports != 5 {
		t.Fatalf("an extra field must not fail: %v", err)
	}
	st, err := c.GetSignatureStatuses(ctx, []string{"s"}, nil)
	if err != nil || string(*st.Value[0].ConfirmationStatus) != "optimistic" {
		t.Fatalf("an unknown enum value must be kept: %+v %v", st, err)
	}
	_, err = c.GetAccountInfo(ctx, "x", nil)
	e := asError(t, err)
	if e.Kind != KindUpstream || e.Method != "getAccountInfo" || !strings.Contains(e.Message, "getAccountInfo") || !strings.Contains(e.Message, "value.lamports") {
		t.Fatalf("a wrong type must name the method and the field path: %v", e)
	}
	var raw json.RawMessage
	if err := c.Call(ctx, "getAccountInfo", []any{"x"}, &raw); err != nil || !strings.Contains(string(raw), `"lamports":"five"`) {
		t.Fatalf("the raw answer must stay readable: %s %v", raw, err)
	}
}

// The shapes the rebuilt spec added, and the two leniencies of the review: a tuple with an extra item, and a
// number where the spec allows a number or a string.
func TestTheRebuiltSpecShapesDecode(t *testing.T) {
	p := plant(t,
		reply{body: ok(`{"context":{"slot":1},"value":{"lamports":5,"owner":"o","data":"3Bxs4h24hBtQy9rw","executable":false,"rentEpoch":0}}`)},
		reply{body: ok(`{"blockhash":"B","previousBlockhash":"P","parentSlot":4,"blockHeight":null,"blockTime":null,"transactions":[{"transaction":{"signatures":["S"],"accountKeys":[{"pubkey":"K","signer":true,"writable":true,"source":"transaction"}]},"meta":null,"version":0}]}`)},
		reply{body: ok(`{"solUsd":140.5,"builtUnix":1700000000,"priced":2,"stale":0,"noroute":0,"total":2,"rows":[["M",1.0,1000.5,"Q","a column added later"]]}`)},
		reply{body: ok(`{"ok":true,"quoteUpstream":{},"canBuild":true,"families":[],"poolStore":{},"chainRpc":{},"priced":2,"total":2,"stale":0,"tableAgeSecs":3,"quote_refusal_ratio":0}`)},
		reply{body: ok(`[{"epoch":800,"effectiveSlot":9,"amount":5,"postBalance":7,"commission":null},null]`)},
	)
	c, _ := testClient(t, p, nil)
	ctx := context.Background()
	a, err := c.GetAccountInfo(ctx, "x", nil)
	if err != nil || a.Value.Data.Data != "3Bxs4h24hBtQy9rw" || a.Value.Data.Encoding != AccountDataEncodingBase58 {
		t.Fatalf("bare base58 data: %+v %v", a, err)
	}
	b, err := c.GetBlock(ctx, 5, &BlockConfig{TransactionDetails: TransactionDetailsAccounts})
	if err != nil || len(b.Transactions) != 1 || len(b.Transactions[0].Transaction.AccountKeys) != 1 || b.Transactions[0].Transaction.AccountKeys[0].Pubkey != "K" || b.Transactions[0].Transaction.Signatures[0] != "S" {
		t.Fatalf("accounts block shape: %+v %v", b, err)
	}
	pr, err := c.Prices(ctx, PricesParams{})
	if err != nil || pr.Stale != 0 || len(pr.Rows) != 1 || pr.Rows[0].QuoteMint != "Q" || pr.Rows[0].TVLUSD != 1000.5 {
		t.Fatalf("prices with stale 0 and an extra column: %+v %v", pr, err)
	}
	h, err := c.Health(ctx)
	if err != nil || !h.OK || h.Stale != 0 {
		t.Fatalf("health with stale 0: %+v %v", h, err)
	}
	r, err := c.GetInflationReward(ctx, []string{"a", "b"}, nil)
	if err != nil || len(r) != 2 || r[0].Commission != nil || r[1] != nil {
		t.Fatalf("commission null: %+v %v", r, err)
	}
	var d Decimal
	if json.Unmarshal([]byte(`18446744073709551615`), &d) != nil || d != "18446744073709551615" || json.Unmarshal([]byte(`"7"`), &d) != nil || d != "7" {
		t.Fatalf("Decimal reads a number and a string: %q", d)
	}
}
