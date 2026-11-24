# carbium-go

Typed Go client for Carbium's Solana services on [services.carbium.sh](https://carbium.sh): JSON-RPC (every served
method, DAS included), the quote plane, WebSocket subscriptions, the account API, webhook signature checks, the DCA API
on the dex, the token API, and Carbon Stream in its own module. Standard library only, plus one WebSocket package. Go
1.26 or later.

The module never holds a private key, signs or sends on its own: swap and DCA calls return one unsigned transaction
for your own wallet to sign, and `SendTransaction` takes bytes you signed. Every method the module offers is served;
what the services do not serve is not here.

```sh
go get github.com/carbium-sh/carbium-go
```

To update: `go get github.com/carbium-sh/carbium-go@latest`; each version's notes are at
https://github.com/carbium-sh/carbium-go/releases.

## Quick start

A service key (`c_serv_…`) comes from the dashboard at carbium.sh. Keep it in `CARBIUM_KEY`. Without one the call
reaches the service and is refused with 401, which the quick start prints.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	carbium "github.com/carbium-sh/carbium-go"
)

func main() {
	c, err := carbium.NewClient(os.Getenv("CARBIUM_KEY"), nil)
	if err != nil {
		panic(err)
	}
	bh, err := c.GetLatestBlockhash(context.Background(), nil)
	var e *carbium.Error
	if errors.As(err, &e) {
		fmt.Println(e.Kind, e.HTTPStatus, e.Code, e.Reason, e.Message)
		return
	}
	fmt.Println("blockhash", bh.Value.Blockhash, "valid until block height", bh.Value.LastValidBlockHeight, "at slot", bh.Context.Slot)
}
```

## RPC, DAS and the quote plane

Every served method is a method on `*carbium.Client` that takes a `context.Context` first. Addresses and signatures
are plain strings. Optional configs are pointers; pass nil to leave them out.

```go
ctx := context.Background()

// Standard Solana methods take positional params, as on the wire.
bal, err := c.GetBalance(ctx, "So11111111111111111111111111111111111111112", nil)
accts, err := c.GetMultipleAccounts(ctx, []string{a, b}, &carbium.AccountInfoConfig{Encoding: carbium.EncodingBase64})
fees, err := c.GetPriorityFeeEstimate(ctx, carbium.PriorityFeeEstimateParams{AccountKeys: []string{a}})

// DAS methods take one params struct in Helius field names.
asset, err := c.GetAsset(ctx, carbium.GetAssetParams{ID: mint})
page, err := c.GetAssetsByOwner(ctx, carbium.GetAssetsByOwnerParams{OwnerAddress: owner, Limit: carbium.Ptr[uint64](1000)})

// Any served method by name, decoded into your own value.
var slot uint64
err = c.Call(ctx, "getSlot", []any{map[string]string{"commitment": "confirmed"}}, &slot)

// A JSON-RPC batch: 1 to 100 calls, at most two permit methods, DAS only with DAS. Scans, getSupply,
// getLargestAccounts, the fee extras, getTransactionsForAddress and the quote plane never batch.
res, err := c.Batch(ctx, []carbium.BatchCall{{Method: "getSlot"}, {Method: "getHealth"}})
err = res[0].Decode(&slot)
```

`getTransaction` and `getBlock` send `maxSupportedTransactionVersion: 1` unless your config sets it.

Token scans (`getTokenAccountsByOwner`, `getTokenAccountsByDelegate`) are billed by rows, up to 100,005 credits;
`getProgramAccounts` costs 505; a `getTransactionsForAddress` walk costs 55 plus 50 per upstream call, up to 51,255 at
`limit` 1000. Name a mint, keep `limit` small, and read the method notes at https://carbium.sh/skill.md before running
one in a loop.

### Swap: quote, build, sign with your own wallet, send

The quote plane needs the account's quote module (free plan) and a key with the quote scope.

```go
q, err := c.Quote(ctx, carbium.QuoteParams{SrcMint: sol, DstMint: usdc, AmountIn: "100000000", SlippageBps: 50})
swap, err := c.Swap(ctx, carbium.SwapParams{QuoteID: q.QuoteID, UserPubkey: wallet})
// swap.Transaction is one unsigned v0 transaction, base64; your wallet is its only signer.
// Sign it with your own wallet library, then:
sig, err := c.SendTransaction(ctx, signedBase64, &carbium.SendTransactionConfig{Encoding: carbium.BinaryEncodingBase64, SkipPreflight: carbium.Ptr(true)})
```

Then poll `GetSignatureStatuses` and resend the same bytes until the block height passes
`swap.LastValidBlockHeight`. The module never retries `SendTransaction`. Quote again (never re-sign the old message)
when the blockhash expires or when `Swap` answers a route error that asks for a new quote.

A quote also takes a cap on the accounts the swap instruction may carry and your own integrator fee:

```go
capped, err := c.Quote(ctx, carbium.QuoteParams{SrcMint: sol, DstMint: usdc, AmountIn: "100000000", SlippageBps: 50,
	MaxAccounts: carbium.Ptr[uint64](32), IntegratorFeeBps: "25", IntegratorFeeAccount: treasury})
// capped.IntegratorFee is {Amount, FeeBps, Mint, Account}; DestAmountOut and MinAmountOut are what the trader
// receives after it.
```

The fee is taken in the token `IntegratorFee.Mint` names. A token fee goes to that wallet's associated token account,
which must already exist; a fee that cannot be paid is skipped and the swap still lands. A quote with no route under
`MaxAccounts` answers `-32055` (kind `route`): change the ask, a retry gives the same answer.

`GET /cq1/quote` is the old quote path, kept for callers already on it (`Cq1Quote`, deprecated); new code uses
`Quote`.

## Errors and retries

Every failure is a `*carbium.Error`; check it with `errors.As`.

| Field | Meaning |
|---|---|
| `Kind` | `auth`, `credits`, `rate_limit`, `request`, `upstream`, `node`, `route`, `transport`, `socket`, `stream`, `account`, `dca`, `tokens`, `webhook` |
| `HTTPStatus` | the HTTP status, 0 when nothing answered |
| `Code` | the JSON-RPC code or a close code |
| `ErrorCode` | the account, DCA or token API's error string |
| `Reason` | the service's `error.data.reason`, a close reason or a webhook check's reason |
| `Retryable` | whether waiting and sending the same request can change the answer |
| `RetryAfter` | the server's wait hint |
| `RequestID` | `x-request-id`, to quote to support |
| `Data` | `error.data` as served |

The retry policy is the one published in skill.md and applies to every HTTP call on services.carbium.sh: wait the
larger of the server's hint (`retry-after-ms`, else `retryAfterMs` in the error's data, else `Retry-After`) and 250 ms
doubling to 4 s, plus up to 250 ms of jitter; a hint is never cut, and a hint over `MaxHint` (30 s by default)
is not waited for: the error comes back at once with it in `RetryAfter`; at most 4 attempts unless the published
row says otherwise (a scan index behind: about 6 from 100 ms). `SendTransaction`, 401, 402 (credits exhausted),
403, bad params (apart from `GetTransactionsForAddress`'s `-32602` "the walk read no history", which is retried),
unknown methods and every answer marked `retryable: false` are never retried. `Options{MaxAttempts: 1}` turns
retries off; `MaxAttempts` lowers the attempt count. Each attempt times out after 100 s by default.

Keys never appear in error messages, data, URLs or formatted output: `%v`, `%+v` and `%#v` of a client or an error
show a placeholder. A key in an endpoint URL is refused.

## WebSocket subscriptions

```go
import "github.com/carbium-sh/carbium-go/ws"

s, err := ws.New(os.Getenv("CARBIUM_KEY"), &ws.Options{OnReseed: func(last ws.CloseInfo, gap bool) { reloadStateOverRPC() }})
sub, err := s.AccountSubscribe(pubkey, &ws.AccountConfig{Commitment: carbium.CommitmentConfirmed},
	func(n carbium.WithContext[carbium.AccountInfo]) { fmt.Println(n.Context.Slot, n.Value.Lamports) })
id, err := sub.Ready(ctx)
err = sub.Unsubscribe(ctx)
```

All eight served subscriptions have a method (`AccountSubscribe`, `ProgramSubscribe`, `LogsSubscribe`,
`SignatureSubscribe`, `SlotSubscribe`, `RootSubscribe`, `SlotsUpdatesSubscribe`, `BlockSubscribe`). The socket
follows the server's close codes: it reconnects and binds every live subscription again after 1013, 1012, 1001 and
network drops, reconnects once after 1008 `credits_exhausted`, and stops with `OnFatal` on a key refusal. Nothing is
replayed by the server after a disconnect: `OnReseed` runs after every reconnect so you read again what you may have
missed (`gap` is true after `stream_gap_reseed_required`). `SignatureSubscribe` fires once; after a reconnect, check
`GetSignatureStatuses`. Callbacks run on the socket's reader and must not block.

The key travels in the `x-api-key` handshake header by default; `Carrier: "subprotocol"` sends it in
`Sec-WebSocket-Protocol` instead, the form browsers need. `Unsubscribe` returns before its frame is necessarily
written, so a program that exits at once may not have told the server (closing the socket ends its subscriptions
either way). A refused handshake comes to both `OnFatal` and `Ready`, so a caller that prints in both prints twice.

## Carbon Stream (Yellowstone gRPC)

Carbon Stream is its own module, so gRPC never enters this one: `go get github.com/carbium-sh/carbium-go/stream`. The
key needs the grpc scope and a plan with streams; it travels as `x-token` metadata.

```go
import (
	"github.com/carbium-sh/carbium-go/stream"
	"github.com/carbium-sh/carbium-go/stream/geyserpb"
	"google.golang.org/protobuf/proto"
)

sc, err := stream.Connect(os.Getenv("CARBIUM_KEY"), nil)
live, err := sc.SubscribeWithReconnect(ctx, stream.Filters{
	Transactions: map[string]*geyserpb.SubscribeRequestFilterTransactions{"pool": {AccountInclude: []string{programID}, Vote: proto.Bool(false)}},
}, stream.Handlers{
	OnUpdate: func(u *geyserpb.SubscribeUpdate) { /* must not block */ },
	OnReseed: func(ctx context.Context, lastSlot *uint64) { reloadOverRPC() },
})
// later
live.Close()
```

`SubscribeWithReconnect` puts the whole filter set in the first message (a filter change needs a new stream), pings
every 10 s, retries with the published waits, re-seeds through `OnReseed` on `OUT_OF_RANGE`, and resumes from the last
slot when the retained floor allows. A replay can start one slot before `from_slot` and overlap what you already have:
dedupe transactions on `(slot, signature)` and accounts on `(pubkey, slot, writeVersion)`.
Fields the service refuses (`token_accounts`, cuckoo filters, `include_update_parent`) are caught before any call.
The seven unary calls (`GetSlot`, `GetBlockHeight`, `GetLatestBlockhash`, `IsBlockhashValid`, `GetVersion`, `Ping`,
`SubscribeReplayInfo`) are on the same client. The `geyserpb` types are generated from the upstream Yellowstone protos
(Apache-2.0, see `stream/NOTICE`).

## Webhooks

```go
import "github.com/carbium-sh/carbium-go/webhooks"

// in your HTTPS handler, with the RAW body bytes (never a re-encoded JSON string)
events, err := webhooks.Verify(rawBody, r.Header, os.Getenv("CARBIUM_WEBHOOK_SECRET"), nil)
```

Standard Webhooks: HMAC-SHA256 over `webhook-id.webhook-timestamp.body` with the secret's base64 after `whsec_`,
`v1,` entries in `webhook-signature` (two during a rotation), compared in constant time, 300 s of clock tolerance.
Header names match in any case, from an `http.Header` or a plain map. A timestamp or clock that cannot be compared is
refused as `stale`. Errors are kind `webhook` with reason `missing_header`, `bad_timestamp`, `stale`, `bad_secret`,
`no_match` or `bad_json`. Dedupe on `webhook-id`: it stays the same across retries.

## Account API

An account API key (`c_acct_…`, made on the dashboard; keep it in `CARBIUM_ACCOUNT_KEY`) runs the account by API:
service keys, webhooks, usage and records. Calls cost no credits; 600 per 10 minutes per key. Nothing here is
retried, because creates are not safe to repeat.

```go
import "github.com/carbium-sh/carbium-go/account"

a, err := account.New(os.Getenv("CARBIUM_ACCOUNT_KEY"), nil)
keys, err := a.Keys.List(ctx)
created, err := a.Webhooks.Create(ctx, account.WebhooksCreateReq{KeyID: keys[0].KeyID, URL: "https://hooks.example.com/carbium", Addresses: []string{wallet}})
result, err := a.Webhooks.Test(ctx, created.Webhook.WebhookID) // read result.Outcome
secret := created.Secret.Reveal() // shown once: store it
```

Groups: `Keys` (List, Create, Rotate, Revoke, Delete, Rename, SetScope, SetCaps, SetAllowlist, SetLimits),
`Webhooks` (List, Get, Deliveries, Create, Update, Delete, RotateSecret, Test), `Usage` (Daily, PerKey, Load, Hourly,
Latency), `Account` (Get, Balance, Invoices, Payments, Modules, CryptoIntent), `Modules.EnableFree`, `Self`,
`Activity`. A key or secret the service shows once comes back as `carbium.Secret`, which prints as a placeholder;
`Reveal` returns it. `account.Session` and the sign-in functions cover the dashboard session calls. Payments, plan
changes and closing the account stay on the dashboard.

## DCA API

```go
import "github.com/carbium-sh/carbium-go/dca"

d, err := dca.New(nil)
q, err := d.Quote(ctx, dca.QuoteRequest{InputMint: usdc, OutputMint: sol, Amount: 1000000})
per := dca.U64(1000000)
build, err := d.Open(ctx, dca.OpenRequest{Owner: owner, InputMint: usdc, OutputMint: sol, Cycles: 10, AmountPerCycle: &per, Frequency: "daily"})
// build.TxnBase64 is unsigned: sign it with the owner's wallet, then send it yourself or relay it:
sent, err := d.Send(ctx, dca.SendRequest{TxnBase64: signedBase64})
```

Reads: `Quote`, `Positions`, `History`, `Position`, `Health`. Builders: `Open`, `Update`, `Withdraw`, `Close`. `Send`
relays bytes the owner signed, once. Server side only: a browser page is refused with `forbidden_origin`. Errors are
kind `dca` with the API's `err` string as `ErrorCode`.

## Token API

```go
import "github.com/carbium-sh/carbium-go/tokens"

t, err := tokens.New(nil)
sol, err := t.Lookup(ctx, "So11111111111111111111111111111111111111112") // nil when unknown
top, err := t.Search(ctx, "", &tokens.SearchOptions{Limit: 20})
b, err := t.Batch(ctx, mints) // up to 500
src := t.ImageURL(mint)
```

Keyless, 300 requests a minute per client. These four calls are the token API's public routes.

## Limits checked before sending

Body 51,200 bytes; batch 1 to 100 calls with at most two permit methods; DAS only with DAS; methods that never batch
are refused in a batch; a method the services do not serve is refused before it is sent; token `Batch` up to 500
mints.

## Working on this module

`go generate ./...` rewrites `methods_gen.go` from `spec/openrpc.json` and `account/account_gen.go` from
`spec/account-openapi.json`, and fails on anything it cannot type; CI checks both are current. Run
`git config core.hooksPath hooks` once per clone: the pre-push hook runs `scripts/leak-check.sh`.

MIT. Issues: https://github.com/carbium-sh/carbium-go/issues. Docs: https://carbium.sh/docs and https://carbium.sh/skill.md.
