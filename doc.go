// Package carbium is a typed Go client for Carbium's Solana services on services.carbium.sh: every served JSON-RPC
// method (Solana RPC, DAS and the quote plane) as a method on Client, with the published retry rules and the
// service's limits checked before a request is sent.
//
// The module never holds a private key, signs or sends on its own. Swap and the DCA builders return one unsigned
// transaction for the caller's own wallet; SendTransaction relays bytes the caller already signed, once.
//
// Sub-packages: account (the account API on carbium.sh), dca (the DCA API), tokens (the token API), webhooks (the
// delivery signature check) and ws (WebSocket subscriptions). Carbon Stream is the separate module
// github.com/carbium-sh/carbium-go/stream, so gRPC stays out of this module's dependencies.
//
// Every failure is an *Error; check it with errors.As. A key never appears in an error, a URL or formatted output.
package carbium
