// Package webhooks checks that a webhook delivery came from Carbium. Deliveries are signed the Standard Webhooks
// way: HMAC-SHA256 with the secret's base64 after whsec_, over "webhook-id.webhook-timestamp.body", sent as one or
// more "v1,<base64>" entries in webhook-signature (two while a secret rotates).
//
// Keep the secret in CARBIUM_WEBHOOK_SECRET and pass the raw body bytes exactly as received: a body that was
// parsed and encoded again does not verify.
package webhooks

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	carbium "github.com/carbium-sh/carbium-go"
)

// Header is how a delivery's headers are passed: an http.Header, a map[string][]string or a map[string]string.
// Names are matched without regard to case.
type Header interface {
	~map[string][]string | ~map[string]string
}

// Options tune the check. The zero value is the default.
type Options struct {
	// Tolerance is how far webhook-timestamp may sit from the clock, either way. Default 300 s. A negative value
	// refuses every delivery.
	Tolerance time.Duration
	// Now is the clock. Default time.Now.
	Now func() time.Time
}

// Event is one element of a Carbium webhook body: a finalized transaction in the getTransaction shape, its short
// form when the full one is over 64 KiB, or the test event.
type Event struct {
	Slot             uint64                   `json:"slot"`
	BlockTime        *int64                   `json:"blockTime"`
	IndexWithinBlock uint64                   `json:"indexWithinBlock"`
	Transaction      json.RawMessage          `json:"transaction,omitempty"`
	Meta             json.RawMessage          `json:"meta,omitempty"`
	Version          carbium.TxVersion        `json:"version,omitempty"`
	Signature        string                   `json:"signature,omitempty"`
	Err              carbium.TransactionError `json:"err,omitempty"`
	Status           json.RawMessage          `json:"status,omitempty"`
	Truncated        bool                     `json:"truncated,omitempty"`
	Test             bool                     `json:"test,omitempty"`
	// Raw is the whole element as it was sent.
	Raw json.RawMessage `json:"-"`
}

func (e *Event) UnmarshalJSON(b []byte) error {
	type plain Event
	var p plain
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}
	*e = Event(p)
	e.Raw = append(json.RawMessage(nil), b...)
	return nil
}

func fail(reason, message string) error {
	return carbium.NewError(carbium.ErrorInit{Kind: carbium.KindWebhook, Reason: reason, Message: message})
}

// header returns the first value of a header, its name matched without regard to case. Names are tried in sorted
// order, so the answer does not depend on map order.
func header[H Header](h H, name string) string {
	v := reflect.ValueOf(h)
	keys := v.MapKeys()
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	for _, k := range keys {
		if !strings.EqualFold(k.String(), name) {
			continue
		}
		val := v.MapIndex(k)
		if val.Kind() == reflect.Slice {
			if val.Len() == 0 {
				continue
			}
			return val.Index(0).String()
		}
		return val.String()
	}
	return ""
}

// within reports whether ts is at most tol from now, either way. Anything it cannot compare is outside.
func within(now time.Time, ts int64, tol time.Duration) bool {
	if tol < 0 {
		return false
	}
	n := now.Unix()
	var gap uint64 // |n - ts|, exact for any two int64 values
	if n >= ts {
		gap = uint64(n) - uint64(ts)
	} else {
		gap = uint64(ts) - uint64(n)
	}
	limit := uint64(tol / time.Second)
	if gap > limit {
		return false
	}
	// The clock's fraction of a second counts when the timestamp is behind it.
	return !(gap == limit && n >= ts && now.Nanosecond() > 0)
}

// VerifySignature returns the body when one webhook-signature entry matches it, and an *carbium.Error of kind
// webhook otherwise, with Reason missing_header, bad_timestamp, stale, bad_secret or no_match. Signatures are
// compared in constant time.
func VerifySignature[H Header](body []byte, headers H, secret string, opts *Options) ([]byte, error) {
	if opts == nil {
		opts = &Options{}
	}
	id, stamp, signature := header(headers, "webhook-id"), header(headers, "webhook-timestamp"), header(headers, "webhook-signature")
	if id == "" || stamp == "" || signature == "" {
		return nil, fail("missing_header", "webhook-id, webhook-timestamp and webhook-signature are all required")
	}
	for _, r := range stamp {
		if r < '0' || r > '9' {
			return nil, fail("bad_timestamp", "webhook-timestamp must be Unix seconds")
		}
	}
	tolerance := opts.Tolerance
	if tolerance == 0 {
		tolerance = 300 * time.Second
	}
	now := time.Now
	if opts.Now != nil {
		now = opts.Now
	}
	ts, err := strconv.ParseInt(stamp, 10, 64)
	if err != nil || !within(now(), ts, tolerance) {
		return nil, fail("stale", fmt.Sprintf("webhook-timestamp is more than %s from the clock", tolerance))
	}
	if !strings.HasPrefix(secret, "whsec_") || len(secret) <= len("whsec_") {
		return nil, fail("bad_secret", "the secret must be the whsec_ value shown once when the webhook was created or rotated")
	}
	key, err := base64.StdEncoding.DecodeString(secret[len("whsec_"):])
	if err != nil {
		return nil, fail("bad_secret", "the secret after whsec_ is not base64")
	}
	if len(key) == 0 {
		return nil, fail("bad_secret", "the secret is empty")
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id + "." + stamp + "."))
	mac.Write(body)
	expected := mac.Sum(nil)
	matched := false
	for _, entry := range strings.Fields(signature) {
		sig, isV1 := strings.CutPrefix(entry, "v1,")
		if !isV1 {
			continue
		}
		candidate, err := base64.StdEncoding.DecodeString(sig)
		if err != nil {
			continue
		}
		if hmac.Equal(candidate, expected) {
			matched = true
		}
	}
	if !matched {
		return nil, fail("no_match", "no webhook-signature entry matches this body and secret")
	}
	return body, nil
}

// Verify checks a delivery as VerifySignature does and returns its events: a Carbium body is always a JSON array.
// It adds Reason bad_json.
func Verify[H Header](body []byte, headers H, secret string, opts *Options) ([]Event, error) {
	b, err := VerifySignature(body, headers, secret, opts)
	if err != nil {
		return nil, err
	}
	var events []Event
	if json.Unmarshal(b, &events) != nil || events == nil {
		return nil, fail("bad_json", "the body verified but is not a JSON array")
	}
	return events, nil
}
