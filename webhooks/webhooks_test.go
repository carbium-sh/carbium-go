package webhooks

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	carbium "github.com/carbium-sh/carbium-go"
)

// The Standard Webhooks specification's own example: an answer this package did not compute. The secret is joined
// here so no file holds it whole.
var (
	secret    = "whsec_" + "MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	msgID     = "msg_p5jXN8AQM9LWM0D4loKWxJek"
	stamp     = "1614265330"
	body      = []byte(`{"test": 2432232314}`)
	signature = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
)

func at(skew int64) *Options {
	return &Options{Now: func() time.Time { return time.Unix(1614265330+skew, 0) }}
}

func hdr(sig, ts, id string) map[string]string {
	return map[string]string{"webhook-id": id, "webhook-timestamp": ts, "webhook-signature": sig}
}

func reason(err error) string {
	if err == nil {
		return "verified"
	}
	var e *carbium.Error
	if !errors.As(err, &e) || e.Kind != carbium.KindWebhook {
		return "not a webhook error: " + err.Error()
	}
	return e.Reason
}

func check(b []byte, h map[string]string, s string, o *Options) string {
	_, err := VerifySignature(b, h, s, o)
	return reason(err)
}

func TestThePublishedExampleVerifies(t *testing.T) {
	got, err := VerifySignature(body, hdr(signature, stamp, msgID), secret, at(0))
	if err != nil || string(got) != string(body) {
		t.Fatalf("%s %v", got, err)
	}
	if r := reason(func() error { _, err := Verify(body, hdr(signature, stamp, msgID), secret, at(0)); return err }()); r != "bad_json" {
		t.Fatalf("a Carbium body is a list: %s", r)
	}
}

// A real delivery arrived as Webhook-Id, Webhook-Timestamp, Webhook-Signature: names match in any case, from an
// http.Header, a map[string][]string and a map[string]string.
func TestHeaderNamesMatchInAnyCase(t *testing.T) {
	var canonical http.Header = http.Header{}
	canonical.Set("webhook-id", msgID) // stored as Webhook-Id
	canonical.Set("webhook-timestamp", stamp)
	canonical.Set("webhook-signature", signature)
	lists := map[string][]string{"WEBHOOK-ID": {msgID}, "Webhook-Timestamp": {stamp}, "webhook-SIGNATURE": {signature}}
	plain := map[string]string{"Webhook-Id": msgID, "Webhook-Timestamp": stamp, "Webhook-Signature": signature}
	if _, err := VerifySignature(body, canonical, secret, at(0)); err != nil {
		t.Errorf("http.Header: %v", err)
	}
	if _, err := VerifySignature(body, lists, secret, at(0)); err != nil {
		t.Errorf("map[string][]string: %v", err)
	}
	if _, err := VerifySignature(body, plain, secret, at(0)); err != nil {
		t.Errorf("map[string]string: %v", err)
	}
}

func TestOneChangedByteAnywhereFails(t *testing.T) {
	for _, pos := range []int{0, len(body) / 2, len(body) - 1} {
		flipped := append([]byte(nil), body...)
		flipped[pos] ^= 1
		if r := check(flipped, hdr(signature, stamp, msgID), secret, at(0)); r != "no_match" {
			t.Errorf("byte %d changed: %s", pos, r)
		}
	}
	if r := check(append(append([]byte(nil), body...), ' '), hdr(signature, stamp, msgID), secret, at(0)); r != "no_match" {
		t.Errorf("a trailing space: %s", r)
	}
	if r := check([]byte(`{"test":2432232314}`), hdr(signature, stamp, msgID), secret, at(0)); r != "no_match" {
		t.Errorf("the same JSON encoded again: %s", r)
	}
	if r := check(body, hdr(signature, stamp, msgID+"x"), secret, at(0)); r != "no_match" {
		t.Errorf("another id: %s", r)
	}
}

func TestAWrongOrMalformedSecretAndMissingHeadersAreNamed(t *testing.T) {
	for _, tc := range []struct {
		h    map[string]string
		s    string
		want string
	}{
		{hdr(signature, stamp, msgID), "whsec_" + base64.StdEncoding.EncodeToString([]byte("another secret")), "no_match"},
		{hdr(signature, stamp, msgID), secret[len("whsec_"):], "bad_secret"},
		{hdr(signature, stamp, msgID), "whsec_", "bad_secret"},
		{hdr(signature, stamp, msgID), "whsec_" + "not base64!", "bad_secret"},
		{map[string]string{"webhook-id": msgID, "webhook-timestamp": stamp}, secret, "missing_header"},
		{hdr(signature, "soon", msgID), secret, "bad_timestamp"},
		{hdr("v2,"+signature[3:], stamp, msgID), secret, "no_match"},
	} {
		if r := check(body, tc.h, tc.s, at(0)); r != tc.want {
			t.Errorf("%v: %s, want %s", tc.h, r, tc.want)
		}
	}
}

func TestTheClockWindowIs300SecondsEitherWay(t *testing.T) {
	for skew, want := range map[int64]string{300: "verified", -300: "verified", 301: "stale", -301: "stale"} {
		if r := check(body, hdr(signature, stamp, msgID), secret, at(skew)); r != want {
			t.Errorf("skew %d: %s, want %s", skew, r, want)
		}
	}
	fraction := &Options{Now: func() time.Time { return time.Unix(1614265330+300, 1) }}
	if r := check(body, hdr(signature, stamp, msgID), secret, fraction); r != "stale" {
		t.Errorf("300 s and a nanosecond behind: %s", r)
	}
	wide := at(400)
	wide.Tolerance = 600 * time.Second
	if r := check(body, hdr(signature, stamp, msgID), secret, wide); r != "verified" {
		t.Errorf("a 600 s tolerance: %s", r)
	}
}

// The window fails closed: a timestamp or clock that cannot be compared is refused as stale, never accepted.
func TestTheClockWindowFailsClosed(t *testing.T) {
	for _, ts := range []string{"99999999999999999999999", "9223372036854775807", "9223372036854775808", "0"} {
		if r := check(body, hdr(signature, ts, msgID), secret, at(0)); r != "stale" {
			t.Errorf("timestamp %s: %s, want stale", ts, r)
		}
	}
	for name, clock := range map[string]func() time.Time{
		"zero time":  func() time.Time { return time.Time{} },
		"year 9999":  func() time.Time { return time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC) },
		"before 1":   func() time.Time { return time.Date(-292277022399, 1, 1, 0, 0, 0, 0, time.UTC) },
		"max future": func() time.Time { return time.Unix(1<<63-1-62135596800, 0) },
	} {
		if r := check(body, hdr(signature, stamp, msgID), secret, &Options{Now: clock}); r != "stale" {
			t.Errorf("clock %s: %s, want stale", name, r)
		}
	}
	negative := at(0)
	negative.Tolerance = -time.Second
	if r := check(body, hdr(signature, stamp, msgID), secret, negative); r != "stale" {
		t.Errorf("a negative tolerance: %s, want stale", r)
	}
}

func TestEitherEntryMatchesWhileASecretRotatesAndAListBodyReturnsItsEvents(t *testing.T) {
	if r := check(body, hdr("v1,AAAA "+signature, stamp, msgID), secret, at(0)); r != "verified" {
		t.Error(r)
	}
	if r := check(body, hdr(signature+" v1,AAAA", stamp, msgID), secret, at(0)); r != "verified" {
		t.Error(r)
	}
	list := []byte(`[{"slot":1,"blockTime":null,"indexWithinBlock":3,"signature":"abc","version":0,"extra":true}]`)
	key, _ := base64.StdEncoding.DecodeString(secret[len("whsec_"):])
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(msgID + "." + stamp + "."))
	mac.Write(list)
	signed := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
	events, err := Verify(list, hdr(signed, stamp, msgID), secret, at(0))
	if err != nil || len(events) != 1 || events[0].Slot != 1 || events[0].IndexWithinBlock != 3 || events[0].Signature != "abc" || events[0].Version != carbium.TxVersion0 || !strings.Contains(string(events[0].Raw), `"extra":true`) {
		t.Fatalf("%+v %v", events, err)
	}
}

func TestTheSecretNeverShowsInAnError(t *testing.T) {
	_, err := VerifySignature(body, hdr("v1,AAAA", stamp, msgID), secret, at(0))
	for _, shown := range []string{err.Error(), fmt.Sprintf("%+v", err), fmt.Sprintf("%#v", err)} {
		if strings.Contains(shown, secret) || strings.Contains(shown, secret[len("whsec_"):]) {
			t.Fatal("the secret shows in an error")
		}
	}
}
