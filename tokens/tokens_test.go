package tokens

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	carbium "github.com/carbium-sh/carbium-go"
	"github.com/carbium-sh/carbium-go/internal/planted"
)

func TestEveryServedRouteHasACallAndNothingElseDoes(t *testing.T) {
	served := planted.Served(t, "../spec/served.json", "tokens")
	isServed := map[string]bool{}
	for _, r := range served {
		isServed[r] = true
		if routes[r] == "" {
			t.Errorf("served but has no call: %s", r)
		}
	}
	for r := range routes {
		if !isServed[r] {
			t.Errorf("has a call but is not served: %s", r)
		}
	}
	s := planted.Start(t, planted.Answer{Body: `{}`})
	c, _ := New(&Options{BaseURL: s.URL})
	ctx := context.Background()
	calls := map[string]func() error{
		"Search": func() error { _, err := c.Search(ctx, "sol", nil); return err },
		"Lookup": func() error { _, err := c.Lookup(ctx, "M"); return err },
		"Batch":  func() error { _, err := c.Batch(ctx, []string{"M"}); return err },
	}
	for route, name := range routes {
		want := strings.Replace(route, ":mint", "M", 1)
		if name == "ImageURL" {
			if got := c.ImageURL("M"); got != s.URL+strings.TrimPrefix(want, "GET ") {
				t.Errorf("ImageURL %s, want %s", got, want)
			}
			continue
		}
		call, ok := calls[name]
		if !ok {
			t.Errorf("no test call for %s", name)
			continue
		}
		before := len(s.Seen())
		if err := call(); err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		got := s.Seen()[before:]
		if len(got) != 1 || got[0].Verb+" "+strings.SplitN(got[0].Path, "?", 2)[0] != want {
			t.Errorf("%s sent %+v, want %s", name, got, want)
		}
	}
	if n := len(s.Seen()); n != 3 {
		t.Errorf("%d requests, want 3: ImageURL makes none", n)
	}
}

// Case 22, token part: 404 is nil, 429 carries the wait, batch size is checked locally.
func TestNotFoundWaitsAndLimits(t *testing.T) {
	s := planted.Start(t,
		planted.Answer{Status: 404, Body: `{"error":"not_found","mint":"x"}`},
		planted.Answer{Status: 429, Body: `{"error":"rate_limited","message":"slow down","retryAfterSec":12}`, Header: map[string]string{"Retry-After": "12"}},
		planted.Answer{Status: 503, Body: `{"error":"unavailable","retryAfterSec":4}`},
		planted.Answer{Body: `{"items":[{"mint":"m","symbol":"M","decimals":6}],"unknown":["u"]}`},
	)
	c, _ := New(&Options{BaseURL: s.URL})
	ctx := context.Background()
	if tok, err := c.Lookup(ctx, "x"); tok != nil || err != nil {
		t.Fatal(tok, err)
	}
	var e *carbium.Error
	_, err := c.Search(ctx, "sol", &SearchOptions{Limit: 5})
	if !errors.As(err, &e) || e.Kind != carbium.KindTokens || e.ErrorCode != "rate_limited" || e.RetryAfter != 12*time.Second || !e.Retryable {
		t.Fatalf("%+v", err)
	}
	if got := s.Seen()[1].Path; got != "/tokens?limit=5&q=sol" {
		t.Fatalf("query %s", got)
	}
	_, err = c.Lookup(ctx, "y")
	if !errors.As(err, &e) || e.RetryAfter != 4*time.Second {
		t.Fatalf("the body's retryAfterSec is the hint when no header: %+v", err)
	}
	b, err := c.Batch(ctx, []string{"m", "u"})
	if err != nil || len(b.Unknown) != 1 || b.Items[0].Decimals != 6 {
		t.Fatal(b, err)
	}
	if _, err := c.Batch(ctx, make([]string, 501)); !errors.As(err, &e) || e.Kind != carbium.KindTokens {
		t.Fatal(err)
	}
	if _, err := c.Batch(ctx, nil); err == nil {
		t.Fatal("an empty batch is refused")
	}
	if n := len(s.Seen()); n != 4 {
		t.Fatalf("%d requests", n)
	}
}

func TestARedirectIsNotFollowed(t *testing.T) {
	other := planted.Start(t)
	s := planted.Start(t, planted.Answer{Status: 302, Header: map[string]string{"Location": other.URL + "/tokens"}})
	c, _ := New(&Options{BaseURL: s.URL})
	_, err := c.Search(context.Background(), "sol", nil)
	var e *carbium.Error
	if !errors.As(err, &e) || e.HTTPStatus != 302 || len(other.Seen()) != 0 {
		t.Fatalf("%+v; the other host saw %d requests", err, len(other.Seen()))
	}
}

// A token row in the live shape: symbol and name are strings, times are numbers, token_standard is a number or
// null, tags hold any JSON, and a logo status the client does not list is kept.
func TestATokenRowInTheLiveShape(t *testing.T) {
	row := `{"mint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","symbol":"USDC","name":"USD Coin","decimals":6,"logo_url":null,"logo_status":"broken_perm","is_token22":false,"daily_volume_usd":null,"first_seen_at":1700000000000,"last_seen_at":1763800000000,"supply":"9000000000000000","mint_authority":null,"freeze_authority":null,"metaplex_uri":null,"update_authority":null,"seller_fee_basis_points":null,"token_standard":2,"description":null,"external_url":null,"extensions":null,"tags":["stablecoin",{"kind":"verified"}]}`
	fresh := `{"mint":"M","symbol":"","name":"","decimals":0,"logo_url":null,"logo_status":"unchecked","is_token22":true,"daily_volume_usd":null,"first_seen_at":1763800000000,"last_seen_at":1763800000000,"supply":null,"mint_authority":null,"freeze_authority":null,"metaplex_uri":null,"update_authority":null,"seller_fee_basis_points":null,"token_standard":null,"description":null,"external_url":null,"extensions":null,"tags":null}`
	s := planted.Start(t, planted.Answer{Body: row}, planted.Answer{Body: `{"items":[` + row + `,` + fresh + `],"unknown":["U"]}`})
	c, _ := New(&Options{BaseURL: s.URL})
	tok, err := c.Lookup(context.Background(), "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
	if err != nil || tok.Symbol != "USDC" || tok.LogoStatus != "broken_perm" || *tok.TokenStandard != 2 || tok.FirstSeenAt != 1700000000000 || len(tok.Tags) != 2 {
		t.Fatalf("%+v %v", tok, err)
	}
	b, err := c.Batch(context.Background(), []string{"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "M", "U"})
	if err != nil || len(b.Items) != 2 || b.Items[1].Symbol != "" || b.Items[1].TokenStandard != nil || b.Items[1].Tags != nil || b.Unknown[0] != "U" {
		t.Fatalf("%+v %v", b, err)
	}
}
