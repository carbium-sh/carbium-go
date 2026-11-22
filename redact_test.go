package carbium

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// Case 20: no key or secret survives in anything an error or the client shows.
func TestNoKeyOrSecretSurvivesInAnErrorOrTheClient(t *testing.T) {
	leaked := testKey + "Leaked"
	account := "c_acct_" + "Abc123Def456"
	webhook := "whsec_" + "MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	text := fmt.Sprintf("bad key %s and %s and %s at https://x.example/?api-key=%s via api-key.%s", leaked, account, webhook, leaked, leaked)
	p := plant(t, reply{status: 401, body: rpcErr(-32001, text, "UNKNOWN_KEY", map[string]any{"echo": []any{text, map[string]any{text: text}}})})
	c, _ := testClient(t, p, nil)
	_, err := c.GetSlot(context.Background(), nil)
	e := asError(t, err)

	cause := NewError(ErrorInit{Kind: KindTransport, Message: "dial failed", Cause: fmt.Errorf("proxy said %s: %w", text, io.ErrUnexpectedEOF)})
	shown := []string{e.Error(), e.Message, string(e.Data), cause.Error()}
	for _, x := range []error{err, cause} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
			shown = append(shown, fmt.Sprintf(verb, x))
		}
		deref := *x.(*Error)
		shown = append(shown, fmt.Sprintf("%v", deref), fmt.Sprintf("%+v", deref), fmt.Sprintf("%#v", deref))
		j, jerr := json.Marshal(x)
		if jerr != nil {
			t.Fatal(jerr)
		}
		shown = append(shown, string(j))
		for layer := errors.Unwrap(x); layer != nil; layer = errors.Unwrap(layer) {
			shown = append(shown, layer.Error(), fmt.Sprintf("%+v", layer), fmt.Sprintf("%#v", layer))
		}
	}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		shown = append(shown, fmt.Sprintf(verb, c), fmt.Sprintf(verb, *c))
	}
	all := strings.Join(shown, "\n")
	for _, secret := range []string{testKey, leaked, account, webhook, webhook[6:]} {
		if strings.Contains(all, secret) {
			t.Errorf("a secret survives: %.12s…", secret)
		}
	}
	for _, mark := range []string{"c_serv_…", "c_acct_…", "whsec_…", "api-key=…", "api-key.…"} {
		if !strings.Contains(e.Message, mark) {
			t.Errorf("message %q lacks the placeholder %q", e.Message, mark)
		}
	}
	if !strings.Contains(fmt.Sprintf("%+v", c), "c_serv_…") {
		t.Errorf("the client should show the key's label: %+v", c)
	}
	if !errors.Is(cause, io.ErrUnexpectedEOF) {
		t.Error("errors.Is must still reach a redacted cause")
	}
}

func TestTheDashboardPrefixStaysReadable(t *testing.T) {
	if got := RedactSecrets("keys c_serv_AbCd and c_acct_Zz99"); got != "keys c_serv_AbCd and c_acct_Zz99" {
		t.Fatal(got)
	}
	if got := RedactSecrets("key c_serv_" + "AbCde"); got != "key c_serv_…" {
		t.Fatal(got)
	}
	if got := RedactSecrets("Bearer c_acct_" + "Abcdef12"); got != "Bearer c_acct_…" {
		t.Fatal(got)
	}
}
