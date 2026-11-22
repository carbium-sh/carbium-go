package carbium

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Secret is a key, secret or token the service shows once, such as a new key's raw_key or a webhook's whsec_
// secret. It prints, formats and encodes as a placeholder that keeps only its label; Reveal returns the value, to
// store it.
type Secret string

// Reveal returns the secret itself.
func (s Secret) Reveal() string { return string(s) }

// String is the placeholder: the label before the value (c_serv_, c_acct_, whsec_) and an ellipsis.
func (s Secret) String() string {
	if s == "" {
		return ""
	}
	for _, label := range []string{"c_serv_", "c_acct_", "whsec_"} {
		if strings.HasPrefix(string(s), label) {
			return label + "…"
		}
	}
	return "…"
}

func (s Secret) GoString() string { return fmt.Sprintf("%q", s.String()) }

func (s Secret) Format(f fmt.State, verb rune) {
	if verb == 'q' || (verb == 'v' && f.Flag('#')) {
		fmt.Fprintf(f, "%q", s.String())
		return
	}
	fmt.Fprint(f, s.String())
}

// MarshalJSON writes the placeholder, so logging an answer as JSON never writes the secret.
func (s Secret) MarshalJSON() ([]byte, error) { return json.Marshal(s.String()) }

// UnmarshalJSON reads the secret as the service sends it.
func (s *Secret) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*s = Secret(v)
	return nil
}
