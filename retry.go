package carbium

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// retryPlan is how a failed call is tried again: the wait before retry n is
// max(server hint, min(cap, base × 2^(n−1))) plus 0 to 250 ms of jitter, for at most maxAttempts calls in all.
type retryPlan struct {
	base, cap   time.Duration
	maxAttempts int
}

// retryContext is what decideRetry reads about one failed attempt.
type retryContext struct {
	method         string
	httpStatus     int // 0 when nothing answered
	code           int // the JSON-RPC error code; 0 when the body had none
	hasCode        bool
	message        string
	data           map[string]json.RawMessage // error.data as served
	minContextSlot *uint64                    // the caller's minContextSlot, for -32016
}

var standardPlan = &retryPlan{base: 250 * time.Millisecond, cap: 4 * time.Second, maxAttempts: 4}

func withAttempts(p *retryPlan, n int) *retryPlan {
	return &retryPlan{base: p.base, cap: p.cap, maxAttempts: n}
}

var internalError = regexp.MustCompile(`^(Internal error|das upstream refused the request|upstream internal error)$`)

// decideRetry is the published retry table as one pure function: the plan for a failed attempt, or nil when it
// must not be sent again. Rows are tried in order.
func decideRetry(rc retryContext) *retryPlan {
	if rc.method == "sendTransaction" {
		return nil // a send may have landed: check the signature, never resend blindly
	}
	if rc.httpStatus == 0 {
		return standardPlan // no answer: a network error or a timeout
	}
	if b, ok := dataBool(rc.data, "retryable"); ok && !b {
		return nil // the service says waiting cannot help
	}
	msg := rc.message
	switch rc.httpStatus {
	case 429:
		if rc.hasCode && rc.code == -32010 {
			return &retryPlan{base: 100 * time.Millisecond, cap: 5 * time.Second, maxAttempts: 6} // a scan index behind
		}
		return standardPlan
	case 425:
		return &retryPlan{maxAttempts: 2} // early data refused: nothing was forwarded
	case 502:
		if strings.Contains(msg, "response too large") {
			return nil
		}
		return standardPlan
	case 503:
		if strings.HasPrefix(msg, "upstream RPC unavailable") {
			return &retryPlan{base: 2 * time.Second, cap: 8 * time.Second, maxAttempts: 4}
		}
		if msg == "compat verbs not initialised" {
			return withAttempts(standardPlan, 3)
		}
		return standardPlan
	case 504:
		if strings.HasPrefix(msg, "upstream_slow") {
			return nil // a scan over its deadline: once
		}
		return withAttempts(standardPlan, 2)
	case 200:
		if _, ok := rc.data["reason"]; ok {
			return nil // a policy answer at 200 is final
		}
		if !rc.hasCode {
			return nil
		}
		switch rc.code {
		case -32602:
			if strings.HasPrefix(msg, "the walk read no history") {
				return standardPlan
			}
			return nil
		case -32016:
			missing := int64(1)
			if slot, ok := dataNumber(rc.data, "contextSlot"); ok && rc.minContextSlot != nil && *rc.minContextSlot > 0 {
				missing = int64(*rc.minContextSlot) - int64(slot)
			}
			wait := time.Duration(min(30000, max(250, 220*max(missing, 1)))) * time.Millisecond // about 220 ms per missing slot
			return &retryPlan{base: wait, cap: wait, maxAttempts: 4}
		case -32004:
			return &retryPlan{base: time.Second, cap: 2 * time.Second, maxAttempts: 4} // block not available yet
		case -32014:
			return &retryPlan{base: time.Second, cap: time.Second, maxAttempts: 4} // block status not yet available
		case -32005, -32021, -32024, -32026:
			return standardPlan
		case -32011:
			if msg == "das upstream timed out" {
				return standardPlan
			}
			return nil
		case -32603:
			if internalError.MatchString(msg) {
				return withAttempts(standardPlan, 2)
			}
			return nil
		case -32010:
			if strings.Contains(msg, "peer refused the copy") {
				return &retryPlan{base: 20 * time.Second, cap: 20 * time.Second, maxAttempts: 2}
			}
			return nil
		}
		return nil
	}
	return nil // 400, 401, 402, 403, 413, 501 and anything unknown
}

// serverHint is the wait the service asked for: retry-after-ms, else data.retryAfterMs, else Retry-After in seconds
// or as an HTTP date. ok is false when there is none.
// A hint too long to wait for, or too large for a Duration, is clamped to maxServerHint: above the cap, it goes
// back to the caller.
func serverHint(h http.Header, data map[string]json.RawMessage, now time.Time) (time.Duration, bool) {
	if ms := h.Get("Retry-After-Ms"); ms != "" && allDigits(ms) {
		return clampHint(ms, time.Millisecond), true
	}
	if n, ok := dataNumber(data, "retryAfterMs"); ok && n >= 0 {
		if n >= float64(maxServerHint/time.Millisecond) {
			return maxServerHint, true
		}
		return time.Duration(n * float64(time.Millisecond)), true
	}
	return retryAfterHeader(h, now)
}

// maxServerHint is the longest hint the client records: a year.
const maxServerHint = 365 * 24 * time.Hour

// clampHint reads decimal digits in a unit, clamped to maxServerHint.
func clampHint(digits string, unit time.Duration) time.Duration {
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || n >= int64(maxServerHint/unit) {
		return maxServerHint // all digits, so err is only an overflow
	}
	return time.Duration(n) * unit
}

// retryAfterHeader reads Retry-After: whole seconds or an HTTP date.
func retryAfterHeader(h http.Header, now time.Time) (time.Duration, bool) {
	ra := strings.TrimSpace(h.Get("Retry-After"))
	if ra == "" {
		return 0, false
	}
	if allDigits(ra) {
		return clampHint(ra, time.Second), true
	}
	at, err := http.ParseTime(ra)
	if err != nil {
		return 0, false
	}
	return min(maxServerHint, max(0, at.Sub(now))), true
}

// RetryAfterHeader reads a Retry-After header (whole seconds or an HTTP date) as a wait. The sub-packages use it.
func RetryAfterHeader(h http.Header) (time.Duration, bool) { return retryAfterHeader(h, time.Now()) }

// backoff is the wait before retry number attempt (1 for the first retry): max(hint, the plan's backoff) plus jitter.
func backoff(p *retryPlan, attempt int, hint time.Duration, jitter time.Duration) time.Duration {
	computed := p.cap
	if attempt-1 < 62 {
		if d := p.base * time.Duration(int64(1)<<(attempt-1)); d >= 0 && d < p.cap {
			computed = d
		}
	}
	return max(hint, computed) + jitter
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func dataBool(data map[string]json.RawMessage, key string) (bool, bool) {
	raw, ok := data[key]
	if !ok {
		return false, false
	}
	var b bool
	if json.Unmarshal(raw, &b) != nil {
		return false, false
	}
	return b, true
}

func dataNumber(data map[string]json.RawMessage, key string) (float64, bool) {
	raw, ok := data[key]
	if !ok {
		return 0, false
	}
	var n float64
	if json.Unmarshal(raw, &n) != nil {
		return 0, false
	}
	return n, true
}

func dataString(data map[string]json.RawMessage, key string) (string, bool) {
	raw, ok := data[key]
	if !ok {
		return "", false
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return "", false
	}
	return s, true
}
