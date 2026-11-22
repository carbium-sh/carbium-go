// Package httpx is the one JSON HTTP exchange the account, DCA and token clients share. It never retries: those
// APIs mark an error retryable and pass the wait on, and the caller decides.
package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	carbium "github.com/carbium-sh/carbium-go"
)

// Doer sends requests under one base URL.
type Doer struct {
	Base    string
	HTTP    *http.Client
	Timeout time.Duration
	Kind    carbium.ErrorKind
	// Auth sets the credential header on each request; it is never printed.
	Auth func(h http.Header)
}

// New checks the base URL and fills the defaults.
func New(base, dflt string, hc *http.Client, timeout time.Duration, kind carbium.ErrorKind) (*Doer, error) {
	if base == "" {
		base = dflt
	}
	if err := carbium.CheckEndpoint(base, "https", "http"); err != nil {
		return nil, err
	}
	hc = carbium.NoRedirects(hc)
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Doer{Base: strings.TrimRight(base, "/"), HTTP: hc, Timeout: timeout, Kind: kind}, nil
}

// Answer is one HTTP answer.
type Answer struct {
	Status int
	Header http.Header
	Body   []byte
}

// Object is the body as a JSON object, or nil.
func (a *Answer) Object() map[string]json.RawMessage {
	var m map[string]json.RawMessage
	if json.Unmarshal(a.Body, &m) != nil {
		return nil
	}
	return m
}

// Text is the body trimmed and cut to n bytes.
func (a *Answer) Text(n int) string {
	t := strings.TrimSpace(string(bytes.ToValidUTF8(a.Body, []byte("?"))))
	if len(t) > n {
		t = t[:n]
	}
	return t
}

// Do sends one request. A path is joined to the base; query may be nil; body, when not nil, is sent as JSON.
// timeout overrides the default when it is longer.
func (d *Doer) Do(ctx context.Context, verb, path string, query url.Values, body any, timeout time.Duration, method string) (*Answer, error) {
	target := d.Base + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	var rd io.Reader
	if body != nil {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(body); err != nil {
			return nil, carbium.NewError(carbium.ErrorInit{Kind: d.Kind, Method: method, Message: "the request does not encode as JSON: " + err.Error()})
		}
		rd = bytes.NewReader(bytes.TrimRight(buf.Bytes(), "\n"))
	}
	t := d.Timeout
	if timeout > t {
		t = timeout
	}
	actx, cancel := context.WithTimeout(ctx, t)
	defer cancel()
	req, err := http.NewRequestWithContext(actx, verb, target, rd)
	if err != nil {
		return nil, carbium.NewError(carbium.ErrorInit{Kind: d.Kind, Method: method, Message: err.Error()})
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if d.Auth != nil {
		d.Auth(req.Header)
	}
	resp, err := d.HTTP.Do(req)
	var data []byte
	if err == nil {
		data, err = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		return nil, carbium.NewError(carbium.ErrorInit{Kind: d.Kind, Method: method, Message: err.Error(), Retryable: verb == http.MethodGet, Cause: err})
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return nil, carbium.NewError(carbium.ErrorInit{Kind: d.Kind, Method: method, HTTPStatus: resp.StatusCode, Message: fmt.Sprintf("the service answered with a redirect (HTTP %d), which is not followed", resp.StatusCode),
			RequestID: resp.Header.Get("X-Request-Id")})
	}
	return &Answer{Status: resp.StatusCode, Header: resp.Header, Body: data}, nil
}

// Str reads a string field of a JSON object.
func Str(m map[string]json.RawMessage, key string) (string, bool) {
	var s string
	if raw, ok := m[key]; ok && json.Unmarshal(raw, &s) == nil {
		return s, true
	}
	return "", false
}
