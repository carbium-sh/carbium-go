// Package planted is a scripted local HTTP server for the sub-packages' tests: it answers from a script and
// records what it was sent.
package planted

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
)

// Answer is one scripted reply.
type Answer struct {
	Status int
	Body   string
	Header map[string]string
}

// Request is what the server was sent.
type Request struct {
	Verb, Path string
	Header     http.Header
	Body       []byte
}

// Server answers from a script; past its end it repeats the last answer.
type Server struct {
	URL    string
	mu     sync.Mutex
	script []Answer
	got    []Request
}

// Start serves the script until the test ends.
func Start(t *testing.T, script ...Answer) *Server {
	s := &Server{script: script}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.got = append(s.got, Request{r.Method, r.URL.RequestURI(), r.Header.Clone(), b})
		n := len(s.got)
		step := Answer{Status: 200, Body: "null"}
		if len(s.script) > 0 {
			step = s.script[min(n, len(s.script))-1]
		}
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-Id", fmt.Sprintf("rid-%d", n))
		for k, v := range step.Header {
			w.Header().Set(k, v)
		}
		if step.Status == 0 {
			step.Status = 200
		}
		w.WriteHeader(step.Status)
		_, _ = io.WriteString(w, step.Body)
	}))
	t.Cleanup(srv.Close)
	s.URL = srv.URL
	return s
}

// Seen returns what was sent so far.
func (s *Server) Seen() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.got...)
}

// Served reads one group of the module's copy of served.json.
func Served(t *testing.T, path, group string) []string {
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var all map[string]json.RawMessage
	if err := json.Unmarshal(b, &all); err != nil {
		t.Fatal(err)
	}
	var out []string
	if err := json.Unmarshal(all[group], &out); err != nil {
		t.Fatal(err)
	}
	return out
}
