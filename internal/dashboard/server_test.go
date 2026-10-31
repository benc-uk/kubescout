package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fixtureSource struct {
	snapshot Snapshot
	detail   DetailResponse
	err      error
}

func (s fixtureSource) List(context.Context) (Snapshot, error) { return s.snapshot, s.err }
func (s fixtureSource) Get(context.Context, string) (DetailResponse, error) {
	return s.detail, s.err
}

func TestHTTPContract(t *testing.T) {
	var logs bytes.Buffer
	source := fixtureSource{snapshot: Snapshot{Namespace: "issues", FetchedAt: time.Now(), Items: []Summary{}}}
	handler := Handler(source, log.New(&logs, "", 0))
	for _, test := range []struct {
		method, path, content string
		status                int
	}{
		{"GET", "/", "text/html", 200},
		{"GET", "/app.js", "text/javascript", 200},
		{"GET", "/model.js", "text/javascript", 200},
		{"GET", "/styles.css", "text/css", 200},
		{"HEAD", "/", "text/html", 200},
		{"POST", "/", "text/plain", 405},
		{"GET", "/api/issues", "application/json", 200},
		{"GET", "/api/issues/issue-one", "application/json", 200},
		{"POST", "/api/issues", "application/json", 405},
		{"PATCH", "/api/issues/issue-one", "application/json", 405},
		{"DELETE", "/api/issues/issue-one", "application/json", 405},
		{"GET", "/api/secret", "application/json", 404},
		{"GET", "/api/issues?namespace=other", "application/json", 400},
		{"GET", "/missing", "text/plain", 404},
		{"GET", "/web/", "text/plain", 404},
		{"GET", "/testdata/browser.html", "text/plain", 404},
	} {
		t.Run(test.method+test.path, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
			if response.Code != test.status || !strings.Contains(response.Header().Get("Content-Type"), test.content) {
				t.Fatalf("response %d %v: %s", response.Code, response.Header(), response.Body.String())
			}
			for _, header := range []string{"Content-Security-Policy", "X-Content-Type-Options", "X-Frame-Options", "Referrer-Policy"} {
				if response.Header().Get(header) == "" {
					t.Fatalf("missing %s", header)
				}
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("issue data must not be cached")
			}
			if strings.Contains(test.path, "/api/") && !json.Valid(response.Body.Bytes()) {
				t.Fatal("API did not return JSON")
			}
			if test.path == "/api/issues" && test.status == 200 && !strings.Contains(response.Body.String(), `"items":[]`) {
				t.Fatal("empty list must be an array")
			}
		})
	}
}

func TestHTTPFailuresAndSizeLimit(t *testing.T) {
	for _, test := range []struct {
		name   string
		source fixtureSource
		code   string
	}{
		{"sanitized", fixtureSource{err: errors.New("private upstream body")}, "internal_error"},
		{"known", fixtureSource{err: &APIError{503, "access_denied", "Check RBAC."}}, "access_denied"},
		{"oversized", fixtureSource{snapshot: Snapshot{Namespace: strings.Repeat("x", MaxResponseBytes)}}, "response_too_large"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			handler := Handler(test.source, log.New(&logs, "", 0))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest("GET", "/api/issues", nil))
			if response.Code < 500 || !strings.Contains(response.Body.String(), test.code) || !strings.Contains(logs.String(), test.code) {
				t.Fatalf("failure not explicit: %s / %s", response.Body, &logs)
			}
			if strings.Contains(response.Body.String()+logs.String(), "private") {
				t.Fatal("leaked internal error")
			}
		})
	}
}

func TestServeShutdown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, listener, Handler(fixtureSource{}, log.New(io.Discard, "", 0)), io.Discard) }()
	response, err := http.Get("http://" + listener.Addr().String() + "/")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal(response.Status)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop gracefully")
	}
}
