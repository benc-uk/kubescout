/*
Purpose: Test the dashboard's HTTP routes, triage boundary and shutdown.
Flow: Offline Sources, request recorders and a loopback listener exercise
embedded assets, bounded JSON, same-origin human actions, explicit failures
and graceful cancellation without contacting Kubernetes or an AI model.
*/
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
	triage   func(string, string, bool) (DetailResponse, error)
}

// List returns the configured snapshot and error without making upstream requests.
func (s fixtureSource) List(context.Context) (Snapshot, error) { return s.snapshot, s.err }

// Get supplies the fixture detail and error for HTTP route tests.
func (s fixtureSource) Get(context.Context, string) (DetailResponse, error) {
	return s.detail, s.err
}

// SetDismissed forwards typed triage arguments to the test callback or returns the configured response.
func (s fixtureSource) SetDismissed(_ context.Context, name, version string, dismissed bool) (DetailResponse, error) {
	if s.triage != nil {
		return s.triage(name, version, dismissed)
	}

	return s.detail, s.err
}

// TestTriageHTTPBoundary checks method, origin, action-header, content-type, and bounded JSON validation.
// Rejected requests must never reach the triage source or enable cross-origin access.
func TestTriageHTTPBoundary(t *testing.T) {
	for _, tt := range []struct {
		name, method, body, origin, site, content, action string
		status                                            int
	}{
		{"dismiss", "POST", `{"version":"seen","dismissed":true}`, "http://dashboard", "same-origin", "application/json", "triage", 200},
		{"restore", "POST", `{"version":"seen","dismissed":false}`, "", "", "application/json", "triage", 200},
		{"cross-site", "POST", `{"version":"seen","dismissed":true}`, "https://evil.invalid", "cross-site", "application/json", "triage", 403},
		{"origin-fallback", "POST", `{"version":"seen","dismissed":true}`, "https://evil.invalid", "", "application/json", "triage", 403},
		{"missing-header", "POST", `{"version":"seen","dismissed":true}`, "", "", "application/json", "", 403},
		{"form", "POST", "dismissed=true", "", "", "application/x-www-form-urlencoded", "triage", 415},
		{"extra-field", "POST", `{"version":"seen","dismissed":true,"namespace":"other"}`, "", "", "application/json", "triage", 400},
		{"duplicate", "POST", `{"version":"seen","dismissed":true,"dismissed":false}`, "", "", "application/json", "triage", 400},
		{"missing", "POST", `{"version":"seen"}`, "", "", "application/json", "triage", 400},
		{"null", "POST", `{"version":"seen","dismissed":null}`, "", "", "application/json", "triage", 400},
		{"wrong-type", "POST", `{"version":"seen","dismissed":"true"}`, "", "", "application/json", "triage", 400},
		{"trailing", "POST", `{"version":"seen","dismissed":true}{}`, "", "", "application/json", "triage", 400},
		{"oversized", "POST", `{"version":"` + strings.Repeat("x", 1024) + `","dismissed":true}`, "", "", "application/json", "triage", 400},
		{"safe-get", "GET", "", "", "", "", "", 405},
		{"delete", "DELETE", "", "", "", "", "", 405},
		{"preflight", "OPTIONS", "", "", "", "", "", 405},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			source := fixtureSource{triage: func(name, version string, dismissed bool) (DetailResponse, error) {
				calls++

				if name != "incident-one" || version != "seen" || dismissed != (tt.name == "dismiss") {
					t.Fatal("incorrect typed triage request")
				}

				return DetailResponse{}, nil
			}}
			request := httptest.NewRequest(tt.method, "http://dashboard/api/incidents/incident-one/triage", strings.NewReader(tt.body))
			request.Header.Set("Origin", tt.origin)
			request.Header.Set("Sec-Fetch-Site", tt.site)
			request.Header.Set("Content-Type", tt.content)
			request.Header.Set("X-KubeScout-Action", tt.action)

			response := httptest.NewRecorder()
			Handler(source, log.New(io.Discard, "", 0)).ServeHTTP(response, request)

			if response.Code != tt.status || (calls == 1) != (tt.status == 200) || !json.Valid(response.Body.Bytes()) {
				t.Fatalf("unexpected triage result: calls=%d status=%d body=%s", calls, response.Code, response.Body)
			}

			if response.Header().Get("Access-Control-Allow-Origin") != "" {
				t.Fatal("triage must not permit cross-origin access")
			}
		})
	}
}

// TestHTTPContract checks embedded assets, API routing, security headers, branding, and namespace-query rejection.
func TestHTTPContract(t *testing.T) {
	var logs bytes.Buffer

	source := fixtureSource{
		snapshot: Snapshot{Namespace: "kubescout", FetchedAt: time.Now(), Incidents: []Summary{}},
		detail:   DetailResponse{FetchedAt: time.Now(), Incident: Detail{Summary: Summary{Name: "incident-one"}}},
	}
	handler := Handler(source, log.New(&logs, "", 0))

	for _, test := range []struct {
		method, path, content string
		status                int
	}{
		{"GET", "/", "text/html", 200},
		{"GET", "/app.js", "text/javascript", 200},
		{"GET", "/model.js", "text/javascript", 200},
		{"GET", "/styles.css", "text/css", 200},
		{"GET", "/logo.png", "image/png", 200},
		{"HEAD", "/", "text/html", 200},
		{"POST", "/", "text/plain", 405},
		{"GET", "/api/incidents", "application/json", 200},
		{"GET", "/api/incidents/incident-one", "application/json", 200},
		{"POST", "/api/incidents", "application/json", 405},
		{"PATCH", "/api/incidents/incident-one", "application/json", 405},
		{"DELETE", "/api/incidents/incident-one", "application/json", 405},
		{"GET", "/api/secret", "application/json", 404},
		{"GET", "/api/incidents?namespace=other", "application/json", 400},
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
				t.Fatal("incident data must not be cached")
			}

			if strings.Contains(test.path, "/api/") && !json.Valid(response.Body.Bytes()) {
				t.Fatal("API did not return JSON")
			}

			if test.path == "/api/incidents" && test.status == 200 && !strings.Contains(response.Body.String(), `"incidents":[]`) {
				t.Fatal("empty list must be an array")
			}

			if test.path == "/api/incidents/incident-one" && test.status == 200 &&
				(!strings.Contains(response.Body.String(), `"incident":`) || strings.Contains(response.Body.String(), `"item":`)) {
				t.Fatal("incident detail must use the incident response property")
			}

			if test.method == "GET" && test.path == "/" {
				for _, text := range []string{"<title>KubeScout Dashboard</title>", ">KubeScout</span>", "Your AI Kubernetes investigator."} {
					if !strings.Contains(response.Body.String(), text) {
						t.Fatalf("missing dashboard branding: %s", text)
					}
				}
			}

			if test.method == "GET" && test.path == "/app.js" && !strings.Contains(response.Body.String(), `"X-KubeScout-Action": "triage"`) {
				t.Fatal("embedded dashboard must use the rebranded triage header")
			}
		})
	}
}

// TestHTTPFailuresAndSizeLimit checks explicit bounded API errors and diagnostics without private upstream details.
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
			handler.ServeHTTP(response, httptest.NewRequest("GET", "/api/incidents", nil))

			if response.Code < 500 || !strings.Contains(response.Body.String(), test.code) || !strings.Contains(logs.String(), test.code) {
				t.Fatalf("failure not explicit: %s / %s", response.Body, &logs)
			}

			if strings.Contains(response.Body.String()+logs.String(), "private") {
				t.Fatal("leaked internal error")
			}
		})
	}
}

// TestServeShutdown verifies a loopback server responds and exits cleanly when its context is canceled.
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
