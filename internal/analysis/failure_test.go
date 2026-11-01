/*
Purpose: Verify safe failure classification and precise quota/retry diagnostics.
Flow: Offline HTTP fixtures exercise the real Responses SDK for rate limits,
authentication, protocol failures and deadlines; direct transport fixtures check
local budgets and reject untrusted header text without producing incident actions.
*/
package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
)

type failingTransport struct{}

// RoundTrip injects a private transport error to verify that raw errors never reach diagnostics.
func (failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("private-transport-error https://private-endpoint/?key=private-key")
}

// TestProviderRetryHeaders checks exact retry boundaries, Azure hints and numeric quota headers.
func TestProviderRetryHeaders(t *testing.T) {
	now := time.Date(2026, 11, 1, 16, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name    string
		headers http.Header
		delay   time.Duration
		source  string
	}{
		{"seconds", http.Header{"Retry-After": {"60"}}, time.Minute, "Retry-After"},
		{"date", http.Header{"Retry-After": {now.Add(90 * time.Second).Format(http.TimeFormat)}}, 90 * time.Second, "Retry-After"},
		{"milliseconds", http.Header{"Retry-After-Ms": {"1500"}}, 1500 * time.Millisecond, "retry-after-ms"},
		{"azure milliseconds", http.Header{"X-Ms-Retry-After-Ms": {"2500"}}, 2500 * time.Millisecond, "x-ms-retry-after-ms"},
		{"latest hint", http.Header{"Retry-After": {"1"}, "Retry-After-Ms": {"3000"}, "X-Ms-Retry-After-Ms": {"2000"}}, 3 * time.Second, "retry-after-ms"},
		{"zero", http.Header{"Retry-After": {"0"}}, 0, "Retry-After"},
		{"missing", http.Header{}, 0, ""},
		{"past date", http.Header{"Retry-After": {now.Add(-time.Second).Format(http.TimeFormat)}}, 0, ""},
		{"negative", http.Header{"Retry-After": {"-1"}}, 0, ""},
		{"overflow", http.Header{"Retry-After": {"9223372036854775807"}}, 0, ""},
		{"oversized", http.Header{"Retry-After": {strings.Repeat("9", 100)}}, 0, ""},
		{"untrusted", http.Header{"Retry-After": {"private-header"}}, 0, ""},
		{"bad plus valid", http.Header{"Retry-After": {"private-header"}, "Retry-After-Ms": {"1000"}}, time.Second, "retry-after-ms"},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.headers.Set("x-ratelimit-remaining-requests", "0")
			test.headers.Set("x-ratelimit-remaining-tokens", "42")
			test.headers.Set("x-request-id", "private-header")

			failure := providerFailure(&http.Response{StatusCode: http.StatusTooManyRequests, Header: test.headers}, now)
			if failure.Kind != "provider_rate_limit" || failure.HTTPStatus != 429 || failure.RetryHintSource != test.source {
				t.Fatalf("unexpected classification: %+v", failure)
			}

			if test.source == "" {
				if failure.RetryNotBefore != nil {
					t.Fatal("invented a retry time")
				}
			} else if failure.RetryNotBefore == nil || !failure.RetryNotBefore.Equal(now.Add(test.delay)) {
				t.Fatalf("retry boundary differs: %+v", failure)
			}

			if failure.RemainingRequests == nil || *failure.RemainingRequests != 0 || failure.RemainingTokens == nil || *failure.RemainingTokens != 42 {
				t.Fatal("numeric quota details missing")
			}

			data, err := json.Marshal(failure)
			if err != nil || strings.Contains(string(data)+failure.Error(), "private-header") {
				t.Fatal("untrusted header text leaked")
			}
		})
	}

	for _, value := range []string{"", "-1", "+1", "1.5", "private-header", strings.Repeat("9", 100), "9223372036854775808"} {
		if numericHeader(value) != nil {
			t.Fatal("invalid quota header accepted")
		}
	}
}

// TestSDKFailureDiagnostics checks status, quota usage and redaction through actual SDK requests.
func TestSDKFailureDiagnostics(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		kind   string
	}{
		{"provider rate limit", 429, "provider_rate_limit"},
		{"unauthorized", 401, "provider_authentication"},
		{"forbidden", 403, "provider_authentication"},
		{"service unavailable", 503, "provider_http_error"},
		{"bad request", 400, "provider_http_error"},
		{"invalid protocol", 200, "tool_or_model_protocol"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Retry-After", "60")
				w.Header().Set("x-ratelimit-remaining-tokens", "0")
				w.Header().Set("x-request-id", "private-header")
				w.WriteHeader(test.status)
				fmt.Fprint(w, `{"error":{"message":"private-provider-body"}}`)
			}))
			defer server.Close()

			a := Analyzer{Config: config.Config{
				Endpoint: server.URL + "/openai/v1/", APIKey: "private-key", Model: "fixture",
				MaxInputChars: 20000, MaxOutputTokens: 2000, MaxToolCalls: config.DefaultMaxToolCalls, CallsPerHour: 60,
				RequestTimeout: time.Second, InvestigationTimeout: 5 * time.Second,
			}}
			started := time.Now()

			decision, err := a.Investigate(t.Context(), inputFixture())

			var failure *Failure

			if !errors.As(err, &failure) || failure.Kind != test.kind || len(decision.Actions) != 0 || calls.Load() != 1 {
				t.Fatalf("unexpected failure or retry: %v", err)
			}

			if failure.ModelRequests != 1 || failure.ModelRequestLimit != 6 || failure.CallsInHour != 1 ||
				failure.CallsPerHourLimit != 60 || failure.ToolCalls != 0 || failure.ToolCallLimit != a.Config.MaxToolCalls ||
				failure.InputBytes == 0 || failure.MaxOutputTokens != 2000 || failure.RequestTimeout != "1s" || failure.InvestigationTimeout != "5s" {
				t.Fatalf("failure counters missing: %+v", failure)
			}

			if test.status != 200 {
				if failure.HTTPStatus != test.status || failure.RetryNotBefore == nil ||
					failure.RetryNotBefore.Before(started.Add(time.Minute)) || failure.RetryNotBefore.After(time.Now().Add(time.Minute)) {
					t.Fatalf("HTTP retry details missing: %+v", failure)
				}
			}

			data, marshalErr := json.Marshal(failure)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}

			for _, private := range []string{"private-provider-body", "private-header", "private-key", server.URL} {
				if strings.Contains(string(data)+err.Error(), private) {
					t.Fatal("private error data leaked")
				}
			}
		})
	}
}

// TestLocalQuotaDiagnostics checks hourly slot expiry and rejection without another HTTP request.
func TestLocalQuotaDiagnostics(t *testing.T) {
	now := time.Now()
	oldest := now.Add(-30 * time.Minute)

	var calls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		respond(w, textResponse(`{"outcome":"no_action","reason":"fixture","actions":[]}`))
	}))
	defer server.Close()

	a := Analyzer{Config: config.Config{
		Endpoint: server.URL + "/openai/v1/", APIKey: "fixture", Model: "fixture",
		MaxInputChars: 20000, MaxOutputTokens: 2000, MaxToolCalls: config.DefaultMaxToolCalls, CallsPerHour: 2,
		RequestTimeout: time.Second, InvestigationTimeout: 5 * time.Second,
	}, calls: []time.Time{now.Add(-2 * time.Hour), oldest, now.Add(-time.Minute)}}

	decision, err := a.Investigate(t.Context(), inputFixture())

	var failure *Failure

	if !errors.As(err, &failure) || failure.Kind != "local_hourly_quota" || len(decision.Actions) != 0 || calls.Load() != 0 {
		t.Fatalf("quota produced HTTP request or incorrect failure: %v", err)
	}

	if failure.CallsInHour != 2 || failure.CallsPerHourLimit != 2 || failure.ModelRequests != 0 ||
		failure.RetryNotBefore == nil || !failure.RetryNotBefore.Equal(oldest.Add(time.Hour)) || failure.RetryHintSource != "local_rolling_hour" {
		t.Fatalf("quota details incorrect: %+v", failure)
	}

	if a.admit(oldest.Add(time.Hour).Add(-time.Nanosecond)) == nil {
		t.Fatal("request allowed before expiry")
	}

	if err := a.admit(oldest.Add(time.Hour)); err != nil {
		t.Fatalf("slot did not expire at exact boundary: %v", err)
	}
}

// TestSDKDeadlineDiagnostics distinguishes a per-request timeout from the investigation deadline.
func TestSDKDeadlineDiagnostics(t *testing.T) {
	for _, test := range []struct {
		name                   string
		request, investigation time.Duration
		kind                   string
	}{
		{"request", 20 * time.Millisecond, time.Second, "model_request_timeout"},
		{"investigation", time.Second, 20 * time.Millisecond, "investigation_deadline"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				time.Sleep(100 * time.Millisecond)
				respond(w, textResponse(`{"outcome":"no_action","reason":"fixture","actions":[]}`))
			}))
			defer server.Close()

			a := Analyzer{Config: config.Config{
				Endpoint: server.URL + "/openai/v1/", APIKey: "fixture", Model: "fixture",
				MaxInputChars: 20000, MaxOutputTokens: 2000, MaxToolCalls: config.DefaultMaxToolCalls, CallsPerHour: 60,
				RequestTimeout: test.request, InvestigationTimeout: test.investigation,
			}}
			decision, err := a.Investigate(t.Context(), inputFixture())

			var failure *Failure

			if !errors.As(err, &failure) || failure.Kind != test.kind || len(decision.Actions) != 0 || calls.Load() != 1 {
				t.Fatalf("wrong deadline classification: %v", err)
			}
		})
	}
}

// TestTransportBudgetDiagnostics verifies distinct per-request and cumulative byte failures.
func TestTransportBudgetDiagnostics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, strings.Repeat("x", 8)) }))
	defer server.Close()

	for _, test := range []struct {
		name, kind      string
		inputLimit      int
		outputLimit     int64
		previousInput   int
		previousOutput  int64
		observed, limit int64
	}{
		{"input", "local_model_input_limit", 1, 100, 0, 0, 2, 1},
		{"cumulative input", "local_cumulative_input_limit", 2, 100, 6, 0, 8, 6},
		{"output", "local_model_output_limit", 100, 4, 0, 0, 5, 4},
		{"cumulative output", "local_cumulative_output_limit", 100, 8, 0, 16, 24, 16},
	} {
		t.Run(test.name, func(t *testing.T) {
			bounded := &boundedClient{
				client: server.Client(), admit: func(time.Time) error { return nil },
				inputLimit: test.inputLimit, limit: test.outputLimit, inputBytes: test.previousInput, bytes: test.previousOutput,
			}

			request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL, strings.NewReader("{}"))
			if err != nil {
				t.Fatal(err)
			}

			_, err = bounded.Do(request)

			var failure *Failure

			if !errors.As(err, &failure) || failure.Kind != test.kind || failure.Observed != test.observed || failure.Limit != test.limit {
				t.Fatalf("budget details incorrect: %v", err)
			}
		})
	}
}

// TestSafeTransportAndCancellationDiagnostics checks private-error suppression and explicit cancellation.
func TestSafeTransportAndCancellationDiagnostics(t *testing.T) {
	for _, test := range []struct {
		name, kind string
		cancel     bool
	}{
		{"transport", "model_transport", false},
		{"cancellation", "investigation_canceled", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := Analyzer{Config: config.Config{
				Endpoint: "https://private-endpoint/openai/v1/", APIKey: "private-key", Model: "fixture",
				MaxInputChars: 20000, MaxOutputTokens: 2000, MaxToolCalls: config.DefaultMaxToolCalls, CallsPerHour: 60,
				RequestTimeout: time.Second, InvestigationTimeout: time.Second,
			}, HTTPClient: &http.Client{Transport: failingTransport{}}}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			if test.cancel {
				cancel()
			}

			decision, err := a.Investigate(ctx, inputFixture())

			var failure *Failure

			if !errors.As(err, &failure) || failure.Kind != test.kind || len(decision.Actions) != 0 {
				t.Fatalf("wrong transport or cancellation classification: %v", err)
			}

			data, marshalErr := json.Marshal(failure)
			if marshalErr != nil || strings.Contains(string(data)+err.Error(), "private-") {
				t.Fatal("raw transport error leaked")
			}
		})
	}
}

// TestResponseBodyTimeoutDiagnostics distinguishes slow response reads from byte-budget exhaustion.
func TestResponseBodyTimeoutDiagnostics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush fixture headers: %v", err)
			return
		}

		time.Sleep(100 * time.Millisecond)
		fmt.Fprint(w, "{}")
	}))
	defer server.Close()

	a := Analyzer{Config: config.Config{
		Endpoint: server.URL + "/openai/v1/", APIKey: "fixture", Model: "fixture",
		MaxInputChars: 20000, MaxOutputTokens: 2000, MaxToolCalls: config.DefaultMaxToolCalls, CallsPerHour: 60,
		RequestTimeout: 20 * time.Millisecond, InvestigationTimeout: time.Second,
	}}
	decision, err := a.Investigate(t.Context(), inputFixture())

	var failure *Failure

	if !errors.As(err, &failure) || failure.Kind != "model_request_timeout" || len(decision.Actions) != 0 {
		t.Fatalf("response timeout incorrectly classified: %v", err)
	}
}

// TestInitialInputBudgetDiagnostics reports the exact rejected input size before any HTTP request.
func TestInitialInputBudgetDiagnostics(t *testing.T) {
	in := inputFixture()

	payload, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}

	a := Analyzer{Config: config.Config{MaxInputChars: len(payload) - 1, MaxToolCalls: config.DefaultMaxToolCalls, CallsPerHour: 60}}
	decision, err := a.Investigate(t.Context(), in)

	var failure *Failure

	if !errors.As(err, &failure) || failure.Kind != "local_investigation_input_limit" ||
		failure.Observed != int64(len(payload)) || failure.Limit != int64(len(payload)-1) ||
		failure.ModelRequests != 0 || len(decision.Actions) != 0 {
		t.Fatalf("initial input budget details incorrect: %v", err)
	}
}
