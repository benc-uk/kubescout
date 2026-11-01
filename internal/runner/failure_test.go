/*
Purpose: Verify failure logging preserves retry scheduling and incident boundaries.
Flow: Offline candidate and HTTP model fixtures check readable Go-labelled diagnostics,
exact backoff, safe once errors and stderr failures without live cluster resources.
*/
package runner

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/analysis"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/output"
)

type outputFailure struct{}

// TestDeferredFailureDiagnostics checks exact existing retry timing and readable failure details.
func TestDeferredFailureDiagnostics(t *testing.T) {
	now := time.Date(2026, 11, 1, 16, 0, 0, 0, time.FixedZone("fixture", 3600))
	hint := now.Add(time.Hour).UTC()
	failure := &analysis.Failure{
		Kind: "local_hourly_quota", CallsInHour: 60, CallsPerHourLimit: 60,
		ModelRequestLimit: 6, ToolCallLimit: 64, RetryNotBefore: &hint,
	}

	for _, test := range []struct {
		failures int
		delay    time.Duration
	}{
		{0, 2 * time.Second}, {1, 4 * time.Second}, {2, 15 * time.Minute}, {8, 15 * time.Minute},
	} {
		t.Run(fmt.Sprint(test.failures), func(t *testing.T) {
			var stdout, stderr bytes.Buffer

			r := Runner{Config: options(), Output: &output.Writer{Out: &stdout, Err: &stderr}}

			c := &candidate{investigation: incident.InvestigationContext{Anchor: "pod:apps:bad"}, failures: test.failures}
			if err := r.deferInvestigation(c, fmt.Errorf("assessment failed: %w", failure), now); err != nil {
				t.Fatal(err)
			}

			if !strings.Contains(stderr.String(), "[GO][SCHEDULE]") || !strings.Contains(stderr.String(), "investigation_deferred") ||
				!strings.Contains(stderr.String(), "local_hourly_quota") {
				t.Fatalf("failure diagnostic missing details: %s", &stderr)
			}

			if !strings.Contains(stderr.String(), fmt.Sprintf("consecutive_failures=%d retry_after_seconds=%.0f next_retry_at=%s",
				test.failures+1, test.delay.Seconds(), now.Add(test.delay).UTC().Format(time.RFC3339))) || !c.next.Equal(now.Add(test.delay)) {
				t.Fatalf("retry schedule changed or logged inaccurately: %s", &stderr)
			}

			if !strings.Contains(stderr.String(), hint.Format(time.RFC3339Nano)) || stdout.Len() != 0 {
				t.Fatal("retry hint lost or diagnostic published as incident")
			}
		})
	}
}

// TestDeferredNonModelFailureAndOutputErrors checks legacy errors and explicit stderr write failure.
func TestDeferredNonModelFailureAndOutputErrors(t *testing.T) {
	var stdout, stderr bytes.Buffer

	r := Runner{Config: options(), Output: &output.Writer{Out: &stdout, Err: &stderr}}
	c := &candidate{investigation: incident.InvestigationContext{Anchor: "pod:apps:bad"}}

	if err := r.deferInvestigation(c, errors.New("superseded evidence"), time.Now()); err != nil {
		t.Fatal(err)
	}

	if strings.Contains(stderr.String(), "model_requests") || !strings.Contains(stderr.String(), "retry_after_seconds=2 ") || stdout.Len() != 0 {
		t.Fatal("non-model failure received invented model details")
	}

	r.Output.Err = outputFailure{}
	if err := r.deferInvestigation(c, errors.New("superseded evidence"), time.Now()); err == nil {
		t.Fatal("diagnostic output failure hidden")
	}
}

// Write simulates an unavailable diagnostic stream for offline runner tests.
func (outputFailure) Write([]byte) (int, error) { return 0, errors.New("closed") }

// TestOnceProviderRateLimitDiagnostics checks safe execution errors and zero incident mutations.
func TestOnceProviderRateLimitDiagnostics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"error":{"message":"private-provider-body"}}`)
	}))
	defer server.Close()

	cfg := options()
	cfg.Endpoint, cfg.APIKey, cfg.Model = server.URL+"/openai/v1/", "fixture", "fixture"
	cfg.MaxInputChars, cfg.MaxOutputTokens, cfg.CallsPerHour = 20000, 2000, 60
	cfg.RequestTimeout, cfg.InvestigationTimeout = time.Second, time.Second
	ai := &analysis.Analyzer{Config: cfg}
	repo := &incident.Memory{}
	source := &fakeSource{objects: []health.Object{failedPod()}}

	var stdout, stderr bytes.Buffer

	r := Runner{Config: cfg, Source: source, Analyzer: ai, Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"},
		Output: &output.Writer{Out: &stdout, Err: &stderr}}
	if code := r.Once(t.Context()); code != 1 {
		t.Fatalf("exit code=%d", code)
	}

	if !strings.Contains(stderr.String(), "Assessment incomplete") || len(repo.Records) != 0 ||
		strings.Contains(stdout.String(), "[GO][INCIDENT]") {
		t.Fatal("provider failure violated once output or incident boundary")
	}

	for _, detail := range []string{"provider_rate_limit", "http_status=429", "model_requests=1/6", "retry_not_before="} {
		if !strings.Contains(stderr.String(), detail) {
			t.Fatalf("once error missing %s: %s", detail, &stderr)
		}
	}

	if strings.Contains(stdout.String()+stderr.String(), "private-provider-body") {
		t.Fatal("raw provider body leaked")
	}
}
