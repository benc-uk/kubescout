/*
Purpose: Provide an opt-in, loopback-only dashboard browser-test harness.
Flow: In-memory cluster-scoped incidents supply desktop modal checks with workload namespace,
empty detail lists, rendering, error, latency and revision-scoped triage scenarios. The harness
serves until signalled and never contacts a live cluster or model.
Stored native descriptions exercise collapsible resources, one exact-copy text block,
independent long-output scrolling, full-screen reading with refresh/focus restoration,
partial coverage and unavailable output without live resource reads.
*/
package dashboard

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/incident"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type browserSource struct {
	mu       sync.Mutex
	items    []Detail
	mode     string
	requests int
}

// List counts browser fixture refreshes and returns incident summaries, an empty list, or an injected error.
func (s *browserSource) List(context.Context) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.requests++
	if s.mode == "error" {
		return Snapshot{}, &APIError{503, "access_denied", "Fixture: check cluster-scoped incident RBAC."}
	}

	result := Snapshot{FetchedAt: time.Now().UTC(), Incidents: []Summary{}}

	if s.mode != "empty" {
		for _, item := range s.items {
			result.Incidents = append(result.Incidents, item.Summary)
		}
	}

	return result, nil
}

// Get returns a named fixture detail with optional cancellable delay, deletion, or access failure.
func (s *browserSource) Get(ctx context.Context, name string) (DetailResponse, error) {
	s.mu.Lock()
	mode := s.mode

	var found *Detail

	for _, item := range s.items {
		if item.Name == name {
			found = &item
			break
		}
	}
	s.mu.Unlock()

	if mode == "slow" {
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return DetailResponse{}, ctx.Err()
		}
	}

	if mode == "error" {
		return DetailResponse{}, &APIError{503, "access_denied", "Fixture: check cluster-scoped incident RBAC."}
	}

	if mode == "deleted" || found == nil {
		return DetailResponse{}, &APIError{404, "not_found", "Incident is no longer available; refresh the list."}
	}

	return DetailResponse{FetchedAt: time.Now().UTC(), Incident: *found}, nil
}

// SetDismissed updates fixture triage and its version under lock, rejecting stale versions or injected denial.
func (s *browserSource) SetDismissed(_ context.Context, name, version string, dismissed bool) (DetailResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mode == "error" {
		return DetailResponse{}, &APIError{403, "triage_denied", "Fixture: triage access denied."}
	}

	for i := range s.items {
		item := &s.items[i]
		if item.Name != name {
			continue
		}

		if item.Version != version {
			return DetailResponse{}, triageConflict()
		}

		item.Dismissed = dismissed
		item.Version = fmt.Sprintf("%x", sha256.Sum256([]byte(version+"triage")))

		return DetailResponse{FetchedAt: time.Now().UTC(), Incident: *item}, nil
	}

	return DetailResponse{}, &APIError{404, "not_found", "Incident no longer available."}
}

// TestBrowserFixture is an opt-in, loopback-only browser harness, never part of the production server.
func TestBrowserFixture(t *testing.T) {
	if os.Getenv("K8S_DASHBOARD_BROWSER") != "yes" {
		t.Skip("set K8S_DASHBOARD_BROWSER=yes to serve offline browser fixtures on 127.0.0.1:8000")
	}

	source := &browserSource{}

	for i := range 64 {
		obj := incidentObject(t, fmt.Sprintf("incident-%03d", i))
		reader := &Reader{}

		item, err := reader.decode(&unstructured.Unstructured{Object: obj})
		if err != nil {
			t.Fatal(err)
		}

		item.Severity = []string{"critical", "high", "medium", "low"}[i%4]
		item.Analysis.Severity = item.Severity
		item.Title = fmt.Sprintf("Workload %02d needs investigation", i)
		item.Analysis.Title = item.Title
		item.Anchor.Name = fmt.Sprintf("workload-%02d", i)
		item.Resources = []Resource{item.Anchor}
		item.Evidence = dashboardEvidence()
		item.Evidence.Descriptions[0].Output.Text += strings.Repeat("\n  api       Running   Ready   0 restarts", 60)
		item.Evidence.Descriptions = append(item.Evidence.Descriptions,
			incident.ResourceDescription{
				EvidenceID: "kubectl-describe-2",
				Resource:   incident.Resource{Ref: "deployment-ref", Kind: "Deployment", Namespace: "apps", Name: "checkout"},
				Source:     incident.DescribeSource, ObservedAt: item.Evidence.Descriptions[0].ObservedAt,
				Output: incident.DescriptionOutput{Status: "captured", Text: "Name:\tcheckout\nReplicas:\t3 desired | 2 available\nEvents:\t<none>" +
					strings.Repeat("\n  replica   Running   Ready", 50)},
			},
			incident.ResourceDescription{
				EvidenceID: "kubectl-describe-3",
				Resource:   incident.Resource{Ref: "node-ref", Kind: "Node", Name: "worker-1"},
				Source:     incident.DescribeSource, ObservedAt: item.Evidence.Descriptions[0].ObservedAt,
				Output: incident.DescriptionOutput{Status: "unavailable", Text: "kubectl describe unavailable. This is not evidence of health."},
			})

		item.Lifecycle.UpdatedAt = item.Lifecycle.UpdatedAt.Add(time.Duration(i) * time.Minute)
		if i >= 60 {
			item.Lifecycle.State = "resolved"
			resolved := item.Lifecycle.UpdatedAt
			item.Lifecycle.ResolvedAt = &resolved
		}

		switch i {
		case 0:
			item.Title = `<img src=x onerror="window.injected=true"> is text, not HTML`
			item.Analysis.Title = item.Title
			item.Analysis.Explanation = strings.Repeat("Long but readable evidence summary. ", 100)
			item.Analysis.Recommendations = []string{`<script>window.injected=true</script>`, "Inspect configuration."}
		case 1:
			item.Anchor = Resource{Kind: "Node", Name: "worker-1"}
			item.Resources = []Resource{item.Anchor}
			item.Family = "node-health"
		case 2:
			item.Anchor.Namespace = "payments"
			item.Resources = []Resource{item.Anchor}
			item.Observer = "other-observer"
		case 3:
			item.Family = "service-availability"
			item.Anchor = Resource{Kind: "Service", Namespace: "apps", Name: "web"}
			item.Resources = []Resource{item.Anchor}
		case 4:
			item.Resources = []Resource{}
			item.Analysis.Recommendations = []string{}
			item.Analysis.EvidenceIDs = []string{}
		}

		source.items = append(source.items, item)
	}

	handler := Handler(source, log.New(io.Discard, "", 0))
	mux := http.NewServeMux()
	mux.Handle("/", handler)
	mux.HandleFunc("GET /__test/browser", func(w http.ResponseWriter, r *http.Request) {
		data, err := assets.ReadFile("web/index.html")
		if err != nil {
			t.Error(err)
			http.Error(w, "fixture unavailable", 500)

			return
		}

		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, strings.Replace(string(data), "</body>",
			`<pre id="test-results" style="white-space:pre-wrap;overflow-wrap:anywhere">Running browser tests...</pre><script type="module" src="/__test/browser-tests.js"></script></body>`, 1))
	})
	mux.HandleFunc("GET /__test/browser-tests.js", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "testdata/browser-tests.js")
	})
	mux.HandleFunc("/__test/state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		source.mu.Lock()
		defer source.mu.Unlock()

		if r.Method == http.MethodPost {
			var state struct{ Mode string }
			if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&state) != nil {
				http.Error(w, "invalid state", 400)
				return
			}

			switch state.Mode {
			case "normal", "error", "empty", "deleted", "slow":
				source.mode = state.Mode
			case "update":
				source.mode = "normal"
				source.items[0].Lifecycle.Revision++
				source.items[0].Dismissed = false
				source.items[0].Version = fmt.Sprintf("%x", sha256.Sum256([]byte(source.items[0].Version+"update")))
			default:
				http.Error(w, "invalid mode", 400)
				return
			}
		} else if r.Method != http.MethodGet {
			http.Error(w, "invalid method", 405)
			return
		}

		_ = json.NewEncoder(w).Encode(map[string]any{"mode": source.mode, "requests": source.requests})
	})

	port := os.Getenv("K8S_DASHBOARD_BROWSER_PORT")
	if port == "" {
		port = "8000"
	}

	number, err := strconv.Atoi(port)
	if err != nil || number < 0 || number > 65535 {
		t.Fatal("K8S_DASHBOARD_BROWSER_PORT must be 0 through 65535 (0 selects an available port)")
	}

	listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(number)))
	if err != nil {
		t.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	t.Logf("Offline dashboard: http://%s/ ; browser tests: http://%s/__test/browser", listener.Addr(), listener.Addr())

	if err := Serve(ctx, listener, mux, os.Stderr); err != nil {
		t.Fatal(err)
	}
}
