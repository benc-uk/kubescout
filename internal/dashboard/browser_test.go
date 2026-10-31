package dashboard

import (
	"context"
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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type browserSource struct {
	mu       sync.Mutex
	items    []Detail
	mode     string
	requests int
}

func (s *browserSource) List(context.Context) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests++
	if s.mode == "error" {
		return Snapshot{}, &APIError{503, "access_denied", "Fixture: check namespace RBAC."}
	}
	result := Snapshot{Namespace: "issues", FetchedAt: time.Now().UTC(), Items: []Summary{}}
	if s.mode != "empty" {
		for _, item := range s.items {
			result.Items = append(result.Items, item.Summary)
		}
	}
	return result, nil
}

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
		return DetailResponse{}, &APIError{503, "access_denied", "Fixture: check namespace RBAC."}
	}
	if mode == "deleted" || found == nil {
		return DetailResponse{}, &APIError{404, "not_found", "Issue is no longer available; refresh the list."}
	}
	return DetailResponse{FetchedAt: time.Now().UTC(), Item: *found}, nil
}

// TestBrowserFixture is an opt-in, loopback-only browser harness, never part of the production server.
func TestBrowserFixture(t *testing.T) {
	if os.Getenv("K8S_DASHBOARD_BROWSER") != "yes" {
		t.Skip("set K8S_DASHBOARD_BROWSER=yes to serve offline browser fixtures on 127.0.0.1:8000")
	}
	source := &browserSource{}
	for i := range 64 {
		obj := issueObject(t, fmt.Sprintf("issue-%03d", i))
		reader := &Reader{namespace: "issues"}
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
		item.Lifecycle.UpdatedAt = item.Lifecycle.UpdatedAt.Add(time.Duration(i) * time.Minute)
		if i >= 60 {
			item.Lifecycle.Phase = "resolved"
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
