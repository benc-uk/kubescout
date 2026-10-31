package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s-ops-agent/internal/health"
	"k8s-ops-agent/internal/incident"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

func issueObject(t *testing.T, name string) map[string]any {
	t.Helper()
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	ref := health.Resource{Kind: "Deployment", Namespace: "apps", Name: "checkout", UID: "private-resource-uid"}
	value := incident.Incident{
		Identity:  incident.Identity{Observer: "kind-default", Anchor: ref, Family: "container-failure"},
		Resources: []health.Resource{ref},
		Analysis: incident.Analysis{
			Title: "Checkout unavailable", Severity: "high", Impact: "unavailable",
			Diagnosis: "container-exits", Explanation: "Repeated container exits.",
			EvidenceIDs: []string{"evidence-1"}, Recommendations: []string{"Check the application configuration."},
			Uncertainty: "The underlying cause is not confirmed.", Model: "fixture",
			PromptVersion: "incident-v1", AnalysisID: "analysis-1",
		},
		Lifecycle: incident.Lifecycle{Phase: "open", Revision: 1, RaisedAt: now, UpdatedAt: now},
	}
	spec, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&value)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{
		"apiVersion": incident.APIVersion, "kind": "ClusterIssue",
		"metadata": map[string]any{"name": name, "namespace": "issues", "resourceVersion": "42", "uid": "private-object-uid",
			"annotations": map[string]any{"private": "private-annotation"}},
		"spec": spec, "status": map[string]any{"private": "private-status"},
	}
}

func TestServiceAvailabilityFamily(t *testing.T) {
	obj := issueObject(t, "service-issue")
	spec := obj["spec"].(map[string]any)
	identity := spec["identity"].(map[string]any)
	identity["family"] = "service-availability"
	identity["anchor"].(map[string]any)["kind"] = "Service"
	reader := &Reader{namespace: "issues"}
	item, err := reader.decode(&unstructured.Unstructured{Object: obj})
	if err != nil || item.Family != "service-availability" || item.Anchor.Kind != "Service" {
		t.Fatalf("service issue rejected: %v", err)
	}
	html, err := assets.ReadFile("web/index.html")
	if err != nil || !strings.Contains(string(html), `<option value="service-availability">Service availability</option>`) {
		t.Fatal("service family missing from dashboard filter")
	}
}

func fixtureReader(t *testing.T, handler http.HandlerFunc) *Reader {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || !strings.HasPrefix(r.URL.Path, "/apis/health.k8s-ops-agent.io/v1alpha1/namespaces/issues/clusterissues") {
			t.Errorf("unexpected API scope: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	client, err := newClient(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewReader(client, "issues")
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func writePage(w http.ResponseWriter, items []map[string]any, version, token string) {
	_ = json.NewEncoder(w).Encode(map[string]any{
		"apiVersion": incident.APIVersion, "kind": "ClusterIssueList",
		"metadata": map[string]any{"resourceVersion": version, "continue": token}, "items": items,
	})
}

func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	var failure *APIError
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func TestListPaginationBoundaries(t *testing.T) {
	for _, count := range []int{0, 1, 1000, 1001} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			items := make([]map[string]any, count)
			for i := range items {
				items[i] = issueObject(t, fmt.Sprintf("issue-%04d", i))
			}
			var calls atomic.Int32
			reader := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Query().Get("limit") != "100" || r.URL.Query().Get("labelSelector") != "" {
					t.Error("must page all observers and phases")
				}
				start := 0
				if token := r.URL.Query().Get("continue"); token != "" {
					start, _ = strconv.Atoi(token)
				}
				end := min(start+100, count)
				next := ""
				if end < count {
					next = strconv.Itoa(end)
				}
				writePage(w, items[start:end], "42", next)
			})
			result, err := reader.List(context.Background())
			if count > MaxIssues {
				assertCode(t, err, "issue_limit")
				if result.Items != nil {
					t.Fatal("returned partial snapshot")
				}
				return
			}
			if err != nil || len(result.Items) != count || result.Items == nil || result.Namespace != "issues" || result.FetchedAt.IsZero() {
				t.Fatalf("invalid result count=%d: %v", len(result.Items), err)
			}
			if calls.Load() != int32(max(1, (count+99)/100)) {
				t.Fatal("incorrect page count")
			}
		})
	}
}

func TestPaginationFailures(t *testing.T) {
	for _, problem := range []string{"version", "missing-version", "duplicate", "token", "bad-record", "namespace", "second-page-error"} {
		t.Run(problem, func(t *testing.T) {
			item := issueObject(t, "issue-one")
			var calls atomic.Int32
			reader := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				version, token := "42", "next"
				items := []map[string]any{item}
				if call == 2 {
					token = ""
					switch problem {
					case "version":
						version = "43"
					case "missing-version":
						version = ""
					case "token":
						token, items = "next", nil
					case "bad-record":
						items = []map[string]any{{"metadata": map[string]any{"name": "issue-two"}}}
					case "namespace":
						items = []map[string]any{{"spec": item["spec"], "metadata": map[string]any{"name": "issue-two", "namespace": "other"}}}
					case "second-page-error":
						w.WriteHeader(500)
						return
					}
				}
				writePage(w, items, version, token)
			})
			result, err := reader.List(context.Background())
			want := "invalid_snapshot"
			if problem == "second-page-error" {
				want = "unavailable"
			}
			assertCode(t, err, want)
			if result.Items != nil {
				t.Fatal("returned partial snapshot")
			}
		})
	}
}

func TestDetailProjectionAndLifecycle(t *testing.T) {
	for _, severity := range []string{"critical", "high", "medium", "low"} {
		for _, phase := range []string{"open", "resolved"} {
			t.Run(severity+"/"+phase, func(t *testing.T) {
				item := issueObject(t, "incident-legacy")
				spec := item["spec"].(map[string]any)
				spec["analysis"].(map[string]any)["severity"] = severity
				spec["lifecycle"].(map[string]any)["phase"] = phase
				reader := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
					if !strings.HasSuffix(r.URL.Path, "/incident-legacy") {
						t.Error("legacy name changed")
					}
					_ = json.NewEncoder(w).Encode(item)
				})
				got, err := reader.Get(context.Background(), "incident-legacy")
				if err != nil || got.Item.Severity != severity || got.Item.Lifecycle.Phase != phase || got.Item.Analysis.Model != "fixture" {
					t.Fatalf("bad detail: %+v %v", got, err)
				}
				data, _ := json.Marshal(got)
				if strings.Contains(string(data), "private-") || strings.Contains(string(data), `"uid"`) {
					t.Fatal("leaked metadata or UIDs")
				}
			})
		}
	}
}

func TestUpstreamFailures(t *testing.T) {
	for _, status := range []int{401, 403, 404, 500} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			reader := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"private-upstream-body","code":`+strconv.Itoa(status)+`}`)
			})
			_, err := reader.List(context.Background())
			want := "unavailable"
			if status == 401 || status == 403 {
				want = "access_denied"
			}
			assertCode(t, err, want)
			if strings.Contains(err.Error(), "private") {
				t.Fatal("leaked upstream error")
			}
			if status == 404 {
				_, err = reader.Get(context.Background(), "issue-missing")
				assertCode(t, err, "not_found")
			}
		})
	}
}

func TestTimeoutAndNameScope(t *testing.T) {
	var calls atomic.Int32
	reader := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-r.Context().Done()
	})
	for _, name := range []string{"", "../secret", "a/b", "UPPER", strings.Repeat("a", 254)} {
		_, err := reader.Get(context.Background(), name)
		assertCode(t, err, "invalid_name")
	}
	if calls.Load() != 0 {
		t.Fatal("invalid name reached API")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := reader.List(ctx)
	assertCode(t, err, "timeout")
	if time.Since(start) > time.Second {
		t.Fatal("ignored caller deadline")
	}
	for _, namespace := range []string{"", "*", "UPPER", "a/b", strings.Repeat("a", 64)} {
		if _, err := NewReader(nil, namespace); err == nil {
			t.Fatalf("accepted namespace %q", namespace)
		}
	}
}

func TestResponseSizeBoundaries(t *testing.T) {
	for _, size := range []int{MaxResponseBytes, MaxResponseBytes + 1} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			body := &boundedBody{ReadCloser: io.NopCloser(strings.NewReader(strings.Repeat("x", size))), remaining: MaxResponseBytes}
			got, err := io.ReadAll(body)
			if size == MaxResponseBytes && (err != nil || len(got) != size) {
				t.Fatalf("rejected exact boundary: %v", err)
			}
			if size > MaxResponseBytes && !errors.Is(err, errResponseSize) {
				t.Fatalf("accepted oversized body: %v", err)
			}
		})
	}
	reader := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, strings.Repeat(" ", MaxResponseBytes+1))
	})
	_, err := reader.List(context.Background())
	assertCode(t, err, "response_too_large")
}

func TestMalformedSchema(t *testing.T) {
	for _, field := range []string{"severity", "phase", "raised_at", "revision", "title", "recommendations", "resources"} {
		t.Run(field, func(t *testing.T) {
			obj := issueObject(t, "issue-bad")
			spec := obj["spec"].(map[string]any)
			switch field {
			case "severity", "title":
				spec["analysis"].(map[string]any)[field] = ""
			case "recommendations":
				delete(spec["analysis"].(map[string]any), field)
			case "phase", "raised_at":
				spec["lifecycle"].(map[string]any)[field] = "invalid"
			case "revision":
				spec["lifecycle"].(map[string]any)[field] = int64(0)
			case "resources":
				spec[field] = []any{}
			}

			reader := &Reader{namespace: "issues"}
			_, err := reader.decode(&unstructured.Unstructured{Object: obj})
			assertCode(t, err, "invalid_snapshot")
		})
	}
}

func TestAggregateSnapshotSizeLimit(t *testing.T) {
	items := make([]map[string]any, 3)
	for i := range items {
		items[i] = issueObject(t, fmt.Sprintf("issue-%d", i))
		items[i]["spec"].(map[string]any)["resources"] = []any{
			map[string]any{"kind": "Deployment", "name": strings.Repeat("x", 3<<20)},
		}
	}
	var calls atomic.Int32
	reader := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
		index := int(calls.Add(1)) - 1
		token := ""
		if index < 2 {
			token = strconv.Itoa(index + 1)
		}
		writePage(w, items[index:index+1], "42", token)
	})
	got, err := reader.List(context.Background())
	assertCode(t, err, "response_too_large")
	if got.Items != nil || calls.Load() != 3 {
		t.Fatal("oversized aggregate must fail without a partial result")
	}
}
