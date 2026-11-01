/*
Purpose: Test bounded issue projection and revision-scoped dashboard triage.
Flow: Offline HTTP and object fixtures check complete pagination, scope,
schema and size failures, plus human dismiss/restore status patches with
identity/version preconditions that preserve incident specs and foreign status.
*/
package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

// issueObject builds a valid stored incident with private fixture fields to test projection boundaries.
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
		Lifecycle: incident.Lifecycle{State: "open", Revision: 1, RaisedAt: now, UpdatedAt: now},
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

// TestServiceAvailabilityFamily checks Service issue decoding and the corresponding embedded family filter.
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

// fixtureReader creates a bounded reader backed by a local HTTP fixture that checks read-only issue scope.
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

// writePage emits a ClusterIssue list fixture with controlled pagination metadata.
func writePage(w http.ResponseWriter, items []map[string]any, version, token string) {
	_ = json.NewEncoder(w).Encode(map[string]any{
		"apiVersion": incident.APIVersion, "kind": "ClusterIssueList",
		"metadata": map[string]any{"resourceVersion": version, "continue": token}, "items": items,
	})
}

// assertCode fails the test unless an error carries the expected dashboard API classification.
func assertCode(t *testing.T, err error, code string) {
	t.Helper()

	var failure *APIError
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

// TestListPaginationBoundaries verifies complete unfiltered reads through the issue-count limit.
// Overflow must fail without returning a partial snapshot.
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
					t.Error("must page all observers and states")
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

// TestPaginationFailures checks that inconsistent pages, invalid records, and later failures discard the snapshot.
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

// TestDetailProjectionAndLifecycle checks supported severity/state combinations and legacy incident names.
// Projected details must omit private metadata and UIDs.
func TestDetailProjectionAndLifecycle(t *testing.T) {
	for _, severity := range []string{"critical", "high", "medium", "low"} {
		for _, state := range []string{"open", "resolved"} {
			t.Run(severity+"/"+state, func(t *testing.T) {
				item := issueObject(t, "incident-legacy")
				spec := item["spec"].(map[string]any)
				spec["analysis"].(map[string]any)["severity"] = severity
				spec["lifecycle"].(map[string]any)["state"] = state
				reader := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
					if !strings.HasSuffix(r.URL.Path, "/incident-legacy") {
						t.Error("legacy name changed")
					}

					_ = json.NewEncoder(w).Encode(item)
				})

				got, err := reader.Get(context.Background(), "incident-legacy")
				if err != nil || got.Item.Severity != severity || got.Item.Lifecycle.State != state || got.Item.Analysis.Model != "fixture" {
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

// TestUpstreamFailures checks sanitized error classification for rejected or unavailable Kubernetes reads.
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

// TestTimeoutAndNameScope verifies invalid names never reach Kubernetes and caller deadlines stop reads.
// It also rejects invalid storage namespaces when constructing readers.
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

// TestResponseSizeBoundaries checks exact-limit acceptance and oversized-body rejection through the reader.
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

// TestMalformedSchema rejects invalid analysis, lifecycle, and resource fields, including legacy phase-only records.
func TestMalformedSchema(t *testing.T) {
	for _, field := range []string{"severity", "state", "legacy-phase", "raised_at", "revision", "title", "recommendations", "resources"} {
		t.Run(field, func(t *testing.T) {
			obj := issueObject(t, "issue-bad")
			spec := obj["spec"].(map[string]any)

			switch field {
			case "severity", "title":
				spec["analysis"].(map[string]any)[field] = ""
			case "recommendations":
				delete(spec["analysis"].(map[string]any), field)
			case "state", "raised_at":
				spec["lifecycle"].(map[string]any)[field] = "invalid"
			case "legacy-phase":
				lifecycle := spec["lifecycle"].(map[string]any)
				lifecycle["phase"] = lifecycle["state"]
				delete(lifecycle, "state")
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

// TestAggregateSnapshotSizeLimit rejects an oversized combined snapshot even when individual pages fit.
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

// TestTriagePersistenceAndRevision checks durable, preconditioned status-only dismiss/restore patches.
// It verifies stale/no-op suppression, preservation of specs and foreign status, and visibility after revision changes.
func TestTriagePersistenceAndRevision(t *testing.T) {
	for _, withStatus := range []bool{false, true} {
		t.Run(fmt.Sprint(withStatus), func(t *testing.T) {
			obj := issueObject(t, "incident-legacy")
			if !withStatus {
				delete(obj, "status")
			}

			specBefore, _ := json.Marshal(obj["spec"])
			patches := 0

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")

				path := "/apis/health.k8s-ops-agent.io/v1alpha1/namespaces/issues/clusterissues/incident-legacy"
				if r.Method == "GET" && r.URL.Path == path {
					_ = json.NewEncoder(w).Encode(obj)
					return
				}

				if r.Method != "PATCH" || r.URL.Path != path+"/status" || r.Header.Get("Content-Type") != "application/json-patch+json" {
					t.Errorf("unexpected write: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(500)

					return
				}

				patches++

				var patch []struct {
					Op, Path string
					Value    json.RawMessage
				}
				if json.NewDecoder(r.Body).Decode(&patch) != nil || len(patch) < 3 || len(patch) > 4 {
					t.Error("invalid JSON patch")
					w.WriteHeader(500)

					return
				}

				meta := obj["metadata"].(map[string]any)

				for i, field := range []string{"uid", "resourceVersion"} {
					var value string

					_ = json.Unmarshal(patch[i].Value, &value)
					if patch[i].Op != "test" || patch[i].Path != "/metadata/"+field || value != meta[field] {
						t.Error("missing identity/version precondition")
					}
				}

				if len(patch) == 4 {
					if patch[2].Op != "add" || patch[2].Path != "/status" || obj["status"] != nil || string(patch[2].Value) != "{}" {
						t.Error("must not replace existing foreign status")
					}

					obj["status"] = map[string]any{}
				}

				last := patch[len(patch)-1]

				var triage map[string]any
				if last.Op != "add" || last.Path != "/status/triage" || json.Unmarshal(last.Value, &triage) != nil || len(triage) != 1 {
					t.Error("write exceeded triage scope")
				}

				obj["status"].(map[string]any)["triage"] = triage
				meta["resourceVersion"] = strconv.Itoa(42 + patches)
				_ = json.NewEncoder(w).Encode(obj)
			}))
			defer server.Close()

			client, err := newClient(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}

			reader, _ := NewReader(client, "issues")

			before, err := reader.Get(t.Context(), "incident-legacy")
			if err != nil || before.Item.Dismissed {
				t.Fatalf("unexpected initial triage: %v", err)
			}

			dismissed, err := reader.SetDismissed(t.Context(), "incident-legacy", before.Item.Version, true)
			if err != nil || !dismissed.Item.Dismissed || dismissed.Item.Lifecycle.Revision != before.Item.Lifecycle.Revision {
				t.Fatalf("dismiss changed lifecycle or failed: %v", err)
			}

			restarted, _ := NewReader(client, "issues")

			persisted, err := restarted.Get(t.Context(), "incident-legacy")
			if err != nil || !persisted.Item.Dismissed {
				t.Fatalf("dismissal not durable: %v", err)
			}

			_, err = reader.SetDismissed(t.Context(), "incident-legacy", persisted.Item.Version, true)
			if err != nil || patches != 1 {
				t.Fatalf("unchanged dismissal must not write: %v", err)
			}

			_, err = reader.SetDismissed(t.Context(), "incident-legacy", before.Item.Version, false)
			assertCode(t, err, "triage_conflict")

			if patches != 1 {
				t.Fatal("stale browser reached write")
			}

			restored, err := reader.SetDismissed(t.Context(), "incident-legacy", persisted.Item.Version, false)
			if err != nil || restored.Item.Dismissed || patches != 2 {
				t.Fatalf("restore failed: %v", err)
			}

			_, err = reader.SetDismissed(t.Context(), "incident-legacy", restored.Item.Version, true)
			if err != nil {
				t.Fatal(err)
			}

			specAfter, _ := json.Marshal(obj["spec"])
			if !bytes.Equal(specBefore, specAfter) || withStatus && obj["status"].(map[string]any)["private"] != "private-status" {
				t.Fatal("triage modified agent spec or foreign status")
			}
			// A material AI update or recurrence advances the agent revision; status is left intact.
			lifecycle := obj["spec"].(map[string]any)["lifecycle"].(map[string]any)
			lifecycle["revision"] = int64(2)
			lifecycle["recurrences"] = int64(1)
			obj["metadata"].(map[string]any)["resourceVersion"] = "99"

			updated, err := reader.Get(t.Context(), "incident-legacy")
			if err != nil || updated.Item.Dismissed || patches != 3 {
				t.Fatalf("new revision must reappear without a status write: %v", err)
			}
		})
	}
}

// TestTriageFailures checks stale identity, patch races, invalid input, and sanitized uncertain-write errors.
// Request counts ensure rejected actions and failed writes do not cause unintended retries.
func TestTriageFailures(t *testing.T) {
	for _, failure := range []string{"changed", "recreated", "race", "patch-test", "forbidden", "missing", "unknown", "invalid-name", "invalid-version"} {
		t.Run(failure, func(t *testing.T) {
			obj := issueObject(t, "issue-one")
			reader := &Reader{namespace: "issues"}

			seen, err := reader.decode(&unstructured.Unstructured{Object: obj})
			if err != nil {
				t.Fatal(err)
			}

			meta := obj["metadata"].(map[string]any)
			if failure == "changed" {
				meta["resourceVersion"] = "43"
			}

			if failure == "recreated" {
				meta["uid"] = "replacement-uid"
			}

			gets, patches := 0, 0

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")

				if r.Method == "GET" {
					gets++
					_ = json.NewEncoder(w).Encode(obj)

					return
				}

				patches++
				status, reason := 503, "ServiceUnavailable"

				switch failure {
				case "race":
					status, reason = 409, "Conflict"
				case "patch-test":
					status, reason = 422, "Invalid"
				case "forbidden":
					status, reason = 403, "Forbidden"
				case "missing":
					status, reason = 404, "NotFound"
				}

				w.WriteHeader(status)
				_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": status, "reason": reason, "message": "private-upstream-body"})
			}))
			defer server.Close()

			client, err := newClient(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}

			reader, _ = NewReader(client, "issues")
			name, version := "issue-one", seen.Version
			want, expectedGets, expectedPatches := "triage_conflict", 1, 1

			switch failure {
			case "changed", "recreated":
				expectedPatches = 0
			case "forbidden":
				want = "triage_denied"
			case "missing":
				want = "not_found"
			case "unknown":
				want = "triage_uncertain"
			case "invalid-name":
				name, want, expectedGets, expectedPatches = "../other", "invalid_name", 0, 0
			case "invalid-version":
				version, want, expectedGets, expectedPatches = "", "invalid_version", 0, 0
			}

			_, err = reader.SetDismissed(t.Context(), name, version, true)
			assertCode(t, err, want)

			if strings.Contains(err.Error(), "private") || gets != expectedGets || patches != expectedPatches {
				t.Fatalf("unsafe failure/retry: gets=%d patches=%d err=%v", gets, patches, err)
			}
		})
	}
}

// TestTriageProjection checks revision-scoped dismissal and malformed triage rejection without spec mutation.
func TestTriageProjection(t *testing.T) {
	for _, revision := range []int64{0, 1, 2} {
		obj := issueObject(t, "issue-one")
		obj["status"] = map[string]any{"triage": map[string]any{"dismissedRevision": revision}}
		reader := &Reader{namespace: "issues"}

		got, err := reader.decode(&unstructured.Unstructured{Object: obj})
		if err != nil || got.Dismissed != (revision == 1) {
			t.Fatalf("incorrect revision-scoped dismissal: %v", err)
		}
	}

	obj := issueObject(t, "issue-one")
	before := (&unstructured.Unstructured{Object: obj}).DeepCopy()
	obj["status"] = map[string]any{"triage": map[string]any{"dismissedRevision": "invalid"}}
	reader := &Reader{namespace: "issues"}
	_, err := reader.decode(&unstructured.Unstructured{Object: obj})
	assertCode(t, err, "invalid_snapshot")

	if !reflect.DeepEqual(before.Object["spec"], obj["spec"]) {
		t.Fatal("projection mutated spec")
	}
}
