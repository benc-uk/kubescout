/*
Purpose: Verify normal alert delivery, dry-run JSON and optional startup replay.
Flow: Offline Kubernetes and webhook fixtures exercise CLI configuration, stdout
isolation and write failures, no-send dry-runs, pagination and watch reconnection.
Seven-field payloads follow the stored Incident schema with safe validation errors.
Replay includes open records only, skipping resolved and malformed lifecycle states.
Fake-time transport fixtures verify five-second retry spacing, limits and cancellation.
No live cluster, external endpoint, model or Kubernetes writes are used.
*/
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

type failingWriter struct{}

// Write simulates a failed stdout sink with private error details that must not be logged.
func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("private fixture writer details")
}

// RoundTrip runs an offline HTTP transport fixture without making network requests.
func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

// TestParseArgs verifies replay and dry-run flags, explicit false, help and invalid arguments.
func TestParseArgs(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want alertOptions
		err  bool
		help bool
	}{
		{name: "default"},
		{name: "include existing", args: []string{"--include-existing"}, want: alertOptions{includeExisting: true}},
		{name: "explicit true", args: []string{"--include-existing=true"}, want: alertOptions{includeExisting: true}},
		{name: "explicit false", args: []string{"--include-existing=false"}},
		{name: "dry run", args: []string{"--dry-run"}, want: alertOptions{dryRun: true}},
		{name: "dry run true", args: []string{"--dry-run=true"}, want: alertOptions{dryRun: true}},
		{name: "dry run false", args: []string{"--dry-run=false"}},
		{name: "dry run replay", args: []string{"--dry-run", "--include-existing"}, want: alertOptions{includeExisting: true, dryRun: true}},
		{name: "invalid boolean", args: []string{"--include-existing=invalid"}, err: true},
		{name: "invalid dry run", args: []string{"--dry-run=invalid"}, err: true},
		{name: "unknown flag", args: []string{"--unknown"}, err: true},
		{name: "positional", args: []string{"existing"}, err: true},
		{name: "trailing positional", args: []string{"--include-existing", "existing"}, err: true},
		{name: "help", args: []string{"--help"}, err: true, help: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer

			got, err := parseArgs(test.args, &output)
			if got != test.want || (err != nil) != test.err || errors.Is(err, flag.ErrHelp) != test.help {
				t.Fatalf("got options %+v, error %v", got, err)
			}

			if test.help && (!strings.Contains(output.String(), "include-existing") || !strings.Contains(output.String(), "dry-run")) {
				t.Fatalf("missing flag help: %s", &output)
			}
		})
	}
}

// TestRunHelp verifies CLI help exits before loading endpoint or Kubernetes credentials.
func TestRunHelp(t *testing.T) {
	var output, stdout bytes.Buffer

	if err := run(context.Background(), []string{"--help"}, &stdout, log.New(&output, "", 0)); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(output.String(), "include-existing") || !strings.Contains(output.String(), "dry-run") || stdout.Len() != 0 {
		t.Fatalf("missing flag help: %s", &output)
	}
}

// TestRunAlertModes exercises CLI endpoint bypass, open-only replay, POSTs and stdout failures.
// Temporary kubeconfig/module files and local HTTP fixtures keep all modes offline.
func TestRunAlertModes(t *testing.T) {
	for _, test := range []struct {
		name         string
		args         []string
		invalidURL   bool
		missingURL   bool
		failStdout   bool
		resolvedOnly bool
		wantErr      string
		wantJSON     []string
		wantPosts    []string
	}{
		{name: "dry run new", args: []string{"--dry-run"}, missingURL: true, wantErr: "history expired", wantJSON: []string{"new"}},
		{name: "dry run replay", args: []string{"--dry-run", "--include-existing"}, missingURL: true,
			wantErr: "history expired", wantJSON: []string{"existing", "new"}},
		{name: "dry run ignores invalid endpoint", args: []string{"--dry-run"}, invalidURL: true,
			wantErr: "history expired", wantJSON: []string{"new"}},
		{name: "normal mode", wantErr: "history expired", wantPosts: []string{"new"}},
		{name: "explicit normal replay", args: []string{"--dry-run=false", "--include-existing"},
			wantErr: "history expired", wantPosts: []string{"existing", "new"}},
		{name: "dry run resolved-only replay", args: []string{"--dry-run", "--include-existing"}, missingURL: true, resolvedOnly: true,
			wantErr: "history expired", wantJSON: []string{"new"}},
		{name: "normal resolved-only replay", args: []string{"--include-existing"}, resolvedOnly: true,
			wantErr: "history expired", wantPosts: []string{"new"}},
		{name: "normal missing endpoint", missingURL: true, wantErr: "set ALERT_ENDPOINT_URL"},
		{name: "normal invalid endpoint", invalidURL: true, wantErr: "set ALERT_ENDPOINT_URL"},
		{name: "dry run stdout failure", args: []string{"--dry-run"}, missingURL: true, failStdout: true,
			wantErr: "cannot write dry-run alert for Incident new to stdout"},
		{name: "dry run replay stdout failure", args: []string{"--dry-run", "--include-existing"}, missingURL: true, failStdout: true,
			wantErr: "cannot write dry-run alert for Incident existing to stdout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var (
				lists  atomic.Int32
				output bytes.Buffer
				stdout bytes.Buffer
			)

			received := make(chan alert, 2)

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/alert" {
					if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
						t.Error("expected a JSON alert POST")
					}

					var payload alert
					if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
						t.Error(err)
					}

					select {
					case received <- payload:
					default:
						t.Error("unexpected extra alert")
					}

					w.WriteHeader(http.StatusAccepted)

					return
				}

				if r.Method != http.MethodGet || r.URL.Path != "/apis/kubescout.benc-uk.github.com/v1alpha1/incidents" {
					t.Error("unexpected Kubernetes request")
					w.WriteHeader(http.StatusForbidden)

					return
				}

				w.Header().Set("Content-Type", "application/json")
				encoder := json.NewEncoder(w)

				if r.URL.Query().Get("watch") != "true" {
					lists.Add(1)

					resolved := fixtureIncident("resolved", "95")
					if err := unstructured.SetNestedField(resolved.Object, "resolved", "spec", "lifecycle", "state"); err != nil {
						t.Error(err)
					}

					items := []unstructured.Unstructured{*resolved}
					if !test.resolvedOnly {
						items = append(items, *fixtureIncident("existing", "90"))
					}

					_ = encoder.Encode(&unstructured.UnstructuredList{
						Object: map[string]any{
							"apiVersion": incident.APIVersion, "kind": "IncidentList",
							"metadata": map[string]any{"resourceVersion": "100"},
						},
						Items: items,
					})

					return
				}

				for _, event := range []watch.Event{
					{Type: watch.Modified, Object: fixtureIncident("existing", "101")},
					{Type: watch.Added, Object: fixtureIncident("new", "102")},
					{Type: watch.Error, Object: &metav1.Status{
						TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
						Status:   "Failure", Code: http.StatusGone, Reason: metav1.StatusReasonExpired,
					}},
				} {
					_ = encoder.Encode(map[string]any{"type": event.Type, "object": event.Object})
				}
			}))
			defer server.Close()

			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module github.com/benc-uk/kubescout\n"), 0o600); err != nil {
				t.Fatal(err)
			}

			t.Chdir(dir)

			kubeconfig := filepath.Join(dir, "config")

			fixture := clientcmdapi.Config{
				CurrentContext: "fixture",
				Clusters:       map[string]*clientcmdapi.Cluster{"fixture": {Server: server.URL}},
				Contexts:       map[string]*clientcmdapi.Context{"fixture": {Cluster: "fixture"}},
			}
			if err := clientcmd.WriteToFile(fixture, kubeconfig); err != nil {
				t.Fatal(err)
			}

			t.Setenv("KUBECONFIG", kubeconfig)

			endpoint := server.URL + "/alert?sig=private-token"
			if test.missingURL {
				endpoint = ""
			} else if test.invalidURL {
				endpoint = "not-an-endpoint?sig=private-token"
			}

			t.Setenv("ALERT_ENDPOINT_URL", endpoint)

			var writer io.Writer = &stdout
			if test.failStdout {
				writer = failingWriter{}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err := run(ctx, test.args, writer, log.New(&output, "", 0))
			if err == nil || !strings.Contains(err.Error(), test.wantErr) ||
				strings.Contains(err.Error(), "private") || strings.Contains(output.String(), "private") {
				t.Fatalf("unsafe or unexpected result: %v, logs: %s", err, &output)
			}

			assertDryRunOutput(t, stdout.String(), test.wantJSON)

			if len(received) != len(test.wantPosts) {
				t.Fatalf("got %d POSTs, want %d", len(received), len(test.wantPosts))
			}

			for _, title := range test.wantPosts {
				payload := <-received

				expected, err := alertFor(fixtureIncident(title, "100"))
				if err != nil || payload != expected || !strings.Contains(output.String(), "Incident "+title+" sent") {
					t.Fatalf("unexpected delivered alert: %+v, error %v, logs: %s", payload, err, &output)
				}
			}

			if len(test.wantJSON) > 0 && strings.Contains(output.String(), " sent") {
				t.Fatalf("dry-run must not report delivery: %s", &output)
			}

			if strings.Contains(test.wantErr, "ALERT_ENDPOINT_URL") && lists.Load() != 0 {
				t.Fatal("invalid delivery configuration must fail before Kubernetes access")
			}
		})
	}
}

// TestAlertFor checks the exact seven-field projection, text joining and malformed field rejection.
func TestAlertFor(t *testing.T) {
	object := fixtureIncident("new", "101")

	payload, err := alertFor(object)
	if err != nil {
		t.Fatal(err)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}

	var fields map[string]string
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatal(err)
	}

	expected := map[string]string{
		"title": "new", "severity": "Critical", "impact": "Degraded",
		"source":          "new-resource",
		"diagnosis":       "computer-thermal-overheating",
		"explanation":     "A \"quoted\" explanation.\nAnother line.",
		"recommendations": "Turn off the computer.\nLet it cool down.",
	}
	if !reflect.DeepEqual(fields, expected) {
		t.Fatalf("unexpected payload: %s", body)
	}

	for _, field := range []string{"title", "severity", "impact", "diagnosis", "explanation", "recommendations"} {
		t.Run("missing-"+field, func(t *testing.T) {
			invalid := object.DeepCopy()
			unstructured.RemoveNestedField(invalid.Object, "spec", "analysis", field)

			if _, err := alertFor(invalid); err == nil || !strings.Contains(err.Error(), "spec.analysis."+field) {
				t.Fatalf("missing field must be identified: %v", err)
			}
		})
	}

	for _, value := range []any{nil, "not-an-array", []any{}, []any{"valid", int64(42)}, []any{""}} {
		invalid := object.DeepCopy()
		if err := unstructured.SetNestedField(invalid.Object, value, "spec", "analysis", "recommendations"); err != nil {
			t.Fatal(err)
		}

		if _, err := alertFor(invalid); err == nil {
			t.Fatalf("invalid recommendations accepted: %v", value)
		}
	}
}

// TestAlertForStoredIncident verifies source uses the typed Incident anchor, not an invented resource field.
func TestAlertForStoredIncident(t *testing.T) {
	stored := incident.Incident{
		Identity: incident.Identity{
			Observer: "fixture", Family: "rollout",
			Anchor: health.Resource{Kind: "Deployment", Namespace: "apps", Name: "api", UID: "private-anchor-uid"},
		},
		Resources: []health.Resource{{Kind: "Pod", Namespace: "apps", Name: "api-pod", UID: "private-pod-uid"}},
		Analysis: incident.Analysis{
			Title: "Deployment unavailable", Severity: "high", Impact: "unavailable",
			Diagnosis: "rollout-failure", Explanation: "No replicas are available.",
			EvidenceIDs: []string{"evidence-1"}, Recommendations: []string{"Check the deployment image."},
			Uncertainty: "No additional evidence.", Model: "fixture", PromptVersion: incident.PromptVersion, AnalysisID: "fixture-analysis",
		},
		Lifecycle: incident.Lifecycle{State: "open", Revision: 1},
	}

	spec, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&stored)
	if err != nil {
		t.Fatal(err)
	}

	object := &unstructured.Unstructured{Object: map[string]any{"spec": spec}}

	payload, err := alertFor(object)
	if err != nil || payload.Source != "api" || payload.Title != stored.Analysis.Title {
		t.Fatalf("stored Incident projection failed: payload %+v, error %v", payload, err)
	}

	for _, value := range []any{nil, "", " ", int64(42)} {
		invalid := object.DeepCopy()
		if err := unstructured.SetNestedField(invalid.Object, value, "spec", "identity", "anchor", "name"); err != nil {
			t.Fatal(err)
		}

		if err := unstructured.SetNestedField(invalid.Object, "must-not-be-used", "spec", "resource", "name"); err != nil {
			t.Fatal(err)
		}

		if _, err := alertFor(invalid); err == nil || err.Error() != "missing or invalid alert field: spec.identity.anchor.name" {
			t.Fatalf("invalid anchor must be rejected with a field-only error: %v", err)
		}
	}
}

// TestWatchAddedOnlyAndReconnect verifies new-only dry-run JSON and cursor resume without POSTs.
// Expired history fails without relisting or replaying records.
func TestWatchAddedOnlyAndReconnect(t *testing.T) {
	var (
		watches atomic.Int32
		posts   atomic.Int32
		output  bytes.Buffer
		stdout  bytes.Buffer
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		encoder := json.NewEncoder(w)

		if r.URL.Path == "/alert" {
			posts.Add(1)
			t.Error("dry-run must not send HTTP alerts")
			w.WriteHeader(http.StatusAccepted)

			return
		}

		if r.Method != http.MethodGet || r.URL.Path != "/apis/kubescout.benc-uk.github.com/v1alpha1/incidents" {
			t.Errorf("unexpected Kubernetes request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusForbidden)

			return
		}

		if r.URL.Query().Get("watch") != "true" {
			if r.URL.Query().Get("limit") != "1" || watches.Load() != 0 {
				t.Error("expected only the initial limited list")
			}

			_ = encoder.Encode(&unstructured.UnstructuredList{
				Object: map[string]any{
					"apiVersion": "kubescout.benc-uk.github.com/v1alpha1", "kind": "IncidentList",
					"metadata": map[string]any{"resourceVersion": "100"},
				},
				Items: []unstructured.Unstructured{*fixtureIncident("existing", "90")},
			})

			return
		}

		if r.URL.Query().Get("allowWatchBookmarks") != "true" || r.URL.Query().Get("sendInitialEvents") == "true" {
			t.Error("watch must allow bookmarks without requesting initial Added events")
		}

		switch watches.Add(1) {
		case 1:
			if r.URL.Query().Get("resourceVersion") != "100" {
				t.Error("watch did not start at the initial list boundary")
			}

			for _, event := range []watch.Event{
				{Type: watch.Modified, Object: fixtureIncident("existing", "101")},
				{Type: watch.Added, Object: fixtureIncident("first-new", "102")},
				{Type: watch.Modified, Object: fixtureIncident("first-new", "103")},
				{Type: watch.Deleted, Object: fixtureIncident("first-new", "104")},
				{Type: watch.Bookmark, Object: fixtureIncident("bookmark", "105")},
			} {
				_ = encoder.Encode(map[string]any{"type": event.Type, "object": event.Object})
			}
		case 2:
			if r.URL.Query().Get("resourceVersion") != "105" {
				t.Error("reconnect did not resume after the last bookmark")
			}

			_ = encoder.Encode(map[string]any{"type": watch.Added, "object": fixtureIncident("second-new", "106")})
			_ = encoder.Encode(map[string]any{
				"type": watch.Error,
				"object": &metav1.Status{
					TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
					Status:   "Failure", Code: http.StatusGone, Reason: metav1.StatusReasonExpired,
				},
			})
		default:
			t.Error("unexpected watch retry after expired history")
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err := watchNew(ctx, fixtureResource(t, server.URL), server.URL+"/alert?sig=fixture-signature", alertOptions{dryRun: true}, &stdout, log.New(&output, "", 0))
	if err == nil || !strings.Contains(err.Error(), "history expired") {
		t.Fatalf("expected expired-history failure, got %v", err)
	}

	assertDryRunOutput(t, stdout.String(), []string{"first-new", "second-new"})

	if posts.Load() != 0 ||
		strings.Contains(output.String(), "sent") ||
		strings.Contains(output.String(), "fixture-signature") ||
		strings.Contains(output.String(), "private fixture response") {
		t.Fatalf("unsafe or missing delivery logs: %s", &output)
	}
}

// TestWatchIncludeExisting verifies open-only paginated replay precedes queued new events without POSTs.
// Resolved/malformed records, empty snapshots, page failures and cancellation use offline fixtures.
func TestWatchIncludeExisting(t *testing.T) {
	for _, test := range []struct {
		name            string
		includeExisting bool
		empty           bool
		failPage        bool
		cancelPage      bool
		wantLists       int32
		wantTitles      []string
	}{
		{name: "default skips pages", wantLists: 1, wantTitles: []string{"new"}},
		{name: "replay open records across pages", includeExisting: true, wantLists: 2, wantTitles: []string{"existing-first", "existing-third", "new"}},
		{name: "empty snapshot", includeExisting: true, empty: true, wantLists: 1, wantTitles: []string{"new"}},
		{name: "continuation failure", includeExisting: true, failPage: true, wantLists: 2, wantTitles: []string{"existing-first"}},
		{name: "continuation canceled", includeExisting: true, cancelPage: true, wantLists: 2, wantTitles: []string{"existing-first"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var (
				lists   atomic.Int32
				watches atomic.Int32
				output  bytes.Buffer
				stdout  bytes.Buffer
			)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/apis/kubescout.benc-uk.github.com/v1alpha1/incidents" {
					t.Error("unexpected request or HTTP delivery")
					w.WriteHeader(http.StatusForbidden)

					return
				}

				w.Header().Set("Content-Type", "application/json")
				encoder := json.NewEncoder(w)

				if r.URL.Query().Get("watch") == "true" {
					if watches.Add(1) != 1 || r.URL.Query().Get("resourceVersion") != "100" ||
						r.URL.Query().Get("sendInitialEvents") == "true" {
						t.Error("watch must start at the original snapshot without replaying initial events")
					}

					for _, event := range []watch.Event{
						{Type: watch.Modified, Object: fixtureIncident("existing-first", "101")},
						{Type: watch.Added, Object: fixtureIncident("new", "102")},
						{Type: watch.Error, Object: &metav1.Status{
							TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
							Status:   "Failure", Code: http.StatusGone, Reason: metav1.StatusReasonExpired,
						}},
					} {
						_ = encoder.Encode(map[string]any{"type": event.Type, "object": event.Object})
					}

					return
				}

				page := lists.Add(1)

				wantLimit := "1"
				if test.includeExisting {
					wantLimit = "100"
				}

				if r.URL.Query().Get("limit") != wantLimit {
					t.Error("unexpected list page limit")
				}

				snapshot := &unstructured.UnstructuredList{
					Object: map[string]any{
						"apiVersion": "kubescout.benc-uk.github.com/v1alpha1", "kind": "IncidentList",
						"metadata": map[string]any{"resourceVersion": "100"},
					},
					Items: []unstructured.Unstructured{},
				}

				switch page {
				case 1:
					if r.URL.Query().Get("continue") != "" {
						t.Error("initial list must not have a continuation")
					}

					if !test.empty {
						snapshot.SetContinue("next-page")
						snapshot.Items = append(snapshot.Items, *fixtureIncident("existing-first", "90"))

						if test.includeExisting {
							invalid := fixtureIncident("invalid", "91")
							unstructured.RemoveNestedField(invalid.Object, "spec", "analysis", "title")
							snapshot.Items = append(snapshot.Items, *invalid)

							for _, field := range []struct {
								name  string
								value any
							}{
								{"missing-state", nil},
								{"invalid-state-type", int64(42)},
								{"unknown-state", "unknown"},
							} {
								malformed := fixtureIncident(field.name, "92")
								if err := unstructured.SetNestedField(malformed.Object, field.value, "spec", "lifecycle", "state"); err != nil {
									t.Error(err)
								}

								snapshot.Items = append(snapshot.Items, *malformed)
							}
						}
					}
				case 2:
					if r.URL.Query().Get("continue") != "next-page" {
						t.Error("missing snapshot continuation")
					}

					if test.failPage {
						w.WriteHeader(http.StatusForbidden)
						_, _ = io.WriteString(w, "private fixture response")

						return
					}

					if test.cancelPage {
						cancel()
						<-r.Context().Done()

						return
					}

					existing := fixtureIncident("existing-second", "95")
					if err := unstructured.SetNestedField(existing.Object, "resolved", "spec", "lifecycle", "state"); err != nil {
						t.Error(err)
					}

					snapshot.Items = append(snapshot.Items, *existing, *fixtureIncident("existing-third", "96"))
				default:
					t.Error("unexpected extra list page")
					w.WriteHeader(http.StatusForbidden)

					return
				}

				_ = encoder.Encode(snapshot)
			}))
			defer server.Close()

			err := watchNew(ctx, fixtureResource(t, server.URL), server.URL+"/alert",
				alertOptions{includeExisting: test.includeExisting, dryRun: true}, &stdout, log.New(&output, "", 0))
			switch {
			case test.cancelPage:
				if err != nil {
					t.Fatalf("cancellation must exit successfully: %v", err)
				}
			case test.failPage:
				if err == nil || !strings.Contains(err.Error(), "cannot list Incidents") ||
					strings.Contains(err.Error(), "private") {
					t.Fatalf("unsafe or missing continuation failure: %v", err)
				}
			default:
				if err == nil || !strings.Contains(err.Error(), "history expired") {
					t.Fatalf("expected expired-history failure, got %v", err)
				}
			}

			assertDryRunOutput(t, stdout.String(), test.wantTitles)

			if lists.Load() != test.wantLists {
				t.Fatalf("unexpected list count: %d", lists.Load())
			}

			if test.includeExisting && !test.empty && !strings.Contains(output.String(), "Incident invalid: missing or invalid alert field: spec.analysis.title") {
				t.Fatalf("malformed existing Incident must be logged explicitly: %s", &output)
			}

			if test.includeExisting && !test.empty {
				for _, name := range []string{"missing-state", "invalid-state-type", "unknown-state"} {
					if !strings.Contains(output.String(), "Incident "+name+": missing or invalid alert field: spec.lifecycle.state") {
						t.Fatalf("invalid lifecycle state must be reported without delivery: %s", &output)
					}
				}
			}
		})
	}
}

// TestWatchCancellation verifies cancellation closes an active watch and exits successfully.
func TestWatchCancellation(t *testing.T) {
	started := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.URL.Query().Get("watch") != "true" {
			_, _ = io.WriteString(w, `{"apiVersion":"v1","kind":"List","metadata":{"resourceVersion":"100"},"items":[]}`)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	resource := fixtureResource(t, server.URL)

	go func() {
		done <- watchNew(ctx, resource, server.URL+"/unused", alertOptions{}, io.Discard, log.New(io.Discard, "", 0))
	}()

	select {
	case <-started:
		cancel()
	case <-ctx.Done():
		t.Fatal("watch did not start")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("watch did not stop")
	}
}

// TestListFailure verifies collection errors stop startup without exposing API response bodies.
func TestListFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, "private fixture response")
	}))
	defer server.Close()

	err := watchNew(context.Background(), fixtureResource(t, server.URL), server.URL+"/unused", alertOptions{}, io.Discard, log.New(io.Discard, "", 0))
	if err == nil || !strings.Contains(err.Error(), "cannot list Incidents") ||
		strings.Contains(err.Error(), "private") {
		t.Fatalf("unexpected list failure: %v", err)
	}
}

// TestPostFailures checks non-2xx, redirects and canceled requests without retries or URL disclosure.
func TestPostFailures(t *testing.T) {
	for _, status := range []int{http.StatusAccepted, http.StatusFound, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", "/must-not-follow")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "private response")
			}))
			defer server.Close()

			client := &http.Client{
				Timeout:       time.Second,
				CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			}

			err := post(context.Background(), client, server.URL+"/?sig=private-token", alert{})
			if (err != nil) != (status >= 300) || calls.Load() != 1 {
				t.Fatalf("status %d: error %v, attempts %d", status, err, calls.Load())
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err = post(ctx, client, server.URL+"/?sig=private-token", alert{})
			if err == nil || strings.Contains(err.Error(), "private") || calls.Load() != 1 {
				t.Fatalf("unsafe cancellation failure: %v", err)
			}
		})
	}
}

// TestPostWithRetry verifies exactly five seconds after failures, at most four attempts and identical bodies.
func TestPostWithRetry(t *testing.T) {
	for _, test := range []struct {
		name     string
		statuses []int
		wantErr  bool
	}{
		{"success", []int{202}, false},
		{"http recovery", []int{500, 202}, false},
		{"transport recovery", []int{0, 202}, false},
		{"last retry recovery", []int{500, 500, 500, 204}, false},
		{"exhausted", []int{503, 503, 503, 503}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var (
					calls  int
					bodies []string
					output bytes.Buffer
				)

				start := time.Now()
				client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					if calls >= len(test.statuses) {
						t.Fatal("too many attempts")
					}

					// Each attempt takes two seconds; the retry delay starts after it fails.
					if elapsed := time.Since(start); elapsed != time.Duration(calls)*7*time.Second {
						t.Fatalf("attempt %d started at %s", calls+1, elapsed)
					}

					body, err := io.ReadAll(request.Body)
					if err != nil {
						t.Fatal(err)
					}

					bodies = append(bodies, string(body))
					status := test.statuses[calls]
					calls++

					time.Sleep(2 * time.Second)

					if status == 0 {
						return nil, errors.New("private transport details")
					}

					return &http.Response{
						StatusCode: status,
						Body:       io.NopCloser(strings.NewReader("private response")),
						Header:     make(http.Header),
					}, nil
				})}

				payload := alert{Title: "new", Recommendations: "First\nSecond"}

				err := postWithRetry(context.Background(), client, "https://fixture.invalid/?sig=private-token",
					payload, "inc-fixture", log.New(&output, "", 0))
				if (err != nil) != test.wantErr || calls != len(test.statuses) {
					t.Fatalf("attempts %d, error %v", calls, err)
				}

				for _, body := range bodies {
					if body != bodies[0] {
						t.Fatal("retry changed the payload")
					}
				}

				if strings.Count(output.String(), "retry ") != calls-1 ||
					strings.Contains(output.String(), "private") {
					t.Fatalf("unsafe or missing retry logs: %s", &output)
				}
			})
		})
	}
}

// TestRetryCancellation checks immediate shutdown during a retry wait without another POST.
func TestRetryCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls int

		client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			return nil, errors.New("fixture failure")
		})}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		start := time.Now()
		done := make(chan error, 1)

		go func() {
			done <- postWithRetry(ctx, client, "https://fixture.invalid", alert{}, "inc-fixture", log.New(io.Discard, "", 0))
		}()

		synctest.Wait()
		cancel()

		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected cancellation result: %v", err)
		}

		if calls != 1 || time.Since(start) != 0 {
			t.Fatalf("cancellation waited or retried: attempts %d, elapsed %s", calls, time.Since(start))
		}

		if err := postWithRetry(ctx, client, "https://fixture.invalid", alert{}, "inc-fixture", log.New(io.Discard, "", 0)); !errors.Is(err, context.Canceled) || calls != 1 {
			t.Fatal("already canceled context must not send")
		}
	})
}

// TestEndpointValidation accepts signed HTTP(S) URLs and rejects malformed destinations without echoing them.
func TestEndpointValidation(t *testing.T) {
	for _, endpoint := range []string{"http://127.0.0.1:8000/invoke", "https://example.test/invoke?a=1&sig=private-token"} {
		if err := validateEndpoint(endpoint); err != nil {
			t.Fatal(err)
		}
	}

	for _, endpoint := range []string{"", "/relative", "https:///missing-host", "ftp://example.test", "https://example.test/#private-token", "https://example.test/%private-token"} {
		if err := validateEndpoint(endpoint); err == nil || strings.Contains(err.Error(), "private-token") {
			t.Fatalf("unsafe or missing endpoint error: %v", err)
		}
	}
}

// TestCurrentKubeconfig verifies current-context selection and rejects missing context without fallback.
func TestCurrentKubeconfig(t *testing.T) {
	var requests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"v1","kind":"List","metadata":{"resourceVersion":"100"},"items":[]}`)
	}))
	defer server.Close()

	path := filepath.Join(t.TempDir(), "kubeconfig")

	fixture := clientcmdapi.Config{
		CurrentContext: "selected",
		Clusters:       map[string]*clientcmdapi.Cluster{"fixture": {Server: server.URL}},
		Contexts:       map[string]*clientcmdapi.Context{"selected": {Cluster: "fixture"}},
	}
	if err := clientcmd.WriteToFile(fixture, path); err != nil {
		t.Fatal(err)
	}

	t.Setenv("KUBECONFIG", path)

	resource, err := incidentClient()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := resource.List(context.Background(), metav1.ListOptions{}); err != nil {
		t.Fatal(err)
	}

	fixture.CurrentContext = ""
	if err := clientcmd.WriteToFile(fixture, path); err != nil {
		t.Fatal(err)
	}

	if _, err := incidentClient(); err == nil || requests.Load() != 1 {
		t.Fatal("missing current context must fail without falling back or making requests")
	}
}

// assertDryRunOutput checks exact two-space JSON formatting, payload fields and record order.
func assertDryRunOutput(t *testing.T, output string, titles []string) {
	t.Helper()

	var expected strings.Builder

	for _, title := range titles {
		payload, err := alertFor(fixtureIncident(title, "100"))
		if err != nil {
			t.Fatal(err)
		}

		body, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			t.Fatal(err)
		}

		expected.Write(body)
		expected.WriteByte('\n')
	}

	if output != expected.String() {
		t.Fatalf("unexpected dry-run output:\n%s\nwant:\n%s", output, &expected)
	}
}

// fixtureIncident creates an offline Incident with a schema-shaped identity anchor and alert analysis.
func fixtureIncident(name, resourceVersion string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kubescout.benc-uk.github.com/v1alpha1", "kind": "Incident",
		"metadata": map[string]any{"name": name, "uid": "fixture-" + name, "resourceVersion": resourceVersion},
		"spec": map[string]any{
			"identity": map[string]any{
				"observer": "fixture", "family": "rollout",
				"anchor": map[string]any{"kind": "Deployment", "namespace": "apps", "name": name + "-resource", "uid": "private-anchor-" + name},
			},
			"lifecycle": map[string]any{"state": "open", "revision": int64(1)},
			"analysis": map[string]any{
				"title": name, "severity": "critical", "impact": "degraded",
				"diagnosis":       "computer-thermal-overheating",
				"explanation":     "A \"quoted\" explanation.\nAnother line.",
				"recommendations": []any{"Turn off the computer.", "Let it cool down."},
			},
		},
	}}
}

// fixtureResource constructs a real dynamic client pointing exclusively at a local HTTP fixture.
func fixtureResource(t *testing.T, address string) dynamic.ResourceInterface {
	t.Helper()

	client, err := dynamic.NewForConfig(&rest.Config{Host: address})
	if err != nil {
		t.Fatal(err)
	}

	return client.Resource(schema.GroupVersionResource{
		Group: "kubescout.benc-uk.github.com", Version: "v1alpha1", Resource: "incidents",
	})
}
