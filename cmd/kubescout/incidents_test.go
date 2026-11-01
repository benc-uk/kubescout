/*
Purpose: Verify local incident persistence and storage failure behavior.
Flow: Offline CLI and Kubernetes HTTP fixtures exercise cluster-scoped writes, no-action,
restart identity and guarded updates from decision fixtures. Missing CRDs or
permissions stop execution without publishing incidents or switching stores.
*/
package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// TestKubeconfigIncidentPersistence verifies cluster-scoped local creates, no-action and restart-safe updates.
func TestKubeconfigIncidentPersistence(t *testing.T) {
	cliEnvironment(t)

	args := []string{"--auth-mode=kubeconfig", "--observer-id=fixture"}

	c, err := config.Parse(args, os.Getenv, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}

	var creates, patches atomic.Int32

	var stored *unstructured.Unstructured

	base := "/apis/kubescout.benc-uk.github.com/v1alpha1/incidents"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.URL.Path != base && !strings.HasPrefix(r.URL.Path, base+"/") {
			t.Error("incident API request escaped the cluster-scoped Incident endpoint")
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		switch r.Method {
		case http.MethodGet:
			if r.URL.Path == base {
				items := []any{}
				if stored != nil {
					items = append(items, stored.Object)
				}

				json.NewEncoder(w).Encode(map[string]any{"apiVersion": incident.APIVersion, "kind": "IncidentList",
					"metadata": map[string]any{"resourceVersion": "2"}, "items": items})

				return
			}

			if stored == nil || r.URL.Path != base+"/"+stored.GetName() {
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "Status", "reason": "NotFound", "code": 404})

				return
			}

			json.NewEncoder(w).Encode(stored.Object)
		case http.MethodPost:
			creates.Add(1)

			stored = &unstructured.Unstructured{}
			if err := json.NewDecoder(r.Body).Decode(&stored.Object); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)

				return
			}

			if stored.GetKind() != "Incident" || stored.GetNamespace() != "" || stored.Object["status"] != nil {
				t.Error("local writes must create only a cluster-scoped Incident spec")
			}

			stored.SetUID("incident-uid")
			stored.SetResourceVersion("1")
			stored.Object["status"] = map[string]any{"triage": map[string]any{"dismissedRevision": int64(1)}}

			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(stored.Object)
		case http.MethodPatch:
			patches.Add(1)

			var patch []map[string]any
			if err := json.NewDecoder(r.Body).Decode(&patch); err != nil || len(patch) != 4 || stored == nil {
				t.Error("invalid incident patch")
				w.WriteHeader(http.StatusBadRequest)

				return
			}

			if r.URL.Path != base+"/"+stored.GetName() || r.Header.Get("Content-Type") != "application/json-patch+json" ||
				patch[0]["op"] != "test" || patch[0]["path"] != "/metadata/resourceVersion" || patch[0]["value"] != stored.GetResourceVersion() ||
				patch[1]["op"] != "test" || patch[1]["path"] != "/metadata/uid" || patch[1]["value"] != string(stored.GetUID()) ||
				patch[2]["op"] != "replace" || patch[2]["path"] != "/spec" ||
				patch[3]["path"] != "/metadata/labels/kubescout.benc-uk.github.com~1state" {
				t.Error("local updates must preserve identity and status with concurrency preconditions")
			}

			stored.Object["spec"] = patch[2]["value"]
			stored.SetResourceVersion("2")
			json.NewEncoder(w).Encode(stored.Object)
		default:
			t.Error("unexpected incident API method")
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}

	reconciler := newIncidentReconciler(c, client)
	resource := health.Resource{Kind: "Pod", Namespace: "apps", Name: "failed", UID: "pod-uid"}
	ref := incident.Ref(resource)
	in := incident.InvestigationContext{Anchor: ref, Resources: []incident.Resource{{Ref: ref, Kind: resource.Kind, Namespace: resource.Namespace, Name: resource.Name}},
		Identities: map[string]health.Resource{ref: resource}, Signals: []incident.Signal{{ID: "evidence"}}}

	decision := incident.Decision{Outcome: "decisions", Reason: "Requires attention", Actions: []incident.Action{{
		Operation: "raise", Family: "container-failure", ResourceRefs: []string{ref}, EvidenceIDs: []string{"evidence"},
		Title: "Container fails", Severity: "medium", Impact: "degraded", Diagnosis: "repeated-exit", Explanation: "Observed repeated failure",
		Recommendations: []string{"Inspect workload configuration"}, Uncertainty: "Root cause not confirmed", MaterialChange: true,
	}}}
	if err := incident.Validate(decision, in, map[string]bool{"evidence": true}); err != nil {
		t.Fatal(err)
	}

	changes, err := reconciler.Apply(t.Context(), in, decision, func() bool { return true })
	if err != nil || len(changes) != 1 {
		t.Fatalf("local raise: changes=%+v error=%v", changes, err)
	}

	incidentName := changes[0].Name

	reconciler = newIncidentReconciler(c, client)

	open, err := reconciler.Repository.ListOpen(t.Context())
	if err != nil || len(open) != 1 || open[0].Name != incidentName {
		t.Fatalf("restart lost the local incident: %v", err)
	}

	in.Incidents, err = reconciler.Existing(t.Context(), resource)
	if err != nil || len(in.Incidents) != 1 {
		t.Fatalf("restart incident lookup: %v", err)
	}

	changes, err = reconciler.Apply(t.Context(), in, incident.Decision{Outcome: "no_action", Reason: "No material change", Actions: []incident.Action{}}, func() bool { return true })
	if err != nil || len(changes) != 0 || creates.Load() != 1 || patches.Load() != 0 {
		t.Fatal("local no-action mutated the stored incident")
	}

	decision.Actions[0].Operation, decision.Actions[0].IncidentID, decision.Actions[0].Severity = "update", incidentName, "high"
	if err := incident.Validate(decision, in, map[string]bool{"evidence": true}); err != nil {
		t.Fatal(err)
	}

	changes, err = reconciler.Apply(t.Context(), in, decision, func() bool { return true })
	if err != nil || len(changes) != 1 || changes[0].Name != incidentName ||
		creates.Load() != 1 || patches.Load() != 1 {
		t.Fatalf("local update did not persist under the same identity: changes=%+v error=%v", changes, err)
	}
}

// TestCLIIncidentIndexFailureStopsExecution checks missing CRD and denied-access failures
// in once and watch without workload access, leaked errors or synthetic incident output.
func TestCLIIncidentIndexFailureStopsExecution(t *testing.T) {
	cliEnvironment(t)

	for _, status := range []int{http.StatusNotFound, http.StatusForbidden} {
		for _, mode := range []string{"once", "watch"} {
			t.Run(http.StatusText(status)+"/"+mode, func(t *testing.T) {
				var incidentReads atomic.Int32

				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")

					if !strings.Contains(r.URL.Path, "/incidents") || r.Method != http.MethodGet {
						t.Error("incident-index failure proceeded to workload access")
						w.WriteHeader(http.StatusBadRequest)

						return
					}

					incidentReads.Add(1)
					w.WriteHeader(status)
					json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "Status", "code": status,
						"reason": strings.ReplaceAll(http.StatusText(status), " ", ""), "message": "private-fixture-error"})
				}))
				defer server.Close()

				client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
				if err != nil {
					t.Fatal(err)
				}

				args := []string{"--ai-log=false", "--auth-mode=kubeconfig", "--observer-id=fixture", "--namespace-watch=apps"}
				if mode != "watch" {
					args = append(args, "--once")
				}

				var out, diag bytes.Buffer

				code := runWithClient(t.Context(), args, &out, &diag, func(config.Config) (dynamic.Interface, string, error) {
					return client, "fixture", nil
				})

				if code != 1 || incidentReads.Load() != 1 || !strings.Contains(diag.String()+out.String(), "check cluster-scoped Incident CRD and RBAC") {
					t.Fatalf("storage failure silently downgraded: code=%d stdout=%s stderr=%s", code, &out, &diag)
				}

				if strings.Contains(out.String()+diag.String(), "private-fixture-error") {
					t.Fatal("incident API error leaked raw details")
				}

				if mode == "watch" {
					if strings.Contains(out.String(), "[GO][INCIDENT]") {
						t.Fatal("watch storage failure emitted an incident")
					}

					return
				}

				if !strings.Contains(diag.String(), "Assessment incomplete: 0 investigations, 0 persisted incident changes") {
					t.Fatalf("storage failure missing once summary: %s", &diag)
				}
			})
		}
	}
}
