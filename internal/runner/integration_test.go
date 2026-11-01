/*
Purpose: Exercise collection, tool-assisted AI and incident persistence together.
Flow: Offline Kubernetes and model HTTP fixtures drive the real collector,
Responses agent and reconciler through namespace scopes and Pod/Service cases.
Assertions check collection-failure retention, projected tool evidence and
AI-authored ClusterIssue output; no live cluster or model is contacted.
*/
package runner

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/analysis"
	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/kube"
	"github.com/benc-uk/kubescout/internal/output"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// TestMultiNamespaceCollectionFailurePublishesNoIncident uses HTTP fixtures to deny scoped collection.
// It checks that failures skip AI, retain unresolved incidents, and omit private API errors.
func TestMultiNamespaceCollectionFailurePublishesNoIncident(t *testing.T) {
	for _, mode := range []string{"watch", "all", "ignore"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Error("collection attempted a write")
				}

				w.Header().Set("Content-Type", "application/json")

				if r.URL.Path == "/api/v1/namespaces/payments/pods" || r.URL.Path == "/api/v1/pods" {
					w.WriteHeader(http.StatusForbidden)
					fmt.Fprint(w, `{"apiVersion":"v1","kind":"Status","reason":"Forbidden","code":403,"message":"private-fixture-error"}`)

					return
				}

				items := []any{}
				if strings.HasSuffix(r.URL.Path, "/nodes") {
					items = append(items, map[string]any{"apiVersion": "v1", "kind": "Node", "metadata": map[string]any{"name": "node", "uid": "node-id", "resourceVersion": "1"}})
				}

				json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{"resourceVersion": "1"}, "items": items})
			}))
			defer server.Close()

			c := options()
			c.RequestTimeout = time.Second

			c.NamespaceWatch = nil
			if mode == "watch" {
				c.NamespaceWatch = []string{"apps", "payments"}
			} else if mode == "ignore" {
				c.NamespaceIgnore = []string{"system"}
			}

			client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
			if err != nil {
				t.Fatal(err)
			}

			resource := failedPod().Resource
			resource.Namespace = "payments"
			repo := &incident.Memory{Records: map[string]incident.Record{"existing": {Value: incident.Incident{
				Identity:  incident.Identity{Observer: "fixture", Anchor: resource, Family: "container-failure"},
				Resources: []health.Resource{resource}, Lifecycle: incident.Lifecycle{State: "open"}}}}}
			ai := &fakeAI{}

			var out, diag bytes.Buffer

			r := Runner{Config: c, Source: kube.NewSource(client, c), Analyzer: ai, Incidents: incident.Reconciler{Repository: repo},
				Output: output.Writer{Out: &out, Err: &diag}}
			if r.Once(t.Context()) != 1 {
				t.Fatal("partial multi-namespace collection accepted")
			}

			var result output.Result
			if json.Unmarshal(out.Bytes(), &result) != nil || result.Status != "incomplete" || len(result.Changes) != 0 ||
				ai.calls.Load() != 0 || repo.Records["existing"].Value.Lifecycle.State != "open" || strings.Contains(out.String(), "private-fixture-error") {
				t.Fatal("collection outage called AI, leaked an error or changed an incident")
			}
		})
	}
}

// TestCollectorToolAgentAndIncidentAPIEndToEnd checks the offline Pod-to-incident flow across namespace modes.
func TestCollectorToolAgentAndIncidentAPIEndToEnd(t *testing.T) {
	for _, tt := range []struct {
		name  string
		scope config.Config
	}{
		{name: "watch", scope: config.Config{NamespaceWatch: []string{"apps"}}},
		{name: "all"},
		{name: "ignore", scope: config.Config{NamespaceIgnore: []string{"system"}}},
	} {
		t.Run(tt.name, func(t *testing.T) { collectorToolAgentAndIncidentAPI(t, tt.scope, "Pod") })
	}
}

// TestServiceCollectionToolAgentAndIncidentAPI checks offline Service evidence and EndpointSlice tool inspection.
func TestServiceCollectionToolAgentAndIncidentAPI(t *testing.T) {
	collectorToolAgentAndIncidentAPI(t, config.Config{NamespaceWatch: []string{"apps"}}, "Service")
}

// collectorToolAgentAndIncidentAPI drives real collection, tool rounds, and reconciliation against local HTTP fixtures.
// It verifies request counts, projection boundaries, and AI-authored ClusterIssue creation and output.
func collectorToolAgentAndIncidentAPI(t *testing.T, scope config.Config, kind string) {
	resource := health.Resource{Kind: kind, Namespace: "apps", Name: "failed", UID: "pod-id"}
	ref := incident.Ref(resource)

	toolRef, family := ref, "container-failure"
	if kind == "Service" {
		toolRef = incident.Ref(health.Resource{Kind: "EndpointSlice", Namespace: "apps", Name: "failed-a", UID: "slice-id"})
		family = "service-availability"
	}

	var (
		creates, modelCalls, toolReads atomic.Int32
		stored                         map[string]any
	)

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if strings.Contains(r.URL.Path, "/clusterissues") {
			if r.Method == "POST" {
				creates.Add(1)
				json.NewDecoder(r.Body).Decode(&stored)
				w.WriteHeader(201)
				json.NewEncoder(w).Encode(stored)

				return
			}

			if strings.HasSuffix(r.URL.Path, "/clusterissues") {
				fmt.Fprint(w, `{"apiVersion":"health.k8s-ops-agent.io/v1alpha1","kind":"ClusterIssueList","metadata":{"resourceVersion":"1"},"items":[]}`)
				return
			}

			w.WriteHeader(404)
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"Status","reason":"NotFound","code":404}`)

			return
		}

		if r.Method != "GET" {
			t.Error("workload write attempted")
		}

		pod := map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "failed", "namespace": "apps", "uid": "pod-id", "resourceVersion": "1"},
			"status": map[string]any{"phase": "Failed"}}
		slice := map[string]any{"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice", "addressType": "IPv4",
			"metadata": map[string]any{"name": "failed-a", "namespace": "apps", "uid": "slice-id", "resourceVersion": "1",
				"labels":          map[string]any{"kubernetes.io/service-name": "failed"},
				"ownerReferences": []any{map[string]any{"apiVersion": "v1", "kind": "Service", "name": "failed", "uid": "pod-id", "controller": true}}},
			"endpoints": []any{map[string]any{"addresses": []any{"private-address"}, "conditions": map[string]any{"ready": false}}}}

		if kind == "Service" {
			pod["kind"] = "Service"
			pod["spec"] = map[string]any{"selector": map[string]any{"app": "private-selector"}}
			delete(pod, "status")
		}

		if kind == "Service" && strings.HasSuffix(r.URL.Path, "/endpointslices/failed-a") {
			toolReads.Add(1)
			json.NewEncoder(w).Encode(slice)

			return
		}

		if strings.HasSuffix(r.URL.Path, "/pods/failed") {
			toolReads.Add(1)
			json.NewEncoder(w).Encode(pod)

			return
		}

		items := []any{}
		if (kind == "Pod" && strings.HasSuffix(r.URL.Path, "/pods")) || (kind == "Service" && strings.HasSuffix(r.URL.Path, "/services")) {
			items = append(items, pod)
		}

		if kind == "Service" && strings.HasSuffix(r.URL.Path, "/endpointslices") {
			items = append(items, slice)
		}

		if strings.HasSuffix(r.URL.Path, "/nodes") {
			items = append(items, map[string]any{"apiVersion": "v1", "kind": "Node", "metadata": map[string]any{"name": "node", "uid": "node-id", "resourceVersion": "1"},
				"status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}})
		}

		json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{"resourceVersion": "1"}, "items": items})
	}))
	defer kubeServer.Close()

	client, err := dynamic.NewForConfig(&rest.Config{Host: kubeServer.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}

	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := modelCalls.Add(1)

		w.Header().Set("Content-Type", "application/json")

		var body map[string]json.RawMessage
		json.NewDecoder(r.Body).Decode(&body)

		if string(body["store"]) != "false" {
			t.Error("provider stored context")
		}

		if strings.Contains(string(body["input"]), "private-address") || strings.Contains(string(body["input"]), "private-selector") {
			t.Error("private Service/EndpointSlice fields reached the model")
		}

		output := []any{}
		if n == 1 {
			output = append(output, map[string]any{"type": "function_call", "id": "fc", "call_id": "tool-call", "name": "inspect_resource", "arguments": fmt.Sprintf(`{"resource_ref":%q}`, toolRef)})
		} else {
			decision := incident.Decision{Outcome: "decisions", Reason: "The inspected container workload failed", Actions: []incident.Action{
				{Operation: "raise", Family: family, ResourceRefs: []string{ref}, Title: "Workload failed", Severity: "high", Impact: "unavailable", Diagnosis: "failed-pod",
					Explanation: "Current inspection confirms Failed phase", EvidenceIDs: []string{"tool-1"}, Recommendations: []string{"Inspect the workload configuration"}, Uncertainty: "The cause is not yet known", MaterialChange: true}}}
			if kind == "Service" {
				decision.Actions[0].Diagnosis = "no-ready-backends"
				decision.Actions[0].Explanation = "No ready published backends; application traffic has not been tested"
				decision.Actions[0].Impact = "degraded"
			}

			text, _ := json.Marshal(decision)
			output = append(output, map[string]any{"type": "message", "id": "m", "role": "assistant", "status": "completed",
				"content": []any{map[string]any{"type": "output_text", "text": string(text), "annotations": []any{}}}})
		}

		json.NewEncoder(w).Encode(map[string]any{"id": "response", "status": "completed", "output": output})
	}))
	defer model.Close()

	cfg := config.Config{Once: true, Auth: "in-cluster", ObserverID: "fixture", NamespaceWatch: []string{"apps"}, MaxObjects: 100, MaxInputChars: 20000, MaxOutputTokens: 2000,
		CallsPerHour: 60, RequestTimeout: time.Second, RunTimeout: 5 * time.Second, InvestigationTimeout: 3 * time.Second, Endpoint: model.URL + "/openai/v1/", APIKey: "fixture", Model: "fixture", Policy: health.DefaultPolicy()}
	cfg.NamespaceWatch, cfg.NamespaceIgnore = scope.NamespaceWatch, scope.NamespaceIgnore

	var out, diag bytes.Buffer

	r := Runner{Config: cfg, Source: kube.NewSource(client, cfg), Analyzer: &analysis.Analyzer{Config: cfg, Kubernetes: client},
		Incidents: incident.Reconciler{Repository: incident.Kubernetes{Client: client, Namespace: "incidents", Observer: "fixture", Limit: 100, Timeout: time.Second}, Observer: "fixture", Model: "fixture"},
		Output:    output.Writer{Out: &out, Err: &diag}}
	if code := r.Once(t.Context()); code != 0 {
		t.Fatalf("code=%d out=%s diag=%s", code, &out, &diag)
	}

	var result output.Result
	if json.Unmarshal(out.Bytes(), &result) != nil || len(result.Changes) != 1 || creates.Load() != 1 || modelCalls.Load() != 2 || toolReads.Load() != 1 {
		t.Fatalf("wrong end-to-end output: %s", &out)
	}

	if stored["kind"] != "ClusterIssue" {
		t.Fatal("legacy HealthReport was published")
	}

	name := stored["metadata"].(map[string]any)["name"]
	if name != incident.Name(incident.Identity{Observer: "fixture", Anchor: resource, Family: family}) ||
		result.Changes[0].Name != name || !strings.HasPrefix(result.Changes[0].Name, "issue-") {
		t.Fatal("ClusterIssue create and report names do not match the issue prefix")
	}

	analysisPayload := stored["spec"].(map[string]any)["analysis"].(map[string]any)
	if analysisPayload["title"] != "Workload failed" || analysisPayload["severity"] != "high" {
		t.Fatal("deterministic evidence replaced AI judgment")
	}
}
