/*
Purpose: Exercise collection, tool-assisted AI and incident persistence together.
Flow: Offline Kubernetes and model HTTP fixtures drive the real collector,
Responses agent and reconciler through workload namespace scopes, cluster-scoped Incidents and workload/storage/attachment cases.
Assertions check collection-failure retention, projected evidence with named citations and
AI-authored stored Incidents and readable activity; no live cluster or model is contacted.
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
				Output: &output.Writer{Out: &out, Err: &diag}}
			if r.Once(t.Context()) != 1 {
				t.Fatal("partial multi-namespace collection accepted")
			}

			if !strings.Contains(diag.String(), "Assessment incomplete") ||
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
		t.Run(tt.name, func(t *testing.T) { collectorToolAgentAndIncidentAPI(t, tt.scope, "Pod", false) })
	}
}

// TestServiceCollectionToolAgentAndIncidentAPI checks offline Service evidence and EndpointSlice tool inspection.
func TestServiceCollectionToolAgentAndIncidentAPI(t *testing.T) {
	collectorToolAgentAndIncidentAPI(t, config.Config{NamespaceWatch: []string{"apps"}}, "Service", false)
}

// TestStorageCollectionToolAgentAndIncidentAPI verifies actual SDK storage inspection, validated writes and AI no-action offline.
func TestStorageCollectionToolAgentAndIncidentAPI(t *testing.T) {
	for _, kind := range []string{"PersistentVolumeClaim", "PersistentVolume"} {
		for _, noAction := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/no-action=%t", kind, noAction), func(t *testing.T) {
				collectorToolAgentAndIncidentAPI(t, config.Config{NamespaceWatch: []string{"apps"}}, kind, noAction)
			})
		}
	}
}

// collectorToolAgentAndIncidentAPI drives real collection, tool rounds, and reconciliation against local HTTP fixtures.
// It verifies request counts, projection boundaries and named citations in AI-authored Incidents.
func collectorToolAgentAndIncidentAPI(t *testing.T, scope config.Config, kind string, noAction bool) {
	resource := health.Resource{Kind: kind, Namespace: "apps", Name: "failed", UID: "pod-id"}
	if health.ClusterScoped(kind) {
		resource.Namespace = ""
	}

	ref := incident.Ref(resource)

	toolRef, family := ref, "container-failure"
	if kind == "Service" {
		toolRef = incident.Ref(health.Resource{Kind: "EndpointSlice", Namespace: "apps", Name: "failed-a", UID: "slice-id"})
		family = "service-availability"
	} else if kind == "PersistentVolumeClaim" || kind == "PersistentVolume" || kind == "VolumeAttachment" {
		family = "storage-availability"
	} else if kind == "CronJob" {
		family = "job-failure"
	} else if kind == "HorizontalPodAutoscaler" {
		family = "autoscaling"
	} else if kind == "Ingress" {
		family = "service-availability"
	}

	var (
		creates, modelCalls, toolReads atomic.Int32
		stored                         map[string]any
	)

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if strings.Contains(r.URL.Path, "/incidents") {
			if !strings.HasPrefix(r.URL.Path, "/apis/kubescout.benc-uk.github.com/v1alpha1/incidents") {
				t.Error("incident request must be cluster-scoped")
			}

			if r.Method == "POST" {
				creates.Add(1)
				json.NewDecoder(r.Body).Decode(&stored)

				if _, exists := stored["metadata"].(map[string]any)["namespace"]; exists {
					t.Error("incident create must omit metadata.namespace")
				}

				w.WriteHeader(201)
				json.NewEncoder(w).Encode(stored)

				return
			}

			if strings.HasSuffix(r.URL.Path, "/incidents") {
				fmt.Fprint(w, `{"apiVersion":"kubescout.benc-uk.github.com/v1alpha1","kind":"IncidentList","metadata":{"resourceVersion":"1"},"items":[]}`)
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

		if kind == "Ingress" {
			pod["apiVersion"], pod["kind"] = "networking.k8s.io/v1", kind
			pod["spec"] = map[string]any{
				"defaultBackend": map[string]any{"service": map[string]any{"name": "backend", "port": map[string]any{"number": int64(81)}}},
				"tls":            []any{map[string]any{"secretName": "private-ingress-secret"}},
			}
			pod["metadata"].(map[string]any)["annotations"] = map[string]any{"private": "private-ingress-annotation"}
			pod["status"] = map[string]any{"loadBalancer": map[string]any{"ingress": []any{map[string]any{"hostname": "private-ingress-address"}}}}
		} else if kind == "Service" {
			pod["kind"] = "Service"
			pod["spec"] = map[string]any{"selector": map[string]any{"app": "private-selector"}}
			delete(pod, "status")
		} else if kind == "VolumeAttachment" {
			pod["apiVersion"], pod["kind"] = "storage.k8s.io/v1", kind
			pod["metadata"].(map[string]any)["namespace"] = ""
			pod["spec"] = map[string]any{"attacher": "disk.csi.example", "nodeName": "node", "source": map[string]any{"persistentVolumeName": "disk"}}
			pod["status"] = map[string]any{"attached": false,
				"attachError":        map[string]any{"errorCode": int64(9), "message": "password=fixture"},
				"attachmentMetadata": map[string]any{"devicePath": "private-attachment-path", "volumeHandle": "private-attachment-handle"}}
		} else if family == "storage-availability" {
			pod["kind"] = kind
			pod["metadata"].(map[string]any)["namespace"] = resource.Namespace
			pod["spec"] = map[string]any{
				"storageClassName": "standard", "capacity": map[string]any{"storage": "1Gi"},
				"resources": map[string]any{"requests": map[string]any{"storage": "1Gi"}},
				"csi":       map[string]any{"volumeHandle": "private-storage-handle", "volumeAttributes": map[string]any{"path": "private-storage-path"}},
			}

			if kind == "PersistentVolumeClaim" {
				pod["status"].(map[string]any)["phase"] = "Lost"
			}
		} else if kind == "HorizontalPodAutoscaler" {
			pod["apiVersion"], pod["kind"] = "autoscaling/v2", kind
			pod["spec"] = map[string]any{"scaleTargetRef": map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "target"},
				"maxReplicas": int64(5), "metrics": []any{map[string]any{"type": "External", "external": map[string]any{
					"metric": map[string]any{"name": "request_rate", "selector": map[string]any{"matchLabels": map[string]any{"private": "private-hpa-selector"}}},
					"target": map[string]any{"type": "Value", "value": "100"},
				}}}}
			pod["status"] = map[string]any{"currentReplicas": int64(2), "desiredReplicas": int64(3),
				"conditions": []any{map[string]any{"type": "ScalingActive", "status": "False", "reason": "FailedGetExternalMetric"}}}
		} else if kind == "CronJob" {
			pod["kind"] = "CronJob"
			pod["spec"] = map[string]any{"schedule": "@daily", "suspend": false,
				"jobTemplate": map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
					"containers": []any{map[string]any{"name": "worker", "image": "worker:v1", "env": []any{map[string]any{"name": "PASSWORD", "value": "private-cron-env"}}}},
				}}}}}
			pod["status"] = map[string]any{"lastScheduleTime": "2026-11-01T00:00:00Z", "lastSuccessfulTime": "2026-10-31T00:01:00Z",
				"active": []any{map[string]any{"name": "private-cron-active", "uid": "private-cron-uid"}}}
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

		if kind == "CronJob" && strings.HasSuffix(r.URL.Path, "/cronjobs/failed") {
			toolReads.Add(1)
			json.NewEncoder(w).Encode(pod)

			return
		}

		if kind == "HorizontalPodAutoscaler" && strings.HasSuffix(r.URL.Path, "/horizontalpodautoscalers/failed") {
			toolReads.Add(1)
			json.NewEncoder(w).Encode(pod)

			return
		}

		if kind == "Ingress" && strings.HasSuffix(r.URL.Path, "/ingresses/failed") {
			toolReads.Add(1)
			json.NewEncoder(w).Encode(pod)

			return
		}

		storageResource := "persistentvolumeclaims"
		if kind == "PersistentVolume" {
			storageResource = "persistentvolumes"
		} else if kind == "VolumeAttachment" {
			storageResource = "volumeattachments"
		}

		if family == "storage-availability" && strings.HasSuffix(r.URL.Path, "/"+storageResource+"/failed") {
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

		if family == "storage-availability" && strings.HasSuffix(r.URL.Path, "/"+storageResource) {
			items = append(items, pod)
		}

		if kind == "VolumeAttachment" && strings.HasSuffix(r.URL.Path, "/persistentvolumes") {
			items = append(items, map[string]any{"apiVersion": "v1", "kind": "PersistentVolume",
				"metadata": map[string]any{"name": "disk", "uid": "attachment-pv-uid", "resourceVersion": "1"},
				"spec":     map[string]any{"capacity": map[string]any{"storage": "1Gi"}}, "status": map[string]any{"phase": "Bound"}})
		}

		if kind == "CronJob" && strings.HasSuffix(r.URL.Path, "/cronjobs") {
			items = append(items, pod)
		}

		if kind == "CronJob" && strings.HasSuffix(r.URL.Path, "/jobs") {
			items = append(items, map[string]any{"apiVersion": "batch/v1", "kind": "Job",
				"metadata": map[string]any{"name": "failed-run", "namespace": "apps", "uid": "job-uid", "resourceVersion": "1",
					"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "CronJob", "name": "failed", "uid": "pod-id", "controller": true}}},
				"status": map[string]any{"conditions": []any{map[string]any{"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded"}}}})
		}

		if kind == "HorizontalPodAutoscaler" && strings.HasSuffix(r.URL.Path, "/horizontalpodautoscalers") {
			items = append(items, pod)
		}

		if kind == "HorizontalPodAutoscaler" && strings.HasSuffix(r.URL.Path, "/deployments") {
			items = append(items, map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
				"metadata": map[string]any{"name": "target", "namespace": "apps", "uid": "target-uid", "resourceVersion": "1"},
				"spec":     map[string]any{"replicas": int64(1)}, "status": map[string]any{"availableReplicas": int64(1), "updatedReplicas": int64(1)}})
		}

		if kind == "Ingress" && strings.HasSuffix(r.URL.Path, "/ingresses") {
			items = append(items, pod)
		}

		if kind == "Ingress" && strings.HasSuffix(r.URL.Path, "/services") {
			items = append(items, map[string]any{"apiVersion": "v1", "kind": "Service",
				"metadata": map[string]any{"name": "backend", "namespace": "apps", "uid": "backend-uid", "resourceVersion": "1"},
				"spec":     map[string]any{"ports": []any{map[string]any{"port": int64(80), "targetPort": int64(81)}}}})
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

		if strings.Contains(string(body["input"]), "private-address") || strings.Contains(string(body["input"]), "private-selector") ||
			strings.Contains(string(body["input"]), "private-storage") || strings.Contains(string(body["input"]), "private-cron") ||
			strings.Contains(string(body["input"]), "private-hpa") || strings.Contains(string(body["input"]), "target-uid") ||
			strings.Contains(string(body["input"]), "private-ingress") || strings.Contains(string(body["input"]), "backend-uid") ||
			strings.Contains(string(body["input"]), "private-attachment") || strings.Contains(string(body["input"]), "attachment-pv-uid") ||
			strings.Contains(string(body["input"]), "password=fixture") {
			t.Error("private workload fields reached the model")
		}

		if kind == "CronJob" && n == 2 && (!strings.Contains(string(body["input"]), "status.lastSuccessfulTime") ||
			!strings.Contains(string(body["input"]), "worker:v1")) {
			t.Error("CronJob scheduling or nested template facts missing from actual SDK tool output")
		}

		if kind == "HorizontalPodAutoscaler" && n == 2 && (!strings.Contains(string(body["input"]), "scale_target_ref") ||
			!strings.Contains(string(body["input"]), "max_replicas") || !strings.Contains(string(body["input"]), "request_rate")) {
			t.Error("HPA approved scaling facts or target handle missing from actual SDK tool output")
		}

		if kind == "Ingress" && n == 2 && (!strings.Contains(string(body["input"]), "service_ref") ||
			!strings.Contains(string(body["input"]), "port_number") || !strings.Contains(string(body["input"]), "default_backend")) {
			t.Error("Ingress routing facts or supplied backend handle missing from actual SDK tool output")
		}

		if kind == "VolumeAttachment" && n == 2 && (!strings.Contains(string(body["input"]), "volume_ref") ||
			!strings.Contains(string(body["input"]), "node_ref") || !strings.Contains(string(body["input"]), "attachError")) {
			t.Error("approved attachment facts or supplied PV/Node handles missing from actual SDK tool output")
		}

		output := []any{}
		if n == 1 {
			output = append(output, map[string]any{"type": "function_call", "id": "fc", "call_id": "tool-call", "name": "inspect_resource", "arguments": fmt.Sprintf(`{"resource_ref":%q}`, toolRef)})
		} else {
			decision := incident.Decision{Outcome: "decisions", Reason: "The inspected container workload failed", Actions: []incident.Action{
				{Operation: "raise", Family: family, ResourceRefs: []string{ref}, Title: "Workload failed", Severity: "high", Impact: "unavailable", Diagnosis: "failed-pod",
					Explanation: "Current inspection confirms Failed phase", EvidenceIDs: []string{"inspect-resource-1"}, Recommendations: []string{"Inspect the workload configuration"}, Uncertainty: "The cause is not yet known", MaterialChange: true}}}
			if kind == "Service" {
				decision.Actions[0].Diagnosis = "no-ready-backends"
				decision.Actions[0].Explanation = "No ready published backends; application traffic has not been tested"
				decision.Actions[0].Impact = "degraded"
			} else if family == "storage-availability" {
				decision.Actions[0].Diagnosis = "storage-reported-failure"
				decision.Actions[0].Explanation = "Storage reports failure; application impact requires further investigation"
				decision.Actions[0].Impact = "degraded"
			} else if kind == "CronJob" {
				decision.Actions[0].Diagnosis = "scheduled-job-failure"
				decision.Actions[0].Explanation = "An owned scheduled Job failed; current schedule and template were inspected"
				decision.Actions[0].Impact = "degraded"
			} else if kind == "HorizontalPodAutoscaler" {
				decision.Actions[0].Diagnosis = "autoscaling-metric-failure"
				decision.Actions[0].Explanation = "HPA metric computation failed; application impact requires further investigation"
				decision.Actions[0].Impact = "degraded"
			} else if kind == "Ingress" {
				decision.Actions[0].Diagnosis = "missing-ingress-service-port"
				decision.Actions[0].Explanation = "The Ingress references a port absent from its backend Service; external traffic has not been tested"
				decision.Actions[0].Impact = "degraded"
			}

			if noAction {
				decision = incident.Decision{Outcome: "no_action", Reason: "No application impact established", Actions: []incident.Action{}}
			}

			text, _ := json.Marshal(decision)
			output = append(output, map[string]any{"type": "message", "id": "m", "role": "assistant", "status": "completed",
				"content": []any{map[string]any{"type": "output_text", "text": string(text), "annotations": []any{}}}})
		}

		json.NewEncoder(w).Encode(map[string]any{"id": "response", "status": "completed", "output": output})
	}))
	defer model.Close()

	cfg := config.Config{Once: true, Auth: "in-cluster", ObserverID: "fixture", NamespaceWatch: []string{"apps"}, MaxObjects: 100, MaxInputChars: 20000, MaxOutputTokens: 2000,
		CallsPerHour: 60, MaxToolCalls: config.DefaultMaxToolCalls, RequestTimeout: time.Second, RunTimeout: 5 * time.Second, InvestigationTimeout: 3 * time.Second, Endpoint: model.URL + "/openai/v1/", APIKey: "fixture", Model: "fixture", Policy: health.DefaultPolicy()}
	cfg.NamespaceWatch, cfg.NamespaceIgnore = scope.NamespaceWatch, scope.NamespaceIgnore

	var out, diag bytes.Buffer

	r := Runner{Config: cfg, Source: kube.NewSource(client, cfg), Analyzer: &analysis.Analyzer{Config: cfg, Kubernetes: client},
		Incidents: incident.Reconciler{Repository: incident.Kubernetes{Client: client, Observer: "fixture", Limit: 100, Timeout: time.Second}, Observer: "fixture", Model: "fixture"},
		Output:    &output.Writer{Out: &out, Err: &diag}}
	if code := r.Once(t.Context()); code != 0 {
		t.Fatalf("code=%d out=%s diag=%s", code, &out, &diag)
	}

	if noAction {
		if !strings.Contains(out.String(), "0 persisted incident changes") || creates.Load() != 0 || modelCalls.Load() != 2 || toolReads.Load() != 1 {
			t.Fatalf("no-action published an incident or skipped tool-assisted AI: %s", &out)
		}

		return
	}

	if !strings.Contains(out.String(), "1 persisted incident changes") || creates.Load() != 1 || modelCalls.Load() != 2 || toolReads.Load() != 1 {
		t.Fatalf("wrong end-to-end output: %s", &out)
	}

	if stored["kind"] != "Incident" {
		t.Fatal("legacy HealthReport was published")
	}

	name := stored["metadata"].(map[string]any)["name"].(string)
	if name != incident.Name(incident.Identity{Observer: "fixture", Anchor: resource, Family: family}) ||
		!strings.Contains(out.String(), name) || !strings.HasPrefix(name, "inc-") {
		t.Fatal("stored Incident name missing from activity log")
	}

	analysisPayload := stored["spec"].(map[string]any)["analysis"].(map[string]any)
	if analysisPayload["title"] != "Workload failed" || analysisPayload["severity"] != "high" {
		t.Fatal("deterministic evidence replaced AI judgment")
	}

	evidenceIDs := analysisPayload["evidence_ids"].([]any)
	if len(evidenceIDs) != 1 || evidenceIDs[0] != "inspect-resource-1" {
		t.Fatal("descriptive tool citation was not persisted")
	}
}

// TestCronJobCollectionToolAgentAndIncidentAPI verifies scoped collection, actual SDK inspection and AI authority for scheduled failures.
func TestCronJobCollectionToolAgentAndIncidentAPI(t *testing.T) {
	for _, scope := range []config.Config{
		{NamespaceWatch: []string{"apps"}},
		{},
		{NamespaceIgnore: []string{"system"}},
	} {
		for _, noAction := range []bool{false, true} {
			t.Run(fmt.Sprintf("watch=%t/ignore=%t/no-action=%t", len(scope.NamespaceWatch) != 0, len(scope.NamespaceIgnore) != 0, noAction), func(t *testing.T) {
				collectorToolAgentAndIncidentAPI(t, scope, "CronJob", noAction)
			})
		}
	}
}

// TestIngressCollectionToolAgentAndIncidentAPI verifies safe routing inspection and AI authority with actual offline SDK request shapes.
func TestIngressCollectionToolAgentAndIncidentAPI(t *testing.T) {
	for _, scope := range []config.Config{
		{NamespaceWatch: []string{"apps"}},
		{},
		{NamespaceIgnore: []string{"system"}},
	} {
		for _, noAction := range []bool{false, true} {
			t.Run(fmt.Sprintf("watch=%t/ignore=%t/no-action=%t", len(scope.NamespaceWatch) != 0, len(scope.NamespaceIgnore) != 0, noAction), func(t *testing.T) {
				collectorToolAgentAndIncidentAPI(t, scope, "Ingress", noAction)
			})
		}
	}
}

// TestAttachmentCollectionToolAgentAndIncidentAPI verifies cluster attachment inspection, AI authority and scope-safe actual SDK request shapes offline.
func TestAttachmentCollectionToolAgentAndIncidentAPI(t *testing.T) {
	for _, scope := range []config.Config{
		{NamespaceWatch: []string{"apps"}},
		{},
		{NamespaceIgnore: []string{"system"}},
	} {
		for _, noAction := range []bool{false, true} {
			t.Run(fmt.Sprintf("watch=%t/ignore=%t/no-action=%t", len(scope.NamespaceWatch) != 0, len(scope.NamespaceIgnore) != 0, noAction), func(t *testing.T) {
				collectorToolAgentAndIncidentAPI(t, scope, "VolumeAttachment", noAction)
			})
		}
	}
}

// TestHPACollectionToolAgentAndIncidentAPI verifies actual SDK HPA inspection, validated persistence and AI no-action in each namespace mode.
func TestHPACollectionToolAgentAndIncidentAPI(t *testing.T) {
	for _, scope := range []config.Config{
		{NamespaceWatch: []string{"apps"}},
		{},
		{NamespaceIgnore: []string{"system"}},
	} {
		for _, noAction := range []bool{false, true} {
			t.Run(fmt.Sprintf("watch=%t/ignore=%t/no-action=%t", len(scope.NamespaceWatch) != 0, len(scope.NamespaceIgnore) != 0, noAction), func(t *testing.T) {
				collectorToolAgentAndIncidentAPI(t, scope, "HorizontalPodAutoscaler", noAction)
			})
		}
	}
}
