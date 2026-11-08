/*
Purpose: Exercise configurable resources through actual local Kubernetes and Responses HTTP shapes.
Flow: Offline lists, scoped projected tool reads and SDK rounds produce validated
AI no-action or an Incident create; private fields and generic describe never reach the model.
*/
package analysis

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/kube"
	"github.com/benc-uk/kubescout/internal/resourcepolicy"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// TestExtraResourceSDKAndPersistence runs the shipped certificate policy through collection, scoped tools and AI-authorized storage.
func TestExtraResourceSDKAndPersistence(t *testing.T) {
	for _, noAction := range []bool{true, false} {
		t.Run(fmt.Sprintf("no_action=%t", noAction), func(t *testing.T) {
			policies, err := resourcepolicy.Load("../../examples/extra-resources.yaml")
			if err != nil {
				t.Fatal(err)
			}

			policy := health.DefaultPolicy()
			policy.ExtraResources = policies
			resource := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "cert-manager.io/v1", "kind": "Certificate",
				"metadata": map[string]any{"name": "tls", "namespace": "apps", "uid": "private-resource-uid",
					"resourceVersion": "1", "creationTimestamp": "2026-01-01T00:00:00Z",
					"annotations": map[string]any{"private": "private-annotation"}},
				"spec": map[string]any{"secretName": "private-secret", "data": "private-data"},
				"status": map[string]any{"conditions": []any{map[string]any{
					"type": "Ready", "status": "False", "reason": "token=private-token", "message": "private-message",
				}}},
			}}

			var (
				mutex                       sync.Mutex
				stored                      *unstructured.Unstructured
				creates, inspections, calls atomic.Int32
			)

			kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")

				incidentPath := "/apis/kubescout.benc-uk.github.com/v1alpha1/incidents"
				if strings.HasPrefix(r.URL.Path, incidentPath) {
					mutex.Lock()
					defer mutex.Unlock()

					if r.Method == http.MethodPost {
						if r.URL.Path != incidentPath {
							t.Error("Incident create escaped cluster scope")
						}

						stored = &unstructured.Unstructured{}
						if err := json.NewDecoder(r.Body).Decode(stored); err != nil {
							t.Error(err)
						}

						stored.SetUID("incident-uid")
						stored.SetResourceVersion("1")
						creates.Add(1)
						w.WriteHeader(http.StatusCreated)
						_ = json.NewEncoder(w).Encode(stored)

						return
					}

					if r.Method != http.MethodGet {
						t.Error("unexpected Incident write")
					}

					if stored != nil && r.URL.Path == incidentPath+"/"+stored.GetName() {
						_ = json.NewEncoder(w).Encode(stored)

						return
					}

					w.WriteHeader(http.StatusNotFound)
					fmt.Fprint(w, `{"apiVersion":"v1","kind":"Status","reason":"NotFound","code":404}`)

					return
				}

				if r.Method != http.MethodGet {
					t.Error("workload write attempted")
				}

				if r.URL.Path == "/apis/cert-manager.io/v1/namespaces/apps/certificates/tls" {
					inspections.Add(1)

					_ = json.NewEncoder(w).Encode(resource)

					return
				}

				if r.URL.Query().Get("limit") != "100" {
					t.Error("collection must use bounded lists, not discovery or generic describe")
				}

				items := []any{}
				if r.URL.Path == "/apis/cert-manager.io/v1/namespaces/apps/certificates" {
					items = append(items, resource.Object)
				}

				if r.URL.Path == "/api/v1/nodes" {
					items = append(items, map[string]any{"apiVersion": "v1", "kind": "Node",
						"metadata": map[string]any{"name": "node", "uid": "private-node-uid", "resourceVersion": "1"},
						"status":   map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}})
				}

				_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List",
					"metadata": map[string]any{"resourceVersion": "1"}, "items": items})
			}))
			defer kubeServer.Close()

			client, err := dynamic.NewForConfig(&rest.Config{Host: kubeServer.URL, QPS: 1000, Burst: 1000})
			if err != nil {
				t.Fatal(err)
			}

			cfg := config.Config{ObserverID: "fixture", NamespaceWatch: []string{"apps"}, Policy: policy, MaxObjects: 20,
				APIKey: "fixture-key", Model: "fixture", MaxInputChars: 20000, MaxOutputTokens: 2000,
				MaxToolCalls: config.DefaultMaxToolCalls, RequestTimeout: time.Second, InvestigationTimeout: 5 * time.Second, CallsPerHour: 60}

			snapshot, err := kube.NewSource(client, cfg).Collect(t.Context(), time.Now())
			if err != nil {
				t.Fatal(err)
			}

			contexts, err := incident.BuildInvestigationContexts(snapshot, policy, nil, 20)
			if err != nil || len(contexts) != 1 {
				t.Fatalf("custom candidate: %v", err)
			}

			var in incident.InvestigationContext
			for _, candidate := range contexts {
				in = candidate
			}

			model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := calls.Add(1)

				var body map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}

				input := string(body["input"])
				if r.URL.Path != "/openai/v1/responses" || string(body["store"]) != "false" ||
					strings.Contains(input, "private-") || !strings.Contains(input, "cert-manager.io") ||
					!strings.Contains(input, "extra.certificates.not-ready") {
					t.Error("incorrect request or private fields reached the model")
				}

				if n == 1 {
					respond(w, toolResponse(in.Anchor))

					return
				}

				if !strings.Contains(input, "inspect-resource-1") || !strings.Contains(input, "redacted credential") {
					t.Error("projected custom resource tool result missing")
				}

				if noAction {
					respond(w, textResponse(`{"outcome":"no_action","reason":"Controller reports require no user-facing change","actions":[]}`))

					return
				}

				decision := incident.Decision{Outcome: "decisions", Reason: "Certificate failure merits investigation",
					Actions: []incident.Action{{Operation: "raise", Family: "custom-resource-health", ResourceRefs: []string{in.Anchor},
						Title: "Certificate not ready", Severity: "medium", Impact: "limited", Diagnosis: "certificate-not-ready",
						Explanation: "The controller reports the certificate is not ready.", EvidenceIDs: []string{"inspect-resource-1"},
						Recommendations: []string{"Review the certificate controller status."}, Uncertainty: "Active TLS was not tested.", MaterialChange: true}}}

				text, err := json.Marshal(decision)
				if err != nil {
					t.Error(err)
				}

				respond(w, textResponse(string(text)))
			}))
			defer model.Close()

			cfg.Endpoint = model.URL + "/openai/v1/"
			analyzer := Analyzer{Config: cfg, Kubernetes: client}

			decision, err := analyzer.Investigate(t.Context(), in)
			if err != nil {
				t.Fatal(err)
			}

			reconciler := incident.Reconciler{Observer: cfg.ObserverID, Model: cfg.Model,
				Repository: incident.Kubernetes{Client: client, Observer: cfg.ObserverID, Limit: 20, Timeout: time.Second}}

			changes, err := reconciler.Apply(t.Context(), in, decision, func() bool { return true })
			if err != nil || calls.Load() != 2 || inspections.Load() != 1 || len(changes) != int(creates.Load()) {
				t.Fatalf("AI/tool/persistence flow: %v", err)
			}

			if noAction && creates.Load() != 0 || !noAction && creates.Load() != 1 {
				t.Fatal("CEL evidence bypassed the AI incident decision")
			}

			if !noAction && changes[0].Incident.Identity.Anchor.APIGroup != "cert-manager.io" {
				t.Fatal("Incident storage lost custom-resource API-group identity")
			}
		})
	}
}
