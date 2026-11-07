/*
Purpose: Verify unified recovery through real collection, SDK tools and Incident HTTP requests.
Flow: Offline Kubernetes list/watch/GET/patch and Responses fixtures cover
present, absent and replaced anchors after continuous recovery. Strict model
context, exact citations, UID/version preconditions and human-status preservation
are checked without live Kubernetes resources or billable model calls.
Removed standalone Pod and Job fixtures verify the actual SDK receives the obsolescence policy without replacement proof.
*/
package runner

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

	"github.com/benc-uk/kubescout/internal/analysis"
	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/kube"
	"github.com/benc-uk/kubescout/internal/output"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

type recoveryWriteNotice struct {
	confirmed chan struct{}
	mu        sync.Mutex
	text      strings.Builder
}

// Write notifies tests only when the runner logs a confirmed AI resolution, not an HTTP proposal.
func (w *recoveryWriteNotice) Write(p []byte) (int, error) {
	w.mu.Lock()
	_, _ = w.text.Write(p)
	w.mu.Unlock()

	if strings.Contains(string(p), "persisted resolve") {
		select {
		case w.confirmed <- struct{}{}:
		default:
		}
	}

	return len(p), nil
}

// activity returns synchronized fixture diagnostics when an expected confirmed write is missing.
func (w *recoveryWriteNotice) activity() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.text.String()
}

// TestRecoveryCollectorSDKAndIncidentAPI verifies all anchor observations use the same actual SDK and persistence flow.
func TestRecoveryCollectorSDKAndIncidentAPI(t *testing.T) {
	for _, state := range []string{"present", "absent", "replaced"} {
		t.Run(state, func(t *testing.T) { recoveryCollectorSDKAndIncidentAPI(t, state, recoveryRecord()) })
	}
}

// TestRemovedResourceIncidentsResolveAsObsolete covers old scheduling, crash and Job conditions without replacement/owner proof.
func TestRemovedResourceIncidentsResolveAsObsolete(t *testing.T) {
	for _, fixture := range []struct {
		kind, family, name string
	}{
		{"Pod", "scheduling", "removed-pending"},
		{"Pod", "container-failure", "removed-crash"},
		{"Job", "job-failure", "removed-job"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			record := recoveryRecord()
			record.Value.Identity.Anchor.Kind = fixture.kind
			record.Value.Identity.Anchor.Name = fixture.name
			record.Value.Identity.Family = fixture.family
			record.Value.Analysis.Title = "Previous " + fixture.kind + " condition"
			record.Value.Analysis.Diagnosis = fixture.family
			recoveryCollectorSDKAndIncidentAPI(t, "absent", record)
		})
	}
}

// recoveryCollectorSDKAndIncidentAPI runs bounded local streams, automatic descriptions and one AI-selected inspection round.
// Removed Pod/Job incidents need no replacement object or owner fixture for AI-approved obsolescence.
func recoveryCollectorSDKAndIncidentAPI(t *testing.T, state string, record incident.Record) {
	record.Value.Identity.Anchor.UID = "private-original-resource-uid"
	record.Value.Resources = []health.Resource{record.Value.Identity.Anchor}
	record.Name = incident.Name(record.Value.Identity)
	record.UID = "private-incident-uid"
	ref := incident.Ref(record.Value.Identity.Anchor)

	current := record.Value.Identity.Anchor
	if state == "replaced" {
		current.UID = "private-replacement-resource-uid"
	}

	resourcePath := "/pods"
	if current.Kind == "Job" {
		resourcePath = "/jobs"
	}

	spec, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&record.Value)
	if err != nil {
		t.Fatal(err)
	}

	stored := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": incident.APIVersion, "kind": "Incident", "metadata": map[string]any{
			"name": record.Name, "uid": record.UID, "resourceVersion": record.Version,
			"labels": map[string]any{"app.kubernetes.io/managed-by": "kubescout", "kubescout.benc-uk.github.com/observer": "fixture",
				"kubescout.benc-uk.github.com/state": "open"}},
		"spec": spec, "status": map[string]any{"triage": map[string]any{"dismissed": true, "revision": int64(1)}}}}

	var (
		mu                  sync.Mutex
		patches, modelCalls atomic.Int32
	)

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if strings.Contains(r.URL.Path, "/incidents") {
			mu.Lock()
			defer mu.Unlock()

			if !strings.HasPrefix(r.URL.Path, "/apis/kubescout.benc-uk.github.com/v1alpha1/incidents") {
				t.Error("Incident API request escaped cluster scope")
			}

			if r.Method == http.MethodPatch {
				var patch []struct {
					Op, Path string
					Value    json.RawMessage
				}

				if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)

					return
				}

				if len(patch) != 4 || patch[0].Op != "test" || patch[0].Path != "/metadata/resourceVersion" ||
					string(patch[0].Value) != `"1"` || patch[1].Op != "test" || patch[1].Path != "/metadata/uid" ||
					string(patch[1].Value) != `"private-incident-uid"` || patch[2].Path != "/spec" ||
					patch[3].Path != "/metadata/labels/kubescout.benc-uk.github.com~1state" || string(patch[3].Value) != `"resolved"` {
					t.Error("resolution patch lost exact Incident preconditions or changed unauthorized fields")
					w.WriteHeader(http.StatusConflict)

					return
				}

				var next map[string]any
				if err := json.Unmarshal(patch[2].Value, &next); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)

					return
				}

				stored.Object["spec"] = next
				stored.SetResourceVersion("2")
				labels := stored.GetLabels()
				labels["kubescout.benc-uk.github.com/state"] = "resolved"
				stored.SetLabels(labels)
				patches.Add(1)

				_ = json.NewEncoder(w).Encode(stored.Object)

				return
			}

			if r.Method != http.MethodGet {
				t.Error("recovery attempted an Incident create/delete or status write")
				w.WriteHeader(http.StatusMethodNotAllowed)

				return
			}

			if strings.HasSuffix(r.URL.Path, "/incidents") {
				if r.URL.Query().Get("limit") != "100" || !strings.Contains(r.URL.Query().Get("labelSelector"), "observer=fixture") ||
					!strings.Contains(r.URL.Query().Get("labelSelector"), "state=open") {
					t.Error("Incident refresh lost pagination or observer/lifecycle scope")
				}

				items := []any{}
				if stored.GetLabels()["kubescout.benc-uk.github.com/state"] == "open" {
					items = append(items, stored.Object)
				}

				_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": incident.APIVersion, "kind": "IncidentList",
					"metadata": map[string]any{"resourceVersion": stored.GetResourceVersion()}, "items": items})

				return
			}

			if strings.HasSuffix(r.URL.Path, "/"+record.Name) {
				_ = json.NewEncoder(w).Encode(stored.Object)
			} else {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `{"apiVersion":"v1","kind":"Status","reason":"NotFound","code":404}`)
			}

			return
		}

		if r.Method != http.MethodGet {
			t.Error("recovery attempted a workload write")
			w.WriteHeader(http.StatusMethodNotAllowed)

			return
		}

		if r.URL.Query().Get("watch") == "true" {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()

			return
		}

		items := []any{}
		if strings.HasSuffix(r.URL.Path, "/nodes") || strings.HasSuffix(r.URL.Path, "/nodes/node") {
			items = append(items, map[string]any{"apiVersion": "v1", "kind": "Node",
				"metadata": map[string]any{"name": "node", "uid": "private-node-uid", "resourceVersion": "1"},
				"status":   map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}})
		}

		if strings.HasSuffix(r.URL.Path, "/nodes/node") {
			_ = json.NewEncoder(w).Encode(items[0])
			return
		}

		if state != "absent" && (strings.HasSuffix(r.URL.Path, resourcePath) || strings.HasSuffix(r.URL.Path, resourcePath+"/"+current.Name)) {
			items = append(items, map[string]any{"apiVersion": "v1", "kind": "Pod",
				"metadata": map[string]any{"name": current.Name, "namespace": current.Namespace, "uid": current.UID, "resourceVersion": "1"},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "app", "image": "app:v1"}}},
				"status":   map[string]any{"phase": "Running", "conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}})
		}

		if strings.HasSuffix(r.URL.Path, resourcePath+"/"+current.Name) {
			if len(items) == 0 {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `{"apiVersion":"v1","kind":"Status","reason":"NotFound","code":404}`)
			} else {
				_ = json.NewEncoder(w).Encode(items[0])
			}

			return
		}

		_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{"resourceVersion": "1"}, "items": items})
	}))
	t.Cleanup(kubeServer.Close)

	cfg := config.Config{Auth: "kubeconfig", ObserverID: "fixture", NamespaceWatch: []string{"apps"}, MaxObjects: 100,
		MaxInputChars: 20000, MaxOutputTokens: 2000, CallsPerHour: 60, MaxToolCalls: config.DefaultMaxToolCalls,
		RequestTimeout: time.Second, RunTimeout: 3 * time.Second, InvestigationTimeout: time.Second,
		BatchWindow: 5 * time.Millisecond, CheckInterval: 10 * time.Millisecond, RecoveryWindow: 30 * time.Millisecond,
		Cooldown: time.Minute, ReviewInterval: time.Hour, WatchTimeout: 10 * time.Second, Model: "fixture", APIKey: "fixture", Policy: health.DefaultPolicy()}

	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := modelCalls.Add(1)

		var body struct {
			Store        json.RawMessage
			Instructions string
			Input        []struct {
				Role, Type, Output string
				Content            []struct{ Text string }
			}
		}

		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		if string(body.Store) != "false" {
			t.Error("model request enabled provider storage")
		}

		if !strings.Contains(body.Instructions, "Normally resolve an obsolete resource-specific incident") ||
			!strings.Contains(body.Instructions, "Do not require a replacement workload") ||
			!strings.Contains(body.Instructions, "Missing replacement or owner context alone is not a") {
			t.Error("actual SDK request omitted the obsolete-resource resolution policy")
		}

		var in incident.InvestigationContext

		found := false

		for _, item := range body.Input {
			if item.Role == "user" && len(item.Content) > 0 {
				text := item.Content[0].Text
				if strings.Contains(text, "private-original-resource-uid") || strings.Contains(text, "private-replacement-resource-uid") ||
					strings.Contains(text, "private-incident-uid") || strings.Contains(text, "private-node-uid") {
					t.Error("raw identity reached actual model context")
				}

				if err := json.Unmarshal([]byte(text), &in); err != nil {
					t.Error(err)
				}

				found = true
			}

			if item.Type == "function_call_output" {
				var fields map[string]any
				if err := json.Unmarshal([]byte(item.Output), &fields); err != nil {
					t.Error(err)
				}

				if fields["exists"] != (state == "present") || fields["evidence_id"] != "inspect-resource-1" ||
					state == "replaced" && fields["replacement_ref"] != incident.Ref(current) {
					t.Error("SDK tool result did not confirm the reviewed anchor incarnation")
				}
			}
		}

		if !found || in.Anchor != ref || !in.RecoveryEligible || in.Recovery == nil || in.Recovery.AnchorState != state ||
			in.Recovery.Since == nil || in.Recovery.WindowSeconds != cfg.RecoveryWindow.Seconds() ||
			time.Since(*in.Recovery.Since) < cfg.RecoveryWindow || len(in.Incidents) != 1 || in.Incidents[0].Lifecycle.Revision != 1 {
			t.Error("actual SDK input lost unified recovery timing or reviewed Incident revision")
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		if state == "absent" && (in.Recovery.ReplacementRef != "" || !strings.Contains(in.Reason, "obsolescence")) {
			t.Error("removed-resource review demanded a replacement or lost its obsolescence purpose")
		}

		result := []any{}
		if call == 1 {
			result = append(result, map[string]any{"type": "function_call", "id": "fc", "call_id": "recovery-tool",
				"name": "inspect_resource", "arguments": fmt.Sprintf(`{"resource_ref":%q}`, ref)})
		} else {
			if call != 2 {
				t.Error("unexpected additional model request")
			}

			decision := recoveryDecision(in)
			decision.Actions[0].EvidenceIDs = []string{"inspect-resource-1"}

			text, err := json.Marshal(decision)
			if err != nil {
				t.Error(err)
			}

			result = append(result, map[string]any{"type": "message", "id": "m", "role": "assistant", "status": "completed",
				"content": []any{map[string]any{"type": "output_text", "text": string(text), "annotations": []any{}}}})
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "response", "status": "completed", "output": result})
	}))
	t.Cleanup(model.Close)

	cfg.Endpoint = model.URL + "/openai/v1/"
	restCfg := &rest.Config{Host: kubeServer.URL, QPS: 1000, Burst: 1000}

	client, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		t.Fatal(err)
	}

	confirmed := make(chan struct{}, 1)
	notice := &recoveryWriteNotice{confirmed: confirmed}
	runner := Runner{Config: cfg, Source: kube.NewSource(client, cfg),
		Analyzer: &analysis.Analyzer{Config: cfg, Kubernetes: client, DescribeConfig: restCfg},
		Incidents: incident.Reconciler{Repository: incident.Kubernetes{Client: client, Observer: "fixture", Limit: 100, Timeout: time.Second},
			Observer: "fixture", Model: "fixture"},
		Output: &output.Writer{Out: notice, Err: notice}}
	startRecoveryWatch(t, &runner)

	select {
	case <-confirmed:
	case <-time.After(3 * time.Second):
		t.Fatalf("actual SDK recovery did not persist a confirmed resolution: model_calls=%d patches=%d\n%s", modelCalls.Load(), patches.Load(), notice.activity())
	}

	mu.Lock()
	defer mu.Unlock()

	var value incident.Incident
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(stored.Object["spec"].(map[string]any), &value); err != nil {
		t.Fatal(err)
	}

	triage, _, err := unstructured.NestedBool(stored.Object, "status", "triage", "dismissed")
	if err != nil || !triage || stored.GetName() != record.Name || value.Identity != record.Value.Identity ||
		value.Lifecycle.State != "resolved" || value.Lifecycle.Revision != 2 || value.Evidence == nil ||
		patches.Load() != 1 || modelCalls.Load() != 2 || len(value.Analysis.EvidenceIDs) != 1 || value.Analysis.EvidenceIDs[0] != "inspect-resource-1" {
		t.Fatal("recovery changed identity/status, bypassed AI, lost evidence or repeated writes")
	}
}
