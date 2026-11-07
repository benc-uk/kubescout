/*
Purpose: Test explicit recovery observations and replacement-aware incident contexts.
Flow: Offline projected resources and stored Incident fixtures retain original
identities, expose bounded replacement evidence and prevent child deletion from
hiding active controller problems. A local HTTP index fixture rejects mismatched
lifecycle labels; no live Kubernetes or AI requests are made.
*/
package incident

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// TestRecoveryAnchorObservations distinguishes present, absent and replaced anchors without leaking UIDs.
func TestRecoveryAnchorObservations(t *testing.T) {
	original := health.Resource{Kind: "Pod", Namespace: "apps", Name: "target", UID: "private-original-uid"}
	replacement := original
	replacement.UID = "private-replacement-uid"
	key := Ref(original)
	open := []Record{{Value: Incident{Identity: Identity{"fixture", original, "container-failure"},
		Resources: []health.Resource{original}, Lifecycle: Lifecycle{State: "open"}}}}

	for _, tt := range []struct {
		name, state string
		objects     []health.Object
		active      bool
	}{
		{name: "present", state: "present", objects: []health.Object{{Resource: original, Phase: "Running", Conditions: []health.Condition{{Type: "Ready", Status: "True"}}}}},
		{name: "absent", state: "absent"},
		{name: "healthy replacement", state: "replaced", objects: []health.Object{{Resource: replacement, Phase: "Running", Conditions: []health.Condition{{Type: "Ready", Status: "True"}}}}},
		{name: "failing replacement", state: "replaced", objects: []health.Object{{Resource: replacement, Phase: "Failed"}}, active: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			named := original
			named.UID = ""

			contexts, err := BuildInvestigationContexts(health.Snapshot{At: time.Now(), Objects: tt.objects, Continuity: 7,
				RecoveryChanges: map[health.Resource]uint64{named: 12}}, health.DefaultPolicy(), open, 10)
			if err != nil {
				t.Fatal(err)
			}

			in := contexts[key]
			if in.Anchor != key || in.Identities[key] != original || in.Recovery == nil || in.Recovery.AnchorState != tt.state ||
				Active(in) != tt.active || in.Continuity != 7 || in.RecoveryChanges[key] != 12 || in.RecoveryEligible {
				t.Fatalf("incorrect recovery observation: %+v", in)
			}

			if tt.state == "replaced" && (in.Recovery.ReplacementRef != Ref(replacement) || in.Objects[Ref(replacement)].Resource != replacement) {
				t.Fatal("replacement context missing or original identity rewritten")
			}

			data, err := json.Marshal(in)
			if err != nil {
				t.Fatal(err)
			}

			if strings.Contains(string(data), original.UID) || strings.Contains(string(data), replacement.UID) ||
				strings.Contains(string(data), "Continuity") || strings.Contains(string(data), "RecoveryChanges") {
				t.Fatal("private recovery identities or continuity reached model JSON")
			}
		})
	}
}

// TestReplacementOwnerSignalsRemainVisible includes a replacement's verified controller evidence on the old anchor.
func TestReplacementOwnerSignalsRemainVisible(t *testing.T) {
	original := health.Resource{Kind: "Pod", Namespace: "apps", Name: "target", UID: "old"}
	deployment := health.Object{Resource: health.Resource{Kind: "Deployment", Namespace: "apps", Name: "workload", UID: "deployment"},
		Desired: 1, Available: 1, Updated: 1}
	rs := health.Object{Resource: health.Resource{Kind: "ReplicaSet", Namespace: "apps", Name: "replicas", UID: "rs"}, Owner: deployment.Resource}
	pod := health.Object{Resource: health.Resource{Kind: "Pod", Namespace: "apps", Name: "target", UID: "new"}, Owner: rs.Resource, Phase: "Failed"}
	open := []Record{{Value: Incident{Identity: Identity{"fixture", original, "container-failure"}, Lifecycle: Lifecycle{State: "open"}}}}

	contexts, err := BuildInvestigationContexts(health.Snapshot{At: time.Now(), Objects: []health.Object{deployment, rs, pod}}, health.DefaultPolicy(), open, 10)
	if err != nil {
		t.Fatal(err)
	}

	in := contexts[Ref(original)]
	if !Active(in) || in.Anchor != Ref(original) || in.Objects[Ref(pod.Resource)].Resource != pod.Resource ||
		in.Objects[Ref(deployment.Resource)].Resource != deployment.Resource {
		t.Fatal("replacement ownership stranded the old incident or hid active failure")
	}
}

// TestDeletedChildDoesNotRecoverController preserves active failure after the original replica is replaced.
func TestDeletedChildDoesNotRecoverController(t *testing.T) {
	deployment := health.Object{Resource: health.Resource{Kind: "Deployment", Namespace: "apps", Name: "workload", UID: "deployment"},
		Desired: 1, Available: 1, Updated: 1}
	rs := health.Object{Resource: health.Resource{Kind: "ReplicaSet", Namespace: "apps", Name: "replicas", UID: "rs"}, Owner: deployment.Resource}
	oldPod := health.Resource{Kind: "Pod", Namespace: "apps", Name: "old", UID: "old"}
	newPod := health.Object{Resource: health.Resource{Kind: "Pod", Namespace: "apps", Name: "new", UID: "new"}, Owner: rs.Resource, Phase: "Failed"}
	open := []Record{{Value: Incident{Identity: Identity{"fixture", deployment.Resource, "container-failure"},
		Resources: []health.Resource{oldPod}, Lifecycle: Lifecycle{State: "open"}}}}

	contexts, err := BuildInvestigationContexts(health.Snapshot{At: time.Now(), Objects: []health.Object{deployment, rs, newPod}}, health.DefaultPolicy(), open, 10)
	if err != nil {
		t.Fatal(err)
	}

	in := contexts[Ref(deployment.Resource)]
	if !Active(in) || in.Recovery.AnchorState != "present" || in.Objects[Ref(newPod.Resource)].Resource != newPod.Resource {
		t.Fatal("deletion of one replica hid the still-failing workload")
	}
}

// TestOpenIndexRejectsResolvedSelectorMismatch rejects resolved records mislabeled as open in a complete HTTP list.
func TestOpenIndexRejectsResolvedSelectorMismatch(t *testing.T) {
	in, decision := fixture()
	identity := Identity{"fixture", in.Identities[in.Anchor], decision.Actions[0].Family}

	record, err := next(nil, identity, decision.Actions[0], in, "fixture", time.Now())
	if err != nil {
		t.Fatal(err)
	}

	record.Value.Lifecycle.State = "resolved"

	spec, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&record.Value)
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": APIVersion, "kind": "IncidentList",
			"metadata": map[string]any{"resourceVersion": "1"}, "items": []any{map[string]any{"apiVersion": APIVersion, "kind": "Incident",
				"metadata": map[string]any{"name": record.Name, "uid": "incident", "resourceVersion": "1",
					"labels": map[string]any{"app.kubernetes.io/managed-by": "kubescout", "kubescout.benc-uk.github.com/observer": "fixture",
						"kubescout.benc-uk.github.com/state": "open"}}, "spec": spec}}})
	}))
	defer server.Close()

	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}

	repository := Kubernetes{Client: client, Observer: "fixture", Limit: 10, Timeout: time.Second}
	if _, err := repository.ListOpen(t.Context()); err == nil || !strings.Contains(err.Error(), "open-state selector") {
		t.Fatalf("mislabeled resolved incident accepted: %v", err)
	}
}
