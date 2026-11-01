/*
Purpose: Test incident lifecycle, decision scope and persistence contracts.
Flow: Offline memory and local Kubernetes HTTP fixtures cover semantic no-ops,
recovery gating, identity across restarts, ambiguous writes and stale results;
request checks verify cluster scope, preconditions and preservation of human triage status.
*/
package incident

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

type ambiguousRepository struct {
	Memory
	saves int
}

// fixture pairs scoped Pod evidence with a valid material raise action for lifecycle tests.
func fixture() (InvestigationContext, Decision) {
	r := health.Resource{Kind: "Pod", Namespace: "apps", Name: "pod", UID: "uid"}
	ref := Ref(r)
	in := InvestigationContext{Anchor: ref, Resources: []Resource{{Ref: ref, Kind: r.Kind, Namespace: r.Namespace, Name: r.Name}}, Identities: map[string]health.Resource{ref: r},
		Incidents: []Existing{}, Signals: []Signal{{ID: "evidence"}}}
	d := Decision{Outcome: "decisions", Reason: "Requires attention", Actions: []Action{{Operation: "raise", Family: "container-failure", ResourceRefs: []string{ref},
		Title: "Container repeatedly fails", Severity: "medium", Impact: "degraded", Diagnosis: "repeated-exit", Explanation: "Observed repeated failure",
		EvidenceIDs: []string{"evidence"}, Recommendations: []string{"Inspect the workload configuration"}, Uncertainty: "Root cause not confirmed", MaterialChange: true}}}

	return in, d
}

// TestIncidentLifecycleAndSemanticNoops checks material updates, recovery gating and
// restart-safe reopening while ensuring prose-only changes neither write nor replace identity.
func TestIncidentLifecycleAndSemanticNoops(t *testing.T) {
	in, d := fixture()
	repo := &Memory{}
	s := Reconciler{Repository: repo, Observer: "fixture", Model: "model"}

	if err := Validate(d, in, map[string]bool{"evidence": true}); err != nil {
		t.Fatal(err)
	}

	changes, err := s.Apply(t.Context(), in, d, func() bool { return true })
	if err != nil || len(changes) != 1 {
		t.Fatalf("raise: %v", err)
	}

	name := changes[0].Name
	if !strings.HasPrefix(name, "inc-") || len(name) != len("inc-")+16 {
		t.Fatalf("incorrect Incident name: %s", name)
	}

	in.Incidents, err = s.Existing(t.Context(), in.Identities[in.Anchor])
	if err != nil {
		t.Fatal(err)
	}

	d.Actions[0].Operation, d.Actions[0].IncidentID = "update", name
	d.Actions[0].Explanation = "Paraphrased same conclusion"

	changes, err = s.Apply(t.Context(), in, d, func() bool { return true })
	if err != nil || len(changes) != 0 {
		t.Fatal("prose-only update wrote a CR")
	}

	d.Actions[0].Severity = "high"

	changes, err = s.Apply(t.Context(), in, d, func() bool { return true })
	if err != nil || len(changes) != 1 || changes[0].Name != name {
		t.Fatal("material update lost identity")
	}

	in.Incidents, _ = s.Existing(t.Context(), in.Identities[in.Anchor])

	d.Actions[0].Operation = "resolve"
	if Validate(d, in, map[string]bool{"evidence": true}) == nil {
		t.Fatal("resolution allowed without recovery")
	}

	in.RecoveryEligible = true

	changes, err = s.Apply(t.Context(), in, d, func() bool { return true })
	if err != nil || len(changes) != 1 || changes[0].Incident.Lifecycle.State != "resolved" {
		t.Fatal("resolution failed")
	}
	// A new reconciler simulates a process restart; identity is independent of run ID.
	s = Reconciler{Repository: repo, Observer: "fixture", Model: "model"}
	in.Incidents, _ = s.Existing(t.Context(), in.Identities[in.Anchor])
	d.Actions[0].Operation = "update"

	changes, err = s.Apply(t.Context(), in, d, func() bool { return true })
	if err != nil || len(changes) != 1 || changes[0].Name != name || changes[0].Operation != "reopen" || changes[0].Incident.Lifecycle.Recurrences != 1 {
		t.Fatal("restart/recurrence duplicated incident")
	}
}

// TestStoredIncidentNameValidation accepts only cluster-scoped records with the current identity-derived name.
func TestStoredIncidentNameValidation(t *testing.T) {
	in, d := fixture()
	identity := Identity{"fixture", in.Identities[in.Anchor], d.Actions[0].Family}

	r, err := next(nil, identity, d.Actions[0], in, "model", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}

	payload, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&r.Value)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name, namespace string
		valid           bool
	}{
		{Name(identity), "", true},
		{"incident-invalid", "", false},
		{Name(Identity{"other", identity.Anchor, identity.Family}), "", false},
		{Name(identity), "kubescout", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obj := &unstructured.Unstructured{Object: map[string]any{"spec": payload}}
			obj.SetName(tc.name)
			obj.SetNamespace(tc.namespace)
			obj.SetLabels(map[string]string{"app.kubernetes.io/managed-by": "kubescout"})

			record, err := (Kubernetes{Observer: "fixture"}).decode(obj)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%t err=%v", tc.valid, err)
			}

			if tc.valid && record.Name != tc.name {
				t.Fatal("stored name was rewritten")
			}
		})
	}
}

// TestDecisionScopeAndNoAction rejects unknown handles, citations and incidents
// or duplicate families while allowing no-action.
func TestDecisionScopeAndNoAction(t *testing.T) {
	in, d := fixture()
	for _, mutate := range []func(*Decision){
		func(d *Decision) { d.Actions[0].ResourceRefs = []string{"unknown"} },
		func(d *Decision) { d.Actions[0].EvidenceIDs = []string{"unknown"} },
		func(d *Decision) { d.Actions[0].Operation = "resolve"; d.Actions[0].IncidentID = "unknown" },
		func(d *Decision) { d.Actions = append(d.Actions, d.Actions[0]) },
	} {
		_, d = fixture()
		mutate(&d)

		if Validate(d, in, map[string]bool{"evidence": true}) == nil {
			t.Fatal("invalid AI decision accepted")
		}
	}

	if err := Validate(Decision{Outcome: "no_action", Reason: "Not worth surfacing", Actions: []Action{}}, in, nil); err != nil {
		t.Fatal(err)
	}
}

// Save counts and commits a fixture write before reporting a lost response to simulate an ambiguous result.
func (r *ambiguousRepository) Save(ctx context.Context, v Record) error {
	r.saves++
	if err := r.Memory.Save(ctx, v); err != nil {
		return err
	}

	return errors.New("response lost after commit")
}

// TestAmbiguousWriteDoesNotCreateDuplicateOrRerunAI checks reread reconciliation of a
// committed write and rejects superseded conclusions before another publication.
func TestAmbiguousWriteDoesNotCreateDuplicateOrRerunAI(t *testing.T) {
	in, d := fixture()
	repo := &ambiguousRepository{Memory: Memory{}}
	s := Reconciler{Repository: repo, Observer: "fixture"}

	changes, err := s.Apply(t.Context(), in, d, func() bool { return true })
	if err != nil || repo.saves != 1 || len(repo.Records) != 1 || len(changes) != 1 {
		t.Fatalf("ambiguous commit: %v", err)
	}

	if _, err := s.Apply(t.Context(), in, d, func() bool { return false }); err == nil {
		t.Fatal("stale conclusion published")
	}
}

// TestKubernetesIncidentRequestContract checks cluster-scoped create, lookup and patch requests,
// state indexing, optimistic concurrency and preservation of human triage status.
func TestKubernetesIncidentRequestContract(t *testing.T) {
	in, d := fixture()

	var object map[string]any

	creates, patches := 0, 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if !strings.HasPrefix(r.URL.Path, "/apis/kubescout.benc-uk.github.com/v1alpha1/incidents") {
			t.Error("wrong incident scope")
		}

		switch r.Method {
		case "GET":
			if object == nil {
				w.WriteHeader(404)
				io.WriteString(w, `{"kind":"Status","apiVersion":"v1","code":404,"reason":"NotFound"}`)

				return
			}

			if r.URL.Path == "/apis/kubescout.benc-uk.github.com/v1alpha1/incidents" {
				if r.URL.Query().Get("labelSelector") != "app.kubernetes.io/managed-by=kubescout,kubescout.benc-uk.github.com/observer=fixture,kubescout.benc-uk.github.com/state=open" {
					t.Error("open incident index must select the state label")
				}

				json.NewEncoder(w).Encode(map[string]any{
					"apiVersion": APIVersion, "kind": "IncidentList",
					"metadata": map[string]any{"resourceVersion": "2"}, "items": []any{object},
				})

				return
			}

			json.NewEncoder(w).Encode(object)
		case "POST":
			creates++

			if json.NewDecoder(r.Body).Decode(&object) != nil {
				t.Error("invalid create")
			}

			if object["kind"] != "Incident" || object["status"] != nil {
				t.Error("incorrect incident object")
			}

			meta := object["metadata"].(map[string]any)
			if _, exists := meta["namespace"]; exists {
				t.Error("cluster-scoped incident must omit metadata.namespace")
			}

			if meta["labels"].(map[string]any)["kubescout.benc-uk.github.com/state"] != "open" ||
				object["spec"].(map[string]any)["lifecycle"].(map[string]any)["state"] != "open" {
				t.Error("create must use state in labels and lifecycle")
			}

			if meta["name"] != Name(Identity{"fixture", in.Identities[in.Anchor], d.Actions[0].Family}) {
				t.Error("incorrect Incident name")
			}

			meta["resourceVersion"], meta["uid"] = "1", "incident-uid"
			object["status"] = map[string]any{"triage": map[string]any{"dismissedRevision": 1}}

			if meta["ownerReferences"] != nil {
				t.Error("incident cannot be owned by a pod")
			}

			w.WriteHeader(201)
			json.NewEncoder(w).Encode(object)
		case "PATCH":
			patches++

			if r.Header.Get("Content-Type") != "application/json-patch+json" {
				t.Error("update lacks optimistic concurrency")
			}

			var patch []map[string]any
			json.NewDecoder(r.Body).Decode(&patch)

			if len(patch) != 4 || patch[0]["path"] != "/metadata/resourceVersion" || patch[1]["value"] != "incident-uid" || patch[2]["path"] != "/spec" {
				t.Error("missing identity/resourceVersion preconditions")
			}

			if patch[3]["path"] != "/metadata/labels/kubescout.benc-uk.github.com~1state" {
				t.Error("update must maintain state label")
			}

			for _, operation := range patch {
				if strings.HasPrefix(operation["path"].(string), "/status") {
					t.Error("agent must preserve human triage status")
				}
			}

			object["spec"] = patch[2]["value"]
			object["metadata"].(map[string]any)["resourceVersion"] = "2"
			json.NewEncoder(w).Encode(object)
		default:
			t.Error("unexpected incident verb")
		}
	}))
	defer server.Close()

	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}

	s := Reconciler{Repository: Kubernetes{Client: client, Observer: "fixture", Limit: 10, Timeout: time.Second}, Observer: "fixture"}

	changes, err := s.Apply(t.Context(), in, d, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	// Only the actual incident name is queried; other family lookups would be
	// rejected by ownership validation in this intentionally narrow fixture.
	r, err := s.Repository.Get(t.Context(), changes[0].Name)
	if err != nil {
		t.Fatal(err)
	}

	in.Incidents = []Existing{{ID: r.Name, Family: r.Value.Identity.Family, Analysis: r.Value.Analysis, Lifecycle: r.Value.Lifecycle}}

	d.Actions[0].Operation, d.Actions[0].IncidentID, d.Actions[0].Severity = "update", r.Name, "critical"
	if _, err := s.Apply(t.Context(), in, d, func() bool { return true }); err != nil {
		t.Fatal(err)
	}

	if creates != 1 || patches != 1 {
		t.Fatal("unexpected CR write count")
	}

	open, err := s.Repository.ListOpen(t.Context())
	if err != nil || len(open) != 1 || open[0].Value.Lifecycle.State != "open" {
		t.Fatalf("state-based restart index failed: %v", err)
	}
}
