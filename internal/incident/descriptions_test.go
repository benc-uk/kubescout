/*
Purpose: Test native-description retention, bounds and Kubernetes write confirmation.
Flow: Offline lifecycle and HTTP fixtures prove only material revisions replace
reviewed evidence, ambiguous retries remain stable and CRD pruning is reported.
Each stored resource has one native output; obsolete evidence formats are rejected.
*/
package incident

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// evidenceFixture builds valid bounded host-owned descriptions for a supplied context.
func evidenceFixture(in InvestigationContext, text string) *Evidence {
	return &Evidence{Format: DescriptionFormat, Descriptions: []ResourceDescription{{
		EvidenceID: "kubectl-describe-1", Resource: in.Resources[0], Source: DescribeSource, ObservedAt: time.Now().UTC(),
		Output: DescriptionOutput{Status: "captured", Text: "Name: pod\nStatus: " + text},
	}}}
}

// TestDescriptionJSONContract checks the descriptions field is emitted and the previous field is not an alias.
func TestDescriptionJSONContract(t *testing.T) {
	in, _ := fixture()
	evidence := evidenceFixture(in, "native output")

	data, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}

	if len(fields) != 3 || fields["descriptions"] == nil || fields["snapshots"] != nil {
		t.Fatalf("unexpected evidence fields: %s", data)
	}

	fields["snapshots"] = fields["descriptions"]
	delete(fields, "descriptions")

	obsolete, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}

	var decoded Evidence
	if err := json.Unmarshal(obsolete, &decoded); err != nil {
		t.Fatal(err)
	}

	if ValidateEvidence(&decoded) == nil {
		t.Fatal("previous field accepted as a description alias")
	}
}

// TestDescriptionLifecycle retains old descriptions on semantic no-ops and replaces them on all material actions.
func TestDescriptionLifecycle(t *testing.T) {
	in, d := fixture()
	repo := &ambiguousRepository{}
	reconciler := Reconciler{Repository: repo, Observer: "fixture", Model: "fixture"}
	d.Evidence = evidenceFixture(in, "Initial failure")

	changes, err := reconciler.Apply(t.Context(), in, d, func() bool { return true })
	if err != nil || len(changes) != 1 || len(repo.Records) != 1 || !reflect.DeepEqual(changes[0].Incident.Evidence, d.Evidence) {
		t.Fatalf("raise/ambiguous retention failed: %v", err)
	}

	name := changes[0].Name
	initial := changes[0].Incident
	in.Incidents, _ = reconciler.Existing(t.Context(), in.Identities[in.Anchor])
	d.Actions[0].Operation, d.Actions[0].IncidentID = "update", name
	d.Evidence = evidenceFixture(in, "Different description alone")

	changes, err = reconciler.Apply(t.Context(), in, d, func() bool { return true })
	if err != nil || len(changes) != 0 || !reflect.DeepEqual(repo.Records[name].Value, initial) {
		t.Fatal("description churn changed an incident")
	}

	for _, operation := range []string{"update", "resolve", "update"} {
		in.Incidents, _ = reconciler.Existing(t.Context(), in.Identities[in.Anchor])
		in.RecoveryEligible = true
		d.Actions[0].Operation, d.Actions[0].Severity = operation, "high"
		d.Evidence = evidenceFixture(in, operation+" description")
		before := repo.Records[name].Value.Lifecycle.Revision

		changes, err = reconciler.Apply(t.Context(), in, d, func() bool { return true })
		if err != nil || len(changes) != 1 || repo.Records[name].Value.Lifecycle.Revision != before+1 ||
			!reflect.DeepEqual(repo.Records[name].Value.Evidence, d.Evidence) {
			t.Fatalf("material %s lost reviewed evidence: %v", operation, err)
		}
	}

	changes, err = reconciler.Apply(t.Context(), in, d, func() bool { return false })
	if err == nil || len(changes) != 0 {
		t.Fatal("stale description was persisted")
	}
}

// TestDescriptionSerializedBoundary checks exact JSON-byte admission, oversized text and malformed sections.
func TestDescriptionSerializedBoundary(t *testing.T) {
	in, _ := fixture()
	evidence := evidenceFixture(in, "x")
	second := evidence.Descriptions[0]
	second.Resource.Ref, second.Resource.Name, second.EvidenceID = "second-ref", "second", "kubectl-describe-2"
	evidence.Descriptions = append(evidence.Descriptions, second)
	evidence.Descriptions[0].Output.Text = strings.Repeat("x", MaxDescribeBytes)
	evidence.Descriptions[1].Output.Text = "x"
	data, _ := json.Marshal(evidence)

	evidence.Descriptions[1].Output.Text = strings.Repeat("x", MaxDescriptionBytes-len(data)+1)
	if err := ValidateEvidence(evidence); err != nil {
		t.Fatalf("exact limit rejected: %v", err)
	}

	evidence.Descriptions[1].Output.Text += "x"
	if ValidateEvidence(evidence) == nil {
		t.Fatal("one-byte overflow accepted")
	}

	for _, change := range []func(*Evidence){
		func(e *Evidence) { e.Format = "unknown" },
		func(e *Evidence) { e.Format = "describe-style/v1" },
		func(e *Evidence) { e.Descriptions[0].Output.Status = "healthy" },
		func(e *Evidence) { e.Descriptions[0].ObservedAt = time.Time{} },
		func(e *Evidence) { e.Descriptions = append(e.Descriptions, e.Descriptions[0]) },
		func(e *Evidence) { e.OmittedResources = 50 },
		func(e *Evidence) { e.Descriptions[0].Output.Text = string([]byte{0xff}) },
		func(e *Evidence) { e.Descriptions[0].Source = "unapproved" },
		func(e *Evidence) { e.Descriptions[0].Output.Text = strings.Repeat("x", MaxDescribeBytes+1) },
	} {
		evidence = evidenceFixture(in, "text")
		change(evidence)

		if ValidateEvidence(evidence) == nil {
			t.Fatal("malformed evidence accepted")
		}
	}
}

// TestDescriptionWriteConfirmation checks create/patch retention and detects schema pruning without new RBAC.
func TestDescriptionWriteConfirmation(t *testing.T) {
	for _, patch := range []bool{false, true} {
		for _, prune := range []string{"", "evidence", "output", "source"} {
			in, d := fixture()
			in.Evidence = evidenceFixture(in, "Approved conditions")
			identity := Identity{Observer: "fixture", Anchor: in.Identities[in.Anchor], Family: d.Actions[0].Family}

			record, err := next(nil, identity, d.Actions[0], in, "fixture", time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}

			if patch {
				record.Version, record.UID = "1", "incident-uid"
			}

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var object map[string]any
				if patch {
					var operations []map[string]any
					if r.Method != "PATCH" || json.NewDecoder(r.Body).Decode(&operations) != nil {
						t.Error("expected conditional patch")
					}

					if operations[0]["path"] != "/metadata/resourceVersion" || operations[1]["path"] != "/metadata/uid" {
						t.Error("lost write preconditions")
					}

					object = map[string]any{"apiVersion": APIVersion, "kind": "Incident",
						"metadata": map[string]any{"name": record.Name, "labels": map[string]any{"app.kubernetes.io/managed-by": "kubescout"}}, "spec": operations[2]["value"]}
				} else if r.Method != "POST" || json.NewDecoder(r.Body).Decode(&object) != nil {
					t.Error("expected Incident create")
				}

				if prune == "evidence" {
					delete(object["spec"].(map[string]any), "evidence")
				} else if prune != "" {
					evidence := object["spec"].(map[string]any)["evidence"].(map[string]any)
					delete(evidence["descriptions"].([]any)[0].(map[string]any), prune)
				}

				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(object)
			}))

			client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}

			store := Kubernetes{Client: client, Observer: "fixture", Timeout: time.Second}
			err = store.Save(t.Context(), record)

			server.Close()

			if (err != nil) != (prune != "") {
				t.Fatalf("patch=%t prune=%s error=%v", patch, prune, err)
			}
		}
	}
}
