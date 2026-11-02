/*
Purpose: Test pre-assessment description delivery and host-owned Incident evidence.
Flow: Offline Kubernetes and actual SDK HTTP fixtures verify exact description
retention, citations and no-action without live resources or model calls.
Native operational labels and messages reach the real SDK and storage unchanged,
without duplicate typed state, event sections or citations.
*/
package analysis

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/kube"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// descriptionKubernetes serves approved Pod facts and empty scoped Warning events for offline model tests.
// The generated REST configuration also supports independent native describe reads.
func descriptionKubernetes(t *testing.T) *kube.Connection {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/namespaces/apps/pods/bad":
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"bad","namespace":"apps","uid":"private-uid","resourceVersion":"1","labels":{"app":"token-service"}},"status":{"phase":"Failed","message":"authorization denied; secret registry unavailable; token refresh failed"}}`)
		case "/api/v1/namespaces/apps/events":
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"EventList","metadata":{"resourceVersion":"1"},"items":[]}`)
		default:
			t.Errorf("unexpected description path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}

	return &kube.Connection{Interface: client, DescribeConfig: &rest.Config{Host: server.URL}}
}

// requestEvidence finds the initial serialized investigation context in an SDK request.
func requestEvidence(value any) *incident.Evidence {
	switch v := value.(type) {
	case map[string]any:
		for _, child := range v {
			if evidence := requestEvidence(child); evidence != nil {
				return evidence
			}
		}
	case []any:
		for _, child := range v {
			if evidence := requestEvidence(child); evidence != nil {
				return evidence
			}
		}
	case string:
		var in incident.InvestigationContext
		if json.Unmarshal([]byte(v), &in) == nil {
			return in.Evidence
		}
	}

	return nil
}

// TestDescriptionDecisionRetention proves AI sees the exact persisted text and cannot author attachments.
func TestDescriptionDecisionRetention(t *testing.T) {
	in := inputFixture()

	var seen *incident.Evidence

	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}

		seen = requestEvidence(request)
		if seen == nil {
			t.Error("model did not receive prepared evidence")
		}

		decision := incident.Decision{Outcome: "decisions", Reason: "Observed failure", Actions: []incident.Action{{
			Operation: "raise", Family: "container-failure", ResourceRefs: []string{in.Anchor},
			Title: "Pod failed", Severity: "high", Impact: "unavailable", Diagnosis: "pod-failed",
			Explanation: "Pod phase is Failed", EvidenceIDs: []string{"kubectl-describe-1"},
			Recommendations: []string{"Review workload configuration"}, Uncertainty: "Cause not established", MaterialChange: true,
		}}}

		data, err := json.Marshal(decision)
		if err != nil {
			t.Error(err)
		}

		respond(w, textResponse(string(data)))
	}))
	defer model.Close()

	client := descriptionKubernetes(t)
	a := Analyzer{Config: config.Config{
		Endpoint: model.URL + "/openai/v1/", APIKey: "fixture", Model: "fixture", MaxInputChars: 20000,
		MaxOutputTokens: 2000, MaxToolCalls: 64, CallsPerHour: 60, RequestTimeout: time.Second, InvestigationTimeout: 5 * time.Second,
	}, Kubernetes: client, DescribeConfig: client.DescribeConfig}

	decision, err := a.Investigate(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}

	if seen == nil || seen.Descriptions[0].Output.Status != "captured" {
		t.Fatal("model did not receive native describe output")
	}

	for _, wanted := range []string{"app=token-service", "authorization denied; secret registry unavailable; token refresh failed"} {
		if !strings.Contains(seen.Descriptions[0].Output.Text, wanted) {
			t.Fatalf("operational detail %q did not reach the SDK", wanted)
		}
	}

	repo := &incident.Memory{}
	reconciler := incident.Reconciler{Repository: repo, Observer: "fixture", Model: "fixture"}

	changes, err := reconciler.Apply(t.Context(), in, decision, func() bool { return true })
	if err != nil || len(changes) != 1 || !reflect.DeepEqual(seen, changes[0].Incident.Evidence) {
		t.Fatalf("AI and persisted description differ: %v", err)
	}

	data, err := json.Marshal(changes[0].Incident.Evidence)
	if err != nil || !strings.Contains(string(data), `"descriptions"`) || strings.Contains(string(data), `"snapshots"`) ||
		strings.Contains(string(data), `"state"`) || strings.Contains(string(data), `"warning_events"`) ||
		strings.Contains(string(data), `"describe"`) || strings.Contains(string(data), "resource-snapshot-") {
		t.Fatalf("duplicate evidence reached the model or Incident: %s", data)
	}

	if _, err := Validate(`{"outcome":"no_action","reason":"fixture","actions":[],"evidence":{}}`, in, nil); err == nil {
		t.Fatal("model supplied host-owned evidence")
	}

	changes, err = reconciler.Apply(t.Context(), in, incident.Decision{Outcome: "no_action", Reason: "unchanged", Actions: []incident.Action{}, Evidence: seen}, func() bool { return true })
	if err != nil || len(changes) != 0 || len(repo.Records) != 1 {
		t.Fatal("no-action wrote a description")
	}
}
