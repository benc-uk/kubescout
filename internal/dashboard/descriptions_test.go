/*
Purpose: Test stored description projection and bounded dashboard pagination.
Flow: Offline Incident HTTP fixtures verify optional evidence, malformed
evidence rejection and complete 1,000-record reads with maximum-sized descriptions.
The dashboard never reads workload APIs or calls a model.
Shared browser fixtures retain one native text block with operational labels,
its own formatting, timestamp and citation, without typed state or event copies.
*/
package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/incident"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// dashboardEvidence builds native text with explicit event unavailability and hostile prose.
func dashboardEvidence() *incident.Evidence {
	return &incident.Evidence{Format: incident.DescriptionFormat, OmittedResources: 2, Descriptions: []incident.ResourceDescription{{
		EvidenceID: "kubectl-describe-1", Resource: incident.Resource{Ref: "resource-ref", Kind: "Pod", Namespace: "apps", Name: "checkout"},
		Source: incident.DescribeSource, ObservedAt: time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC),
		Output: incident.DescriptionOutput{Status: "partial", Text: "Name:\tcheckout\nNamespace:\tapps\nStatus:\tRunning\nLabels:\tapp=token-service\nConditions:\n  Type   Status\n  Ready  False\nMessage:\t<img src=x onerror=alert(1)>\nEvents: [unavailable: related read incomplete or outside scope]"},
	}}}
}

// attachEvidence adds a typed description to an offline Incident object.
func attachEvidence(t *testing.T, object map[string]any, evidence *incident.Evidence) {
	t.Helper()

	value, err := runtime.DefaultUnstructuredConverter.ToUnstructured(evidence)
	if err != nil {
		t.Fatal(err)
	}

	object["spec"].(map[string]any)["evidence"] = value
}

// TestDescriptionDetailProjection checks exact native text, optional absence and obsolete/malformed evidence rejection.
func TestDescriptionDetailProjection(t *testing.T) {
	object := incidentObject(t, "description-incident")
	reader := &Reader{}

	detail, err := reader.decode(&unstructured.Unstructured{Object: object})
	if err != nil || detail.Evidence != nil {
		t.Fatal("legacy incident did not remain readable")
	}

	evidence := dashboardEvidence()
	attachEvidence(t, object, evidence)

	detail, err = reader.decode(&unstructured.Unstructured{Object: object})
	if err != nil || !reflect.DeepEqual(detail.Evidence, evidence) {
		t.Fatalf("description content changed: %v", err)
	}

	summary, _ := json.Marshal(detail.Summary)
	if strings.Contains(string(summary), "Conditions") || strings.Contains(string(summary), "evidence") {
		t.Fatal("description content bloated a list summary")
	}

	evidence.Format = "describe-style/v1"
	attachEvidence(t, object, evidence)

	if _, err := reader.decode(&unstructured.Unstructured{Object: object}); err == nil {
		t.Fatal("obsolete description format accepted")
	}

	evidence.Format = incident.DescriptionFormat
	evidence.Descriptions[0].Output.Status = "healthy"
	attachEvidence(t, object, evidence)

	if _, err := reader.decode(&unstructured.Unstructured{Object: object}); err == nil {
		t.Fatal("malformed description accepted")
	}
}

// TestDescriptionListPages exercises maximum envelopes through complete pagination or an explicit deadline.
func TestDescriptionListPages(t *testing.T) {
	evidence := dashboardEvidence()
	second := evidence.Descriptions[0]
	second.Resource.Ref, second.Resource.Name, second.EvidenceID = "second-ref", "second", "kubectl-describe-2"
	evidence.Descriptions = append(evidence.Descriptions, second)
	evidence.Descriptions[0].Output.Text = strings.Repeat("x", incident.MaxDescribeBytes)
	evidence.Descriptions[1].Output.Text = "x"
	encoded, _ := json.Marshal(evidence)

	evidence.Descriptions[1].Output.Text = strings.Repeat("x", incident.MaxDescriptionBytes-len(encoded)+1)
	if err := incident.ValidateEvidence(evidence); err != nil {
		t.Fatal(err)
	}

	pages := make([][]byte, MaxIncidents/100)
	for page := range pages {
		start := page * 100

		items := make([]map[string]any, 100)
		for i := range items {
			items[i] = incidentObject(t, fmt.Sprintf("description-%04d", start+i))
			attachEvidence(t, items[i], evidence)
		}

		next := ""
		if start+100 < MaxIncidents {
			next = strconv.Itoa(start + 100)
		}

		var err error

		pages[page], err = json.Marshal(map[string]any{
			"apiVersion": incident.APIVersion, "kind": "IncidentList",
			"metadata": map[string]any{"resourceVersion": "42", "continue": next}, "items": items,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	reader := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
		start, _ := strconv.Atoi(r.URL.Query().Get("continue"))

		w.Header().Set("Content-Type", "application/json")

		if _, err := w.Write(pages[start/100]); err != nil {
			t.Error(err)
		}
	})

	result, err := reader.List(t.Context())
	if err != nil {
		assertCode(t, err, "timeout")

		if result.Incidents != nil {
			t.Fatal("deadline returned a partial incident list")
		}

		t.Log("Maximum envelope pagination reached the fixed deadline; no partial list returned")

		return
	}

	if len(result.Incidents) != MaxIncidents {
		t.Fatalf("bounded full description pages failed: %v", err)
	}
}
