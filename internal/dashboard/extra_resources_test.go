/*
Purpose: Verify stored extra-resource Incidents retain API groups in dashboard views.
Flow: Offline Incident projections and embedded asset assertions cover custom-family
filtering and group-aware display/search without reading workloads or calling AI.
*/
package dashboard

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestExtraResourceDashboard verifies custom incident identity and the embedded family filter.
func TestExtraResourceDashboard(t *testing.T) {
	obj := incidentObject(t, "certificate-incident")
	spec := obj["spec"].(map[string]any)
	identity := spec["identity"].(map[string]any)
	identity["family"] = "custom-resource-health"
	anchor := identity["anchor"].(map[string]any)
	anchor["kind"], anchor["api_group"] = "Certificate", "cert-manager.io"
	resource := spec["resources"].([]any)[0].(map[string]any)
	resource["kind"], resource["api_group"] = "Certificate", "cert-manager.io"
	reader := &Reader{}

	item, err := reader.decode(&unstructured.Unstructured{Object: obj})
	if err != nil || item.Anchor.APIGroup != "cert-manager.io" || item.Resources[0].APIGroup != "cert-manager.io" {
		t.Fatalf("dashboard lost extra-resource API identity: %v", err)
	}

	html, err := assets.ReadFile("web/index.html")
	if err != nil || !strings.Contains(string(html), `<option value="custom-resource-health">Custom resource health</option>`) {
		t.Fatal("custom-resource family filter is unavailable")
	}
}
