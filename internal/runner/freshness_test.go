/*
Purpose: Test the inspected-resource freshness check before publication.
Flow: Offline investigation and snapshot fixtures compare tool-observed
versions, rejecting changed or missing inspected resources while allowing
unrelated resource changes; no AI or Kubernetes requests are made.
*/
package runner

import (
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"testing"
)

// TestInspectedResourceMustStillMatchBeforePublication rejects changed or missing tool-inspected resources.
// Unrelated resource revisions must not invalidate the inspected evidence.
func TestInspectedResourceMustStillMatchBeforePublication(t *testing.T) {
	resource := health.Resource{Kind: "Pod", Namespace: "apps", Name: "target", UID: "uid"}
	ref := incident.Ref(resource)
	in := incident.InvestigationContext{ToolVersions: map[string]string{ref: "10"}}

	snapshot := health.Snapshot{Objects: []health.Object{
		{Resource: resource, Version: "10"},
		{Resource: health.Resource{Kind: "Pod", Namespace: "apps", Name: "unrelated", UID: "other"}, Version: "200"},
	}}
	if !toolsCurrent(in, snapshot) {
		t.Fatal("unrelated resource revision invalidated evidence")
	}

	snapshot.Objects[0].Version = "11"
	if toolsCurrent(in, snapshot) {
		t.Fatal("changed inspected evidence was accepted")
	}

	snapshot.Objects = snapshot.Objects[1:]
	if toolsCurrent(in, snapshot) {
		t.Fatal("disappeared inspected resource was accepted")
	}
}
