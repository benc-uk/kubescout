package runner

import (
	"k8s-ops-agent/internal/health"
	"k8s-ops-agent/internal/incident"
	"testing"
)

func TestInspectedResourceMustStillMatchBeforePublication(t *testing.T) {
	resource := health.Resource{Kind: "Pod", Namespace: "apps", Name: "target", UID: "uid"}
	ref := incident.Ref(resource)
	in := incident.Input{ToolVersions: map[string]string{ref: "10"}}
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
