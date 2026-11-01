/*
Purpose: Test controller correlation and retention of recovery candidates.
Flow: Offline projected snapshots verify that replacement replicas share an
anchor and unresolved records remain investigable after signals disappear,
without treating absent evidence as an incident resolution.
*/
package incident

import (
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
)

// TestReplicaReplacementSharesOwnerIdentity keeps replacement Pods correlated
// through their ReplicaSet to one Deployment.
func TestReplicaReplacementSharesOwnerIdentity(t *testing.T) {
	deployment := health.Object{Resource: health.Resource{Kind: "Deployment", Namespace: "apps", Name: "app", UID: "deployment-uid"}, Desired: 1, Available: 1, Updated: 1}
	rs := health.Object{Resource: health.Resource{Kind: "ReplicaSet", Namespace: "apps", Name: "replicas", UID: "rs-uid"}, Owner: deployment.Resource}
	pod := health.Object{Resource: health.Resource{Kind: "Pod", Namespace: "apps", Name: "one", UID: "one-uid"}, Owner: rs.Resource, Phase: "Failed"}

	first, err := BuildInvestigationContexts(health.Snapshot{At: time.Now(), Objects: []health.Object{deployment, rs, pod}}, health.DefaultPolicy(), nil, 10)
	if err != nil {
		t.Fatal(err)
	}

	pod.Name, pod.UID = "two", "two-uid"

	second, err := BuildInvestigationContexts(health.Snapshot{At: time.Now(), Objects: []health.Object{deployment, rs, pod}}, health.DefaultPolicy(), nil, 10)
	if err != nil {
		t.Fatal(err)
	}

	key := Ref(deployment.Resource)
	if len(first) != 1 || len(second) != 1 || first[key].Anchor != second[key].Anchor {
		t.Fatal("replica replacement created another correlation scope")
	}
}

// TestRecoverySurvivesSignalDisappearance retains unresolved incidents for review after their signals vanish.
func TestRecoverySurvivesSignalDisappearance(t *testing.T) {
	anchor := health.Resource{Kind: "Pod", Namespace: "apps", Name: "gone", UID: "old-uid"}
	open := []Record{{Value: Incident{Identity: Identity{Observer: "fixture", Anchor: anchor, Family: "container-failure"}, Resources: []health.Resource{anchor}, Lifecycle: Lifecycle{State: "open"}}}}

	investigations, err := BuildInvestigationContexts(health.Snapshot{At: time.Now()}, health.DefaultPolicy(), open, 10)
	if err != nil || len(investigations) != 1 || Active(investigations[Ref(anchor)]) {
		t.Fatal("missing signals stranded an open incident")
	}
}
