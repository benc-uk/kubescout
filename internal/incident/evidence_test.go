package incident

import (
	"testing"
	"time"

	"k8s-ops-agent/internal/health"
)

func TestReplicaReplacementSharesOwnerIdentity(t *testing.T) {
	deployment := health.Object{Resource: health.Resource{Kind: "Deployment", Namespace: "apps", Name: "app", UID: "deployment-uid"}, Desired: 1, Available: 1, Updated: 1}
	rs := health.Object{Resource: health.Resource{Kind: "ReplicaSet", Namespace: "apps", Name: "replicas", UID: "rs-uid"}, Owner: deployment.Resource}
	pod := health.Object{Resource: health.Resource{Kind: "Pod", Namespace: "apps", Name: "one", UID: "one-uid"}, Owner: rs.Resource, Phase: "Failed"}
	first, err := Inputs(health.Snapshot{At: time.Now(), Objects: []health.Object{deployment, rs, pod}}, health.DefaultPolicy(), nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	pod.Name, pod.UID = "two", "two-uid"
	second, err := Inputs(health.Snapshot{At: time.Now(), Objects: []health.Object{deployment, rs, pod}}, health.DefaultPolicy(), nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	key := Ref(deployment.Resource)
	if len(first) != 1 || len(second) != 1 || first[key].Anchor != second[key].Anchor {
		t.Fatal("replica replacement created another correlation scope")
	}
}

func TestRecoverySurvivesSignalDisappearance(t *testing.T) {
	anchor := health.Resource{Kind: "Pod", Namespace: "apps", Name: "gone", UID: "old-uid"}
	open := []Record{{Value: Incident{Identity: Identity{Observer: "fixture", Anchor: anchor, Family: "container-failure"}, Resources: []health.Resource{anchor}, Lifecycle: Lifecycle{Phase: "open"}}}}
	inputs, err := Inputs(health.Snapshot{At: time.Now()}, health.DefaultPolicy(), open, 10)
	if err != nil || len(inputs) != 1 || Active(inputs[Ref(anchor)]) {
		t.Fatal("missing signals stranded an open incident")
	}
}
