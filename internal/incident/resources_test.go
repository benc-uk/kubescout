package incident

import (
	"testing"
	"time"

	"k8s-ops-agent/internal/health"
)

func TestStatefulAndDaemonReplicaCorrelation(t *testing.T) {
	for _, kind := range []string{"StatefulSet", "DaemonSet"} {
		controller := health.Object{Resource: health.Resource{Kind: kind, Namespace: "apps", Name: "app", UID: "controller"}}
		pod := health.Object{Resource: health.Resource{Kind: "Pod", Namespace: "apps", Name: "one", UID: "one"}, Owner: controller.Resource, Phase: "Failed"}
		second := pod
		second.Name, second.UID = "two", "two"
		first, err := Inputs(health.Snapshot{At: time.Now(), Objects: []health.Object{controller, pod, second}}, health.DefaultPolicy(), nil, 10)
		if err != nil || len(first) != 1 || len(first[Ref(controller.Resource)].Signals) != 2 {
			t.Fatalf("%s replicas not coalesced: %+v %v", kind, first, err)
		}
		pod.Name, pod.UID = "replacement", "replacement"
		next, err := Inputs(health.Snapshot{At: time.Now(), Objects: []health.Object{controller, pod}}, health.DefaultPolicy(), nil, 10)
		if err != nil || next[Ref(controller.Resource)].Anchor != first[Ref(controller.Resource)].Anchor {
			t.Fatal("replica replacement changed controller identity")
		}
	}
}

func TestServiceIdentitySurvivesEndpointSliceReplacement(t *testing.T) {
	service := health.Object{Resource: health.Resource{Kind: "Service", Namespace: "apps", Name: "web", UID: "service"},
		Created: time.Now().Add(-time.Hour), Selector: map[string]string{"app": "web"}}
	slice := health.Object{Resource: health.Resource{Kind: "EndpointSlice", Namespace: "apps", Name: "web-a", UID: "one"},
		Owner: service.Resource, ServiceName: "web"}
	for _, uid := range []string{"one", "replacement"} {
		slice.UID = uid
		inputs, err := Inputs(health.Snapshot{At: time.Now(), Objects: []health.Object{service, slice}}, health.DefaultPolicy(), nil, 10)
		if err != nil || len(inputs) != 1 || inputs[Ref(service.Resource)].Anchor != Ref(service.Resource) {
			t.Fatal("EndpointSlice replacement changed Service identity")
		}
	}
}
