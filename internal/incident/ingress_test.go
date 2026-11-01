/*
Purpose: Test Ingress backend context without changing stable incident anchors.
Flow: Offline Ingress/Service/EndpointSlice/Pod snapshots provide bounded
related handles and private fingerprints. Backend readiness stays Service
evidence; missing configuration and controller events stay Ingress evidence.
*/
package incident

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
)

// ingressSnapshot supplies a missing route port with related Service, endpoint and owned workload evidence.
func ingressSnapshot() health.Snapshot {
	ingress := health.Object{Resource: health.Resource{Kind: "Ingress", Namespace: "apps", Name: "web", UID: "ingress"},
		Ingress: health.IngressState{Routes: []health.IngressRoute{{Host: "web.example", Path: "/", Service: "web", PortNumber: 81}}}}
	service := health.Object{Resource: health.Resource{Kind: "Service", Namespace: "apps", Name: "web", UID: "service"},
		Selector: map[string]string{"app": "web"}, Ports: []health.Port{{Number: 80}}}
	controller := health.Object{Resource: health.Resource{Kind: "Deployment", Namespace: "apps", Name: "web", UID: "deployment"},
		Desired: 1, Available: 1, Updated: 1}
	pod := health.Object{Resource: health.Resource{Kind: "Pod", Namespace: "apps", Name: "web-0", UID: "pod"},
		Owner: controller.Resource, Labels: map[string]string{"app": "web"}, Phase: "Failed"}
	slice := health.Object{Resource: health.Resource{Kind: "EndpointSlice", Namespace: "apps", Name: "web-a", UID: "slice"},
		ServiceName: "web", Owner: service.Resource, Endpoints: []health.Endpoint{{Target: pod.Resource, Ready: false}}}

	return health.Snapshot{At: time.Now(), Objects: []health.Object{ingress, service, controller, pod, slice}}
}

// TestIngressContextPreservesAnchors checks configuration, Service readiness and workload signals remain in their own groups.
func TestIngressContextPreservesAnchors(t *testing.T) {
	snapshot := ingressSnapshot()

	contexts, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 10)
	if err != nil || len(contexts) != 3 {
		t.Fatalf("unexpected Ingress/backend candidates: %+v %v", contexts, err)
	}

	for _, index := range []int{0, 1} {
		resource := snapshot.Objects[index].Resource

		in := contexts[Ref(resource)]
		if in.Anchor != Ref(resource) || len(in.Resources) != 5 || len(in.Signals) != 1 {
			t.Fatal("Ingress context changed an anchor, duplicated readiness signals or omitted related handles")
		}
	}

	snapshot.Objects[0].Ingress.Routes[0].PortNumber = 80

	next, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 10)
	if err != nil || len(next) != 2 {
		t.Fatal("healthy Ingress configuration duplicated backend failure evidence")
	}

	if _, ok := next[Ref(snapshot.Objects[1].Resource)].Identities[Ref(snapshot.Objects[0].Resource)]; !ok {
		t.Fatal("Service investigation lost Ingress routing context")
	}

	snapshot.Objects[0].Namespace = "other"

	unrelated, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 10)
	if err != nil || len(unrelated[Ref(snapshot.Objects[0].Resource)].Resources) != 1 {
		t.Fatal("Ingress context followed a Service across namespaces")
	}
}

// TestIngressWarningAndFingerprint handles controller warnings and route changes without probing network state.
func TestIngressWarningAndFingerprint(t *testing.T) {
	snapshot := ingressSnapshot()
	snapshot.Objects[0].Ingress.Routes[0].PortNumber = 80
	event := health.Object{Resource: health.Resource{Kind: "Event", Namespace: "apps", Name: "rejected", UID: "event"},
		Target: snapshot.Objects[0].Resource, EventTime: snapshot.At, EventReason: "Rejected"}
	snapshot.Objects = append(snapshot.Objects, event)

	contexts, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 10)
	if err != nil {
		t.Fatal(err)
	}

	key := Ref(snapshot.Objects[0].Resource)

	in := contexts[key]
	if len(in.Signals) != 1 || in.Signals[0].Rule != "event.warning" || in.Anchor != key {
		t.Fatal("controller Warning was not associated with the collected Ingress")
	}

	baseline := Fingerprint(in)

	for _, edit := range []func(*health.Object){
		func(o *health.Object) { o.Ingress.Routes[0].Service = "other" },
		func(o *health.Object) { o.Ingress.Routes[0].PortNumber = 81 },
		func(o *health.Object) { o.Ingress.Routes[0].Path = "/different" },
		func(o *health.Object) { o.Ingress.Class = "other" },
		func(o *health.Object) { o.Deleting = true },
	} {
		o := snapshot.Objects[0]
		o.Ingress.Routes = slices.Clone(o.Ingress.Routes)
		edit(&o)
		in.Objects[key] = o

		if Fingerprint(in) == baseline {
			t.Fatal("material Ingress configuration change did not invalidate its investigation")
		}
	}
}

// TestIngressContextBounds refuses oversized backend contexts rather than returning success-shaped partial evidence.
func TestIngressContextBounds(t *testing.T) {
	snapshot := ingressSnapshot()
	for i := 0; i < 50; i++ {
		pod := snapshot.Objects[3]
		pod.Name, pod.UID = fmt.Sprintf("web-%d", i), fmt.Sprintf("web-%d", i)
		snapshot.Objects = append(snapshot.Objects, pod)
	}

	if _, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 100); err == nil {
		t.Fatal("oversized Ingress backend context was accepted")
	}
}
