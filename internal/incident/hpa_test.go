/*
Purpose: Test HPA target context and semantic freshness without ownership changes.
Flow: Offline HPA/controller/Pod snapshots keep standalone and workload anchors
stable, reject unrelated namespaces and enforce context bounds. Metric sample
and timestamp churn do not invalidate no-action suppression by themselves.
*/
package incident

import (
	"fmt"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
)

// hpaSnapshot supplies an inactive HPA and its failed Deployment Pod for context fixtures.
func hpaSnapshot() health.Snapshot {
	target := health.Object{Resource: health.Resource{Kind: "Deployment", Namespace: "apps", Name: "web", UID: "deployment"},
		Desired: 1, Available: 1, Updated: 1}
	hpa := health.Object{Resource: health.Resource{Kind: "HorizontalPodAutoscaler", Namespace: "apps", Name: "web", UID: "hpa"},
		Conditions: []health.Condition{{Type: "ScalingActive", Status: "False", Reason: "FailedGetResourceMetric"}},
		HPA:        health.HPAState{TargetVersion: "apps/v1", TargetKind: "Deployment", TargetName: "web", Min: 1, Max: 5}}
	pod := health.Object{Resource: health.Resource{Kind: "Pod", Namespace: "apps", Name: "web-0", UID: "pod"},
		Owner: target.Resource, Phase: "Failed"}

	return health.Snapshot{At: time.Now(), Objects: []health.Object{hpa, target, pod}}
}

// TestHPATargetContextPreservesAnchors verifies bidirectional context without merging autoscaler and workload identities.
func TestHPATargetContextPreservesAnchors(t *testing.T) {
	snapshot := hpaSnapshot()
	hpa, target := snapshot.Objects[0], snapshot.Objects[1]

	contexts, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 10)
	if err != nil || len(contexts) != 2 {
		t.Fatalf("unexpected HPA/workload candidates: %+v %v", contexts, err)
	}

	for _, o := range []health.Object{hpa, target} {
		in := contexts[Ref(o.Resource)]
		if in.Anchor != Ref(o.Resource) || len(in.Resources) != 3 || len(in.Signals) != 1 {
			t.Fatal("scale-target relationship changed incident anchor or mixed signals")
		}
	}

	snapshot.Objects[1].UID = "replacement"
	snapshot.Objects[2].Owner = snapshot.Objects[1].Resource

	replacement, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 10)
	if err != nil || replacement[Ref(hpa.Resource)].Anchor != Ref(hpa.Resource) {
		t.Fatal("target replacement changed HPA identity")
	}

	if _, ok := replacement[Ref(hpa.Resource)].Identities[Ref(snapshot.Objects[1].Resource)]; !ok {
		t.Fatal("HPA did not use the current target incarnation")
	}

	snapshot.Objects[1].Namespace = "other"
	snapshot.Objects[2].Namespace, snapshot.Objects[2].Owner = "other", snapshot.Objects[1].Resource

	unrelated, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 10)
	if err != nil || len(unrelated[Ref(hpa.Resource)].Resources) != 1 {
		t.Fatal("HPA target context escaped its namespace")
	}
}

// TestHPAFingerprintIgnoresObservationNoise tracks configuration/condition changes rather than raw metric and time churn.
func TestHPAFingerprintIgnoresObservationNoise(t *testing.T) {
	o := hpaSnapshot().Objects[0]
	o.HPA.Metrics = []health.Metric{{Type: "Resource", Name: "cpu", TargetType: "Utilization", Utilization: new(int32(70))}}
	o.HPA.CurrentMetrics = []health.Metric{{Type: "Resource", Name: "cpu", Utilization: new(int32(90))}}
	key := Ref(o.Resource)
	in := InvestigationContext{Anchor: key, Resources: []Resource{{Ref: key}}, Objects: map[string]health.Object{key: o}}
	baseline := Fingerprint(in)
	o.HPA.CurrentMetrics[0].Utilization = new(int32(80))
	o.HPA.LastScale = new(time.Now())
	o.Conditions[0].Transition = time.Now()
	in.Objects[key] = o

	if Fingerprint(in) != baseline {
		t.Fatal("HPA metric samples or timestamps invalidated no-action suppression")
	}

	for _, edit := range []func(*health.Object){
		func(o *health.Object) { o.HPA.Max = 10 },
		func(o *health.Object) { o.HPA.TargetName = "other" },
		func(o *health.Object) { o.HPA.ScaleDown = &health.ScalingRules{Stabilization: new(int32(300))} },
		func(o *health.Object) { o.HPA.Metrics[0].Utilization = new(int32(60)) },
		func(o *health.Object) { o.Conditions = []health.Condition{{Type: "ScalingActive", Status: "True"}} },
		func(o *health.Object) { o.Deleting = true },
	} {
		changed := o
		changed.HPA = o.HPA.Clone()
		edit(&changed)
		in.Objects[key] = changed

		if Fingerprint(in) == baseline {
			t.Fatal("material HPA configuration or condition change did not invalidate evidence")
		}
	}
}

// TestHPAContextBounds refuses oversized target replica context rather than returning partial evidence.
func TestHPAContextBounds(t *testing.T) {
	snapshot := hpaSnapshot()
	for i := 0; i < 50; i++ {
		pod := snapshot.Objects[2]
		pod.Name, pod.UID = fmt.Sprintf("pod-%d", i), fmt.Sprintf("pod-%d", i)
		snapshot.Objects = append(snapshot.Objects, pod)
	}

	if _, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 100); err == nil {
		t.Fatal("oversized HPA target context accepted")
	}
}
