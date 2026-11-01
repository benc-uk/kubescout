/*
Purpose: Test storage context, stable identities and semantic freshness.
Flow: Offline PVC/PV/Pod/controller fixtures verify mutual binding and scoped
consumer context without replacing workload or storage anchors. Fingerprints
track binding and capacity changes while ignoring condition timestamp churn.
*/
package incident

import (
	"fmt"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
)

// storageSnapshot supplies related storage, a failed controller Pod and an unrelated same-named claim.
func storageSnapshot() health.Snapshot {
	claim := health.Object{Resource: health.Resource{Kind: "PersistentVolumeClaim", Namespace: "apps", Name: "data", UID: "claim"},
		Phase: "Lost", Storage: health.Storage{VolumeName: "disk", Capacity: "1Gi"}}
	volume := health.Object{Resource: health.Resource{Kind: "PersistentVolume", Name: "disk", UID: "volume"},
		Phase: "Failed", Storage: health.Storage{Claim: claim.Resource, Capacity: "1Gi"}}
	controller := health.Object{Resource: health.Resource{Kind: "StatefulSet", Namespace: "apps", Name: "db", UID: "controller"}}
	pod := health.Object{Resource: health.Resource{Kind: "Pod", Namespace: "apps", Name: "db-0", UID: "pod"},
		Phase: "Failed", Owner: controller.Resource, Claims: []string{"data"}}
	other := health.Object{Resource: health.Resource{Kind: "PersistentVolumeClaim", Namespace: "other", Name: "data", UID: "other"}, Phase: "Bound"}

	return health.Snapshot{At: time.Now(), Objects: []health.Object{claim, volume, controller, pod, other}}
}

// TestStorageContextKeepsStableAnchors verifies verified bindings and consumers are context, not ownership.
func TestStorageContextKeepsStableAnchors(t *testing.T) {
	snapshot := storageSnapshot()

	contexts, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 10)
	if err != nil || len(contexts) != 3 {
		t.Fatalf("unexpected storage/workload candidates: %d %v", len(contexts), err)
	}

	for _, o := range snapshot.Objects[:3] {
		in := contexts[Ref(o.Resource)]
		if in.Anchor != Ref(o.Resource) || len(in.Resources) != 4 {
			t.Fatalf("storage relationship changed anchor or context: %+v", in)
		}

		for _, r := range in.Identities {
			if r.Namespace == "other" {
				t.Fatal("same-named cross-namespace claim included")
			}
		}
	}

	snapshot.Objects[1].Storage.Claim.UID = "old-claim"

	next, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 10)
	if err != nil {
		t.Fatal(err)
	}

	pv := snapshot.Objects[1].Resource
	if next[Ref(pv)].Anchor != Ref(pv) || len(next[Ref(pv)].Resources) != 1 {
		t.Fatal("stale claimRef established binding or changed PV identity")
	}

	claim := snapshot.Objects[0].Resource
	if _, included := next[Ref(claim)].Identities[Ref(pv)]; included {
		t.Fatal("stale PV binding included in claim context")
	}

	snapshot.Objects[0].UID = "replacement-claim"

	rebound, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 10)
	if err != nil || rebound[Ref(pv)].Anchor != Ref(pv) {
		t.Fatal("PVC replacement changed stable PV anchor")
	}
}

// TestStorageFingerprintTracksMaterialFacts rejects stale bindings/capacities without condition timestamp noise.
func TestStorageFingerprintTracksMaterialFacts(t *testing.T) {
	snapshot := storageSnapshot()
	o := snapshot.Objects[0]
	o.Phase = "Bound"
	o.Conditions = []health.Condition{{Type: "Resizing", Status: "True", Transition: time.Now()}}
	ref := Ref(o.Resource)
	in := InvestigationContext{Anchor: ref, Resources: []Resource{{Ref: ref}}, Objects: map[string]health.Object{ref: o}}
	baseline := Fingerprint(in)

	o.Conditions[0].Transition = time.Now().Add(time.Hour)

	in.Objects[ref] = o
	if Fingerprint(in) != baseline {
		t.Fatal("condition timestamp churn invalidated no-action cache")
	}

	for _, edit := range []func(*health.Object){
		func(o *health.Object) { o.Storage.Capacity = "2Gi" },
		func(o *health.Object) { o.Storage.Requested = "3Gi" },
		func(o *health.Object) { o.Storage.VolumeName = "different" },
		func(o *health.Object) { o.Storage.Claim.UID = "different" },
		func(o *health.Object) { o.Conditions = []health.Condition{{Type: "Resizing", Status: "False"}} },
		func(o *health.Object) { o.Deleting = true },
		func(o *health.Object) { o.Claims = []string{"different"} },
	} {
		changed := o
		edit(&changed)
		in.Objects[ref] = changed

		if Fingerprint(in) == baseline {
			t.Fatal("material storage change did not invalidate evidence revision")
		}
	}
}

// TestStorageContextLimits refuses oversized related consumers rather than returning partial evidence.
func TestStorageContextLimits(t *testing.T) {
	snapshot := storageSnapshot()
	for i := 0; i < 50; i++ {
		pod := snapshot.Objects[3]
		pod.Name, pod.UID = fmt.Sprintf("pod-%d", i), fmt.Sprintf("uid-%d", i)
		snapshot.Objects = append(snapshot.Objects, pod)
	}

	if _, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 100); err == nil {
		t.Fatal("oversized storage consumer context accepted")
	}
}
