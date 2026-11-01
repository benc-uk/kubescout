/*
Purpose: Test attachment/PV/Node context and stable incident identities.
Flow: Offline snapshots link named current targets and verified scoped claims
to consumers without changing ownership. Timestamp churn is suppressed while
operation, identity and numeric error-code changes invalidate investigation facts.
*/
package incident

import (
	"fmt"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
)

// attachmentSnapshot supplies a failed attachment, scoped bound claim and owned failed consumer.
func attachmentSnapshot() health.Snapshot {
	claim := health.Object{Resource: health.Resource{Kind: "PersistentVolumeClaim", Namespace: "apps", Name: "data", UID: "claim"},
		Phase: "Bound", Storage: health.Storage{VolumeName: "disk"}}
	volume := health.Object{Resource: health.Resource{Kind: "PersistentVolume", Name: "disk", UID: "pv"},
		Phase: "Bound", Storage: health.Storage{Claim: claim.Resource}}
	node := health.Object{Resource: health.Resource{Kind: "Node", Name: "node", UID: "node"},
		Conditions: []health.Condition{{Type: "Ready", Status: "True"}}}
	attachment := health.Object{Resource: health.Resource{Kind: "VolumeAttachment", Name: "disk-node", UID: "attachment"},
		Attachment: health.AttachmentState{VolumeName: "disk", NodeName: "node", AttachError: health.AttachmentError{Present: true}}}
	controller := health.Object{Resource: health.Resource{Kind: "Job", Namespace: "apps", Name: "worker", UID: "job"}}
	pod := health.Object{Resource: health.Resource{Kind: "Pod", Namespace: "apps", Name: "worker-0", UID: "pod"},
		Owner: controller.Resource, Claims: []string{"data"}, Phase: "Failed"}
	other := claim
	other.Namespace, other.UID = "other", "other-claim"

	return health.Snapshot{At: time.Now(), Objects: []health.Object{attachment, volume, node, claim, controller, pod, other}}
}

// TestAttachmentContextKeepsAnchors verifies bidirectional storage context and scoped consumers without replacing attachment/workload identities.
func TestAttachmentContextKeepsAnchors(t *testing.T) {
	snapshot := attachmentSnapshot()

	contexts, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 100)
	if err != nil || len(contexts) != 2 {
		t.Fatalf("unexpected attachment/workload contexts: %+v %v", contexts, err)
	}

	for _, index := range []int{0, 4} {
		o := snapshot.Objects[index]

		in := contexts[Ref(o.Resource)]
		if in.Anchor != Ref(o.Resource) || len(in.Resources) != 6 || len(in.Signals) != 1 {
			t.Fatal("attachment relationship changed ownership, mixed signals or omitted target context")
		}

		for _, resource := range in.Identities {
			if resource.Namespace == "other" {
				t.Fatal("unrelated claim namespace entered attachment context")
			}
		}
	}

	snapshot.Objects[1].UID = "replacement-pv"
	next, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 100)

	key := Ref(snapshot.Objects[0].Resource)
	if err != nil || next[key].Anchor != key {
		t.Fatal("named target replacement changed attachment identity")
	}

	if _, ok := next[key].Identities[Ref(snapshot.Objects[1].Resource)]; !ok {
		t.Fatal("attachment retained a stale PV incarnation")
	}
}

// TestAttachmentFingerprintIgnoresErrorTimes tracks operation/code changes rather than driver retry timestamp churn.
func TestAttachmentFingerprintIgnoresErrorTimes(t *testing.T) {
	o := attachmentSnapshot().Objects[0]
	key := Ref(o.Resource)
	in := InvestigationContext{Anchor: key, Resources: []Resource{{Ref: key}}, Objects: map[string]health.Object{key: o}}
	baseline := Fingerprint(in)
	o.Attachment.AttachError.At = time.Now()
	o.Attachment.DetachError.At = time.Now()
	in.Objects[key] = o

	if Fingerprint(in) != baseline {
		t.Fatal("error timestamp churn invalidated attachment no-action suppression")
	}

	for _, edit := range []func(*health.Object){
		func(o *health.Object) { o.Attachment.Attached = true },
		func(o *health.Object) { o.Attachment.NodeName = "other" },
		func(o *health.Object) { o.Attachment.VolumeName = "other" },
		func(o *health.Object) { o.Attachment.AttachError.HasCode, o.Attachment.AttachError.Code = true, 9 },
		func(o *health.Object) { o.Attachment.AttachError.Present = false },
		func(o *health.Object) { o.Deleting = true },
	} {
		changed := o
		edit(&changed)
		in.Objects[key] = changed

		if Fingerprint(in) == baseline {
			t.Fatal("attachment state change did not invalidate evidence revision")
		}
	}
}

// TestAttachmentContextBounds rejects oversized scoped consumer groups without exposing partial evidence.
func TestAttachmentContextBounds(t *testing.T) {
	snapshot := attachmentSnapshot()
	for i := 0; i < 50; i++ {
		pod := snapshot.Objects[5]
		pod.Name, pod.UID = fmt.Sprintf("consumer-%d", i), fmt.Sprintf("consumer-%d", i)
		snapshot.Objects = append(snapshot.Objects, pod)
	}

	if _, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 100); err == nil {
		t.Fatal("oversized attachment consumer context accepted")
	}
}
