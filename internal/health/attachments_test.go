/*
Purpose: Test reported attachment errors and current named storage relationships.
Flow: Offline VolumeAttachment fixtures exercise operation/grace boundaries
and stale-error exclusions. Current PV/Node links are context only; tests never
mount storage, call CSI or decide incident lifecycle.
*/
package health

import (
	"testing"
	"time"
)

// TestAttachmentEvidence checks stage-aware attach/detach rules, exact grace boundaries and normal-operation exclusions.
func TestAttachmentEvidence(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	p := DefaultPolicy()

	for _, tt := range []struct {
		name, rule, severity string
		attached, deleting   bool
		attach, detach       bool
		age                  time.Duration
	}{
		{name: "normal pending"},
		{name: "old pending is not a stall detector", age: time.Hour},
		{name: "attached", attached: true},
		{name: "new attach error", attach: true, age: p.StartupGrace - time.Nanosecond, rule: "volumeattachment.attach_error", severity: "info"},
		{name: "attach grace boundary", attach: true, age: p.StartupGrace, rule: "volumeattachment.attach_error", severity: "warning"},
		{name: "stale attach error", attached: true, attach: true, age: time.Hour},
		{name: "stale detach error", attached: true, detach: true, age: time.Hour},
		{name: "normal deletion", attached: true, deleting: true, age: time.Hour},
		{name: "new detach error", attached: true, deleting: true, detach: true, age: p.StartupGrace - time.Nanosecond, rule: "volumeattachment.detach_error", severity: "info"},
		{name: "detach grace boundary", attached: true, deleting: true, detach: true, age: p.StartupGrace, rule: "volumeattachment.detach_error", severity: "warning"},
		{name: "detached stale error", deleting: true, detach: true, age: time.Hour},
		{name: "deletion ignores old attach error", deleting: true, attach: true, age: time.Hour},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o := Object{Resource: Resource{Kind: "VolumeAttachment", Name: "disk-node", UID: "attachment"},
				Created: at.Add(-tt.age), Deleting: tt.deleting,
				Attachment: AttachmentState{Attached: tt.attached, DeletingSince: at.Add(-tt.age),
					AttachError: AttachmentError{Present: tt.attach}, DetachError: AttachmentError{Present: tt.detach}}}
			if tt.deleting {
				o.Created = at.Add(-24 * time.Hour)
			}

			findings := Evaluate(Snapshot{At: at, Objects: []Object{o}}, p)
			if tt.rule == "" {
				if len(findings) != 0 {
					t.Fatalf("normal attachment state produced evidence: %+v", findings)
				}
			} else if len(findings) != 1 || findings[0].Rule != tt.rule || findings[0].Severity != tt.severity || findings[0].Resource != o.Resource {
				t.Fatalf("unexpected attachment evidence: %+v", findings)
			}
		})
	}
}

// TestAttachmentRelationships checks cluster scope and named current PV/Node context without authorizing inline source reads.
func TestAttachmentRelationships(t *testing.T) {
	attachment := Object{Resource: Resource{Kind: "VolumeAttachment"}, Attachment: AttachmentState{VolumeName: "disk", NodeName: "node"}}
	volume := Object{Resource: Resource{Kind: "PersistentVolume", Name: "disk", UID: "pv"}}
	node := Object{Resource: Resource{Kind: "Node", Name: "node", UID: "node"}}

	if !ClusterScoped("VolumeAttachment") || ClusterScoped("PersistentVolumeClaim") ||
		!MatchesAttachmentVolume(attachment, volume) || !MatchesAttachmentNode(attachment, node) {
		t.Fatal("cluster attachment target context missing")
	}

	volume.Namespace = "apps"

	node.Name = "other"
	if MatchesAttachmentVolume(attachment, volume) || MatchesAttachmentNode(attachment, node) {
		t.Fatal("attachment matched out-of-scope targets")
	}

	volume.Namespace, attachment.Attachment.Inline = "", true
	if MatchesAttachmentVolume(attachment, volume) {
		t.Fatal("inline attachment fabricated a PV link")
	}
}
