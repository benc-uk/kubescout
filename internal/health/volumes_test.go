/*
Purpose: Test storage evidence and verified claim relationships.
Flow: Offline PVC/PV/Pod projections exercise grace boundaries, failure states,
normal reclamation/resizing and UID-safe links. Findings remain private evidence;
these tests make no API calls or incident decisions.
*/
package health

import (
	"testing"
	"time"
)

// TestVolumeEvidence verifies exact storage rules and grace boundaries without treating normal states as failures.
func TestVolumeEvidence(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	p := DefaultPolicy()

	for _, tt := range []struct {
		name, kind, phase, condition, status, rule, severity string
		age                                                  time.Duration
		deleting                                             bool
	}{
		{name: "pending within grace", kind: "PersistentVolumeClaim", phase: "Pending", age: p.StartupGrace - time.Nanosecond, rule: "pvc.pending", severity: "info"},
		{name: "pending at boundary", kind: "PersistentVolumeClaim", phase: "Pending", age: p.StartupGrace, rule: "pvc.pending", severity: "warning"},
		{name: "lost", kind: "PersistentVolumeClaim", phase: "Lost", rule: "pvc.lost", severity: "critical"},
		{name: "bound", kind: "PersistentVolumeClaim", phase: "Bound"},
		{name: "unknown claim", kind: "PersistentVolumeClaim"},
		{name: "deleting claim", kind: "PersistentVolumeClaim", phase: "Lost", deleting: true},
		{name: "controller resize error", kind: "PersistentVolumeClaim", phase: "Bound", condition: "ControllerResizeError", status: "True", rule: "pvc.resize_error", severity: "warning"},
		{name: "node resize error", kind: "PersistentVolumeClaim", phase: "Bound", condition: "NodeResizeError", status: "True", rule: "pvc.resize_error", severity: "warning"},
		{name: "cleared resize error", kind: "PersistentVolumeClaim", phase: "Bound", condition: "ControllerResizeError", status: "False"},
		{name: "unknown resize error", kind: "PersistentVolumeClaim", phase: "Bound", condition: "NodeResizeError", status: "Unknown"},
		{name: "normal resizing", kind: "PersistentVolumeClaim", phase: "Bound", condition: "Resizing", status: "True"},
		{name: "filesystem resize pending", kind: "PersistentVolumeClaim", phase: "Bound", condition: "FileSystemResizePending", status: "True"},
		{name: "failed volume", kind: "PersistentVolume", phase: "Failed", rule: "pv.failed", severity: "critical"},
		{name: "released volume", kind: "PersistentVolume", phase: "Released"},
		{name: "available volume", kind: "PersistentVolume", phase: "Available"},
		{name: "pending volume", kind: "PersistentVolume", phase: "Pending"},
		{name: "bound volume", kind: "PersistentVolume", phase: "Bound"},
		{name: "deleting volume", kind: "PersistentVolume", phase: "Failed", deleting: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o := Object{Resource: Resource{Kind: tt.kind, Name: "storage", UID: "uid"}, Created: at.Add(-tt.age),
				Phase: tt.phase, Deleting: tt.deleting, Storage: Storage{Requested: "1Gi"}}
			if tt.condition != "" {
				o.Conditions = []Condition{{Type: tt.condition, Status: tt.status}}
			}

			findings := Evaluate(Snapshot{At: at, Objects: []Object{o}}, p)
			if tt.rule == "" {
				if len(findings) != 0 {
					t.Fatalf("normal storage state produced evidence: %+v", findings)
				}
			} else if len(findings) != 1 || findings[0].Rule != tt.rule || findings[0].Severity != tt.severity || findings[0].Resource != o.Resource {
				t.Fatalf("unexpected evidence: %+v", findings)
			}
		})
	}
}

// TestStorageRelationships rejects stale, one-sided and cross-namespace links while keeping Pod use separate from ownership.
func TestStorageRelationships(t *testing.T) {
	claim := Object{Resource: Resource{Kind: "PersistentVolumeClaim", Namespace: "apps", Name: "data", UID: "claim"}, Storage: Storage{VolumeName: "disk"}}
	volume := Object{Resource: Resource{Kind: "PersistentVolume", Name: "disk", UID: "volume"}, Storage: Storage{Claim: claim.Resource}}
	pod := Object{Resource: Resource{Kind: "Pod", Namespace: "apps"}, Claims: []string{"data"}}

	if !MatchesVolumeClaim(volume, claim) || !MatchesClaimPod(claim, pod) {
		t.Fatal("verified storage links missing")
	}

	for _, edit := range []func(*Object){
		func(o *Object) { o.Storage.Claim.UID = "old" },
		func(o *Object) { o.Storage.Claim.Namespace = "other" },
		func(o *Object) { o.Storage.Claim.UID = "" },
		func(o *Object) { o.Name = "different" },
		func(o *Object) { o.Namespace = "apps" },
	} {
		o := volume
		edit(&o)

		if MatchesVolumeClaim(o, claim) {
			t.Fatal("unverified volume binding accepted")
		}
	}

	pod.Namespace = "other"
	if MatchesClaimPod(claim, pod) {
		t.Fatal("Pod claim relationship escaped namespace")
	}

	pod.Namespace, pod.Deleting = "apps", true
	if MatchesClaimPod(claim, pod) {
		t.Fatal("deleting consumer included")
	}
}
