/*
Purpose: Test core evidence rules, grace periods, severities and finding identity.
Flow: Offline snapshot fixtures exercise the check registry and fingerprint
helpers, including informational signals; no cluster or AI calls are made.
*/
package health

import (
	"slices"
	"testing"
	"time"
)

// TestRules covers evidence severities, grace boundaries and exclusions using projected resource fixtures.
func TestRules(t *testing.T) {
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	old := now.Add(-time.Hour)
	p := DefaultPolicy()

	tests := []struct {
		name           string
		object         Object
		rule, severity string
	}{
		{"node ready", Object{Resource: Resource{Kind: "Node"}, Conditions: []Condition{{Type: "Ready", Status: "True"}}}, "", ""},
		{"node missing ready", Object{Resource: Resource{Kind: "Node"}}, "node.ready", "critical"},
		{"node unknown", Object{Resource: Resource{Kind: "Node"}, Conditions: []Condition{{Type: "Ready", Status: "Unknown"}}}, "node.ready", "critical"},
		{"pressure unknown", Object{Resource: Resource{Kind: "Node"}, Conditions: []Condition{{Type: "Ready", Status: "True"}, {Type: "DiskPressure", Status: "Unknown"}}}, "node.DiskPressure", "warning"},
		{"pending new", Object{Resource: Resource{Kind: "Pod"}, Created: now, Phase: "Pending"}, "", ""},
		{"pending grace boundary", Object{Resource: Resource{Kind: "Pod"}, Created: now.Add(-p.StartupGrace), Phase: "Pending"}, "pod.pending", "warning"},
		{"pending missing creation", Object{Resource: Resource{Kind: "Pod"}, Phase: "Pending"}, "pod.pending", "warning"},
		{"unscheduled", Object{Resource: Resource{Kind: "Pod"}, Created: old, Phase: "Pending", Conditions: []Condition{{Type: "PodScheduled", Status: "False", Transition: old}}}, "pod.unschedulable", "warning"},
		{"unscheduled transition grace", Object{Resource: Resource{Kind: "Pod"}, Created: old, Phase: "Pending", Conditions: []Condition{{Type: "PodScheduled", Status: "False", Transition: now}}}, "", ""},
		{"readiness", Object{Resource: Resource{Kind: "Pod"}, Created: old, Phase: "Running"}, "pod.ready", "warning"},
		{"readiness transition grace", Object{Resource: Resource{Kind: "Pod"}, Created: old, Phase: "Running", Conditions: []Condition{{Type: "Ready", Status: "False", Transition: now}}}, "", ""},
		{"failed", Object{Resource: Resource{Kind: "Pod"}, Phase: "Failed"}, "pod.failed", "critical"},
		{"unknown", Object{Resource: Resource{Kind: "Pod"}, Phase: "Unknown"}, "pod.unknown", "warning"},
		{"succeeded", Object{Resource: Resource{Kind: "Pod"}, Phase: "Succeeded"}, "", ""},
		{"deleting", Object{Resource: Resource{Kind: "Pod"}, Deleting: true, Phase: "Failed"}, "", ""},
		{"immediate image failure", Object{Resource: Resource{Kind: "Pod"}, Created: now, Phase: "Pending", Containers: []Container{{Name: "app", Image: "registry/app:bad", Waiting: "ErrImagePull"}}}, "pod.container.waiting", "warning"},
		{"normal container startup", Object{Resource: Resource{Kind: "Pod"}, Created: now, Phase: "Pending", Containers: []Container{{Waiting: "ContainerCreating"}}}, "", ""},
		{"init failure", Object{Resource: Resource{Kind: "Pod"}, Created: now, Phase: "Pending", Containers: []Container{{Name: "init", Init: true, Waiting: "CrashLoopBackOff"}}}, "pod.container.waiting", "warning"},
		{"terminated", Object{Resource: Resource{Kind: "Pod"}, Created: now, Phase: "Pending", Containers: []Container{{ExitCode: new(int32(1))}}}, "pod.container.terminated", "warning"},
		{"restarts info", Object{Resource: Resource{Kind: "Pod"}, Created: now, Phase: "Pending", Containers: []Container{{Restarts: 3}}}, "pod.container.restarts", "info"},
		{"deployment scaled zero", Object{Resource: Resource{Kind: "Deployment"}, Generation: 2}, "", ""},
		{"deployment fresh rollout", Object{Resource: Resource{Kind: "Deployment"}, Created: now, Desired: 1, Generation: 1, Observed: 1}, "deployment.replicas", "info"},
		{"deployment rollout boundary", Object{Resource: Resource{Kind: "Deployment"}, Created: now.Add(-p.RolloutGrace), Desired: 1, Generation: 1, Observed: 1}, "deployment.replicas", "warning"},
		{"unobserved", Object{Resource: Resource{Kind: "Deployment"}, Created: old, Desired: 1, Generation: 2, Observed: 1}, "deployment.unobserved", "warning"},
		{"paused", Object{Resource: Resource{Kind: "Deployment"}, Created: old, Desired: 1, Available: 1, Paused: true, Generation: 1, Observed: 1}, "", ""},
		{"deadline immediate", Object{Resource: Resource{Kind: "Deployment"}, Created: now, Desired: 1, Available: 1, Updated: 1, Conditions: []Condition{{Type: "Progressing", Status: "False", Reason: "ProgressDeadlineExceeded"}}}, "deployment.progress_deadline", "critical"},
		{"replica failure immediate", Object{Resource: Resource{Kind: "Deployment"}, Created: now, Desired: 1, Available: 1, Updated: 1, Conditions: []Condition{{Type: "ReplicaFailure", Status: "True"}}}, "deployment.replica_failure", "critical"},
		{"job failure", Object{Resource: Resource{Kind: "Job"}, Conditions: []Condition{{Type: "FailureTarget", Status: "True"}}}, "job.failed", "critical"},
		{"job running", Object{Resource: Resource{Kind: "Job"}}, "", ""},
		{"event current", Object{Resource: Resource{Kind: "Event"}, EventTime: now}, "event.warning", "info"},
		{"event stale", Object{Resource: Resource{Kind: "Event"}, EventTime: old.Add(-time.Second)}, "", ""},
		{"event future", Object{Resource: Resource{Kind: "Event"}, EventTime: now.Add(time.Second)}, "", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			findings := Evaluate(Snapshot{At: now, Objects: []Object{test.object}}, p)
			if test.rule == "" {
				if len(findings) != 0 {
					t.Fatalf("unexpected findings: %+v", findings)
				}
			} else if len(findings) != 1 || findings[0].Rule != test.rule || findings[0].Severity != test.severity {
				t.Fatalf("want %s/%s, got %+v", test.rule, test.severity, findings)
			}

			if test.name == "immediate image failure" && !slices.Contains(findings[0].Evidence, "image=registry/app:bad") {
				t.Fatal("missing image evidence")
			}
		})
	}
}

// TestStableIdentityAndMaterialFingerprint separates resource identity from mutable evidence
// and verifies that informational findings alone do not produce an unhealthy evidence status.
func TestStableIdentityAndMaterialFingerprint(t *testing.T) {
	r := Resource{Kind: "Pod", Name: "pod", UID: "one"}
	f := NewFinding("test", "warning", r, "container", "summary", "image=app:v1")

	g := NewFinding("test", "critical", r, "container", "summary", "image=app:v2")
	if f.ID != g.ID || Fingerprint(f) == Fingerprint(g) {
		t.Fatal("identity must survive evidence/severity changes but fingerprint must change")
	}

	r.UID = "two"
	if f.ID == NewFinding("test", "warning", r, "container", "summary").ID {
		t.Fatal("recreated resources need a new identity")
	}

	if Status([]Finding{NewFinding("test", "info", r, "", "summary")}) != "healthy" {
		t.Fatal("info is not an unhealthy signal")
	}
}
