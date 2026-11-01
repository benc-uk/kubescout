/*
Purpose: Test HPA evidence exclusions and scoped scale-target relationships.
Flow: Offline projections exercise startup-grace boundaries, explicit condition
failures, normal limits, disabled scaling and current workload references.
These pure findings are private hints, never AI decisions or scaling writes.
*/
package health

import (
	"testing"
	"time"
)

// TestHPAEvidence checks exact condition rules and grace boundaries without turning normal scaling into incidents.
func TestHPAEvidence(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	policy := DefaultPolicy()

	for _, tt := range []struct {
		name, condition, status, reason, rule, severity string
		age                                             time.Duration
		deleting                                        bool
	}{
		{name: "inactive during grace", condition: "ScalingActive", status: "False", reason: "FailedGetResourceMetric", age: policy.StartupGrace - time.Nanosecond, rule: "hpa.inactive", severity: "info"},
		{name: "inactive at boundary", condition: "ScalingActive", status: "False", age: policy.StartupGrace, rule: "hpa.inactive", severity: "warning"},
		{name: "unable to scale", condition: "AbleToScale", status: "False", age: time.Hour, rule: "hpa.unable_to_scale", severity: "warning"},
		{name: "startup scale target", condition: "AbleToScale", status: "False", rule: "hpa.unable_to_scale", severity: "info"},
		{name: "normal limit", condition: "ScalingLimited", status: "True", age: time.Hour},
		{name: "disabled scaling", condition: "ScalingActive", status: "False", reason: "ScalingDisabled", age: time.Hour},
		{name: "active", condition: "ScalingActive", status: "True", age: time.Hour},
		{name: "unknown", condition: "ScalingActive", status: "Unknown", age: time.Hour},
		{name: "missing conditions", age: time.Hour},
		{name: "deleting", condition: "AbleToScale", status: "False", age: time.Hour, deleting: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o := Object{Resource: Resource{Kind: "HorizontalPodAutoscaler", Namespace: "apps", Name: "web", UID: "hpa"},
				Created: at.Add(-tt.age), Deleting: tt.deleting, Generation: 2, Observed: 1, HPA: HPAState{Current: 2, Desired: 5, Min: 1, Max: 5}}
			if tt.condition != "" {
				o.Conditions = []Condition{{Type: tt.condition, Status: tt.status, Reason: tt.reason}}
			}

			findings := Evaluate(Snapshot{At: at, Objects: []Object{o}}, policy)
			if tt.rule == "" {
				if len(findings) != 0 {
					t.Fatalf("normal HPA state produced evidence: %+v", findings)
				}
			} else if len(findings) != 1 || findings[0].Rule != tt.rule || findings[0].Severity != tt.severity || findings[0].Resource != o.Resource {
				t.Fatalf("unexpected HPA evidence: %+v", findings)
			}
		})
	}
}

// TestHPATargetRelationship requires a supported exact API/kind/name and the same observed namespace.
func TestHPATargetRelationship(t *testing.T) {
	hpa := Object{Resource: Resource{Kind: "HorizontalPodAutoscaler", Namespace: "apps"},
		HPA: HPAState{TargetVersion: "apps/v1", TargetKind: "Deployment", TargetName: "web"}}
	target := Object{Resource: Resource{Kind: "Deployment", Namespace: "apps", Name: "web", UID: "current"}}

	if !MatchesHPATarget(hpa, target) {
		t.Fatal("known scale target missing")
	}

	for _, edit := range []func(*Object){
		func(o *Object) { o.Namespace = "other" },
		func(o *Object) { o.Kind = "StatefulSet" },
		func(o *Object) { o.Name = "different" },
		func(o *Object) { o.UID = "" },
	} {
		o := target
		edit(&o)

		if MatchesHPATarget(hpa, o) {
			t.Fatal("unverified scale target matched")
		}
	}

	for _, version := range []string{"", "extensions/v1beta1", "custom.example/v1"} {
		hpa.HPA.TargetVersion = version
		if MatchesHPATarget(hpa, target) {
			t.Fatal("unsupported scale-target API version matched")
		}
	}
}
