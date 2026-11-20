/*
Purpose: Test AI-first custom-resource no-action, unknown assessment and recovery scope.
Flow: Offline source/model/repository fixtures preserve open records on failures,
reset continuous recovery and keep watch running through temporary CEL errors.
*/
package runner

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/output"
	"github.com/benc-uk/kubescout/internal/resourcepolicy"
)

// extraForRunner supplies one compiled certificate policy and expired private evidence with a stable anchor.
func extraForRunner(t *testing.T) (health.Policy, health.Object) {
	t.Helper()

	policy := health.DefaultPolicy()

	var err error

	policy.ExtraResources, err = resourcepolicy.Load("../../examples/cert-manager.yaml")
	if err != nil {
		t.Fatal(err)
	}

	return policy, health.Object{
		Resource: health.Resource{Kind: "Certificate", APIGroup: "cert-manager.io", Namespace: "apps", Name: "tls", UID: "certificate"},
		Created:  time.Now().Add(-time.Hour), Version: "1",
		Extra: map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "False"}},
			"notAfter": time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)},
	}
}

// TestExtraResourceNoActionAndFailuresRetainIncident verifies critical CEL hints never publish incidents or bypass AI.
func TestExtraResourceNoActionAndFailuresRetainIncident(t *testing.T) {
	for _, mode := range []string{"no-action", "collection-error", "model-error", "cel-error"} {
		t.Run(mode, func(t *testing.T) {
			policy, o := extraForRunner(t)

			source := &fakeSource{objects: []health.Object{o}}
			if mode == "collection-error" {
				source.err = errors.New("custom collection unknown")
			}

			if mode == "cel-error" {
				o.Extra["notAfter"] = "invalid"
			}

			ai := &fakeAI{fail: mode == "model-error"}
			repo := &incident.Memory{Records: map[string]incident.Record{"existing": {Value: incident.Incident{
				Identity:  incident.Identity{Anchor: o.Resource, Family: "custom-resource-health"},
				Resources: []health.Resource{o.Resource}, Lifecycle: incident.Lifecycle{State: "open"},
			}}}}
			cfg := options()
			cfg.Policy = policy

			var out, diag bytes.Buffer

			r := Runner{Config: cfg, Source: source, Analyzer: ai, Incidents: incident.Reconciler{Repository: repo},
				Output: &output.Writer{Out: &out, Err: &diag}}

			code := r.Once(t.Context())
			if (code == 0) != (mode == "no-action") || repo.Records["existing"].Value.Lifecycle.State != "open" ||
				len(repo.Records) != 1 {
				t.Fatal("no-action or unknown assessment changed a stored incident")
			}

			expected := int32(1)
			if mode == "cel-error" || mode == "collection-error" {
				expected = 0
			}

			if ai.calls.Load() != expected {
				t.Fatal("unknown observations reached AI or valid hints skipped AI")
			}
		})
	}
}

// TestExtraResourceScopeAndRecovery checks removed policies and exact continuous recovery timing with status changes.
func TestExtraResourceScopeAndRecovery(t *testing.T) {
	policy, o := extraForRunner(t)
	cfg := options()
	cfg.Policy = policy
	record := incident.Record{Value: incident.Incident{
		Identity:  incident.Identity{Anchor: o.Resource, Family: "custom-resource-health"},
		Resources: []health.Resource{o.Resource}, Lifecycle: incident.Lifecycle{State: "open"},
	}}
	repo := &incident.Memory{Records: map[string]incident.Record{"existing": record}}

	r := Runner{Config: cfg, Incidents: incident.Reconciler{Repository: repo}}
	if _, err := r.loadOpen(t.Context()); err != nil {
		t.Fatal(err)
	}

	r.Config.Policy.ExtraResources = nil
	if _, err := r.loadOpen(t.Context()); err == nil {
		t.Fatal("removed custom scope was interpreted as recovery")
	}

	o.Extra = map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}
	base := time.Now()
	snapshot := health.Snapshot{At: base, Continuity: 1, Objects: []health.Object{o}}
	open := []incident.Record{record}

	contexts, err := incident.BuildInvestigationContexts(snapshot, cfg.Policy, open, cfg.MaxObjects)
	if err != nil {
		t.Fatal(err)
	}

	states := map[string]*candidate{}
	updateCandidates(states, contexts, open, base, cfg)

	key := incident.Ref(o.Resource)
	if recoveryEligible(states[key], base.Add(cfg.RecoveryWindow-time.Nanosecond), cfg.RecoveryWindow) ||
		!recoveryEligible(states[key], base.Add(cfg.RecoveryWindow), cfg.RecoveryWindow) {
		t.Fatal("custom resource bypassed the exact continuous recovery window")
	}

	o.Extra = map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True", "reason": "Renewed"}}}
	snapshot.Objects = []health.Object{o}

	contexts, err = incident.BuildInvestigationContexts(snapshot, cfg.Policy, open, cfg.MaxObjects)
	if err != nil {
		t.Fatal(err)
	}

	updateCandidates(states, contexts, open, base.Add(time.Minute), cfg)

	if recoveryEligible(states[key], base.Add(cfg.RecoveryWindow), cfg.RecoveryWindow) {
		t.Fatal("selected status changes did not reset recovery")
	}
}

// TestExtraResourceWatchEvaluationFailure remains alive with coalesced explicit diagnostics when CEL facts are unavailable.
func TestExtraResourceWatchEvaluationFailure(t *testing.T) {
	policy, o := extraForRunner(t)
	o.Extra["notAfter"] = "invalid"
	cfg := options()
	cfg.Policy = policy
	ai := &fakeAI{}
	source := &fakeSource{objects: []health.Object{o}, events: make(chan struct{}, 1)}

	var out, diag bytes.Buffer

	r := Runner{Config: cfg, Source: source, Analyzer: ai,
		Incidents: incident.Reconciler{Repository: &incident.Memory{}}, Output: &output.Writer{Out: &out, Err: &diag}}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	if code := r.Watch(ctx); code != 0 || ctx.Err() == nil || ai.calls.Load() != 0 ||
		strings.Count(diag.String(), "extra-resource evaluation unavailable") != 1 {
		t.Fatalf("unavailable checks stopped watch, called AI or were not coalesced: %s", diag.String())
	}
}
