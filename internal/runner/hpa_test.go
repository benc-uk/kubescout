/*
Purpose: Test HPA incident retention and recovery publication gates offline.
Flow: In-memory collection/AI fixtures preserve unresolved autoscaling incidents
during no-action, model failure and collection outages. Fresh healthy conditions
start recovery review, while changed configurations or inspected versions reject
stale AI conclusions; no Kubernetes or live model calls are made.
*/
package runner

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/output"
)

// TestHPANoActionAndFailuresRetainIncidents requires AI authority and preserves unresolved state during unknown observations.
func TestHPANoActionAndFailuresRetainIncidents(t *testing.T) {
	for _, tt := range []struct {
		name              string
		collection, model bool
		code              int
	}{
		{name: "no-action"},
		{name: "unknown collection", collection: true, code: 1},
		{name: "model failure", model: true, code: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			hpa := health.Object{Resource: health.Resource{Kind: "HorizontalPodAutoscaler", Namespace: "apps", Name: "web", UID: "hpa"},
				Conditions: []health.Condition{{Type: "ScalingActive", Status: "False", Reason: "FailedGetResourceMetric"}}}

			source := &fakeSource{objects: []health.Object{hpa}}
			if tt.collection {
				source.err = errors.New("HPA collection unknown")
			}

			repo := &incident.Memory{Records: map[string]incident.Record{"existing": {Value: incident.Incident{
				Identity:  incident.Identity{Anchor: hpa.Resource, Family: "autoscaling"},
				Resources: []health.Resource{hpa.Resource}, Lifecycle: incident.Lifecycle{State: "open"}}}}}
			ai := &fakeAI{fail: tt.model}

			var out, diag bytes.Buffer

			r := Runner{Config: options(), Source: source, Analyzer: ai, Incidents: incident.Reconciler{Repository: repo},
				Output: &output.Writer{Out: &out, Err: &diag}}
			if r.Once(t.Context()) != tt.code || len(repo.Records) != 1 || repo.Records["existing"].Value.Lifecycle.State != "open" ||
				tt.collection && ai.calls.Load() != 0 || !tt.collection && ai.calls.Load() != 1 {
				t.Fatal("HPA no-action/failure changed an incident or bypassed AI")
			}
		})
	}
}

// TestHPARecoveryFreshness requires fresh healthy conditions and rejects changed HPA identities, configurations or inspected versions.
func TestHPARecoveryFreshness(t *testing.T) {
	cfg := options()
	base := time.Now()
	hpa := health.Object{Resource: health.Resource{Kind: "HorizontalPodAutoscaler", Namespace: "apps", Name: "web", UID: "hpa"},
		Version: "10", HPA: health.HPAState{Min: 1, Max: 5},
		Conditions: []health.Condition{{Type: "ScalingActive", Status: "True"}, {Type: "AbleToScale", Status: "True"}}}
	open := []incident.Record{{Value: incident.Incident{Identity: incident.Identity{Anchor: hpa.Resource, Family: "autoscaling"},
		Resources: []health.Resource{hpa.Resource}, Lifecycle: incident.Lifecycle{State: "open"}}}}

	contexts, err := incident.BuildInvestigationContexts(health.Snapshot{At: base, Objects: []health.Object{hpa}}, cfg.Policy, open, 100)
	if err != nil {
		t.Fatal(err)
	}

	key := incident.Ref(hpa.Resource)
	states := map[string]*candidate{}
	updateCandidates(states, contexts, open, base, cfg)

	if !states[key].recoverySince.Equal(base) {
		t.Fatal("fresh HPA recovery interval was not started")
	}

	in := contexts[key]
	in.RecoveryEligible = true
	in.ToolVersions = map[string]string{key: "10"}
	source := &fakeSource{objects: []health.Object{hpa}}
	r := Runner{Config: cfg, Source: source}

	if !r.current(in, open) {
		t.Fatal("fresh unchanged HPA evidence rejected")
	}

	source.err = errors.New("HPA collection unknown")

	if r.current(in, open) {
		t.Fatal("unknown collection established HPA recovery")
	}

	source.err = nil
	source.objects[0].Version = "11"

	if r.current(in, open) {
		t.Fatal("changed inspected HPA version accepted")
	}

	source.objects[0].Version = "10"
	source.objects[0].HPA.Max = 10

	if r.current(in, open) {
		t.Fatal("changed HPA configuration accepted as unchanged recovery")
	}

	source.objects[0].HPA.Max = 5
	source.objects[0].Conditions = []health.Condition{{Type: "ScalingActive", Status: "False"}}

	if r.current(in, open) {
		t.Fatal("new HPA failure accepted as healthy recovery evidence")
	}

	contexts, err = incident.BuildInvestigationContexts(health.Snapshot{At: base.Add(time.Minute), Objects: source.objects}, cfg.Policy, open, 100)
	if err != nil {
		t.Fatal(err)
	}

	updateCandidates(states, contexts, open, base.Add(time.Minute), cfg)

	if !states[key].recoverySince.IsZero() {
		t.Fatal("HPA failure did not reset continuous recovery")
	}
}
