/*
Purpose: Test storage incident restart scope, failure retention and recovery gates.
Flow: Offline sources and repositories exercise PVC/PV cases without cluster or
model calls. Storage recovery needs fresh continuous evidence and AI authority;
unknown collection and normal no-action never resolve stored incidents.
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

// TestStoredStorageIncidentScope accepts cluster PVs while rejecting unobserved or malformed storage identities.
func TestStoredStorageIncidentScope(t *testing.T) {
	for _, tt := range []struct {
		kind, namespace string
		invalid         bool
	}{
		{kind: "PersistentVolume"},
		{kind: "PersistentVolume", namespace: "apps", invalid: true},
		{kind: "PersistentVolume", namespace: "other", invalid: true},
		{kind: "PersistentVolumeClaim", namespace: "apps"},
		{kind: "PersistentVolumeClaim", namespace: "other", invalid: true},
	} {
		resource := health.Resource{Kind: tt.kind, Namespace: tt.namespace, Name: "storage", UID: "uid"}
		repo := &incident.Memory{Records: map[string]incident.Record{"existing": {Value: incident.Incident{
			Identity:  incident.Identity{Observer: "fixture", Anchor: resource, Family: "storage-availability"},
			Resources: []health.Resource{resource}, Lifecycle: incident.Lifecycle{State: "open"}}}}}
		r := Runner{Config: options(), Incidents: incident.Reconciler{Repository: repo}}

		if _, err := r.loadOpen(t.Context()); (err != nil) != tt.invalid {
			t.Fatalf("storage restart scope mismatch: %v", err)
		}
	}
}

// TestStorageNoActionAndCollectionFailureRetainOpenIncidents checks explicit AI authority for loss/failure cases.
func TestStorageNoActionAndCollectionFailureRetainOpenIncidents(t *testing.T) {
	for _, kind := range []string{"PersistentVolumeClaim", "PersistentVolume"} {
		for _, failed := range []bool{false, true} {
			o := health.Object{Resource: health.Resource{Kind: kind, Namespace: "apps", Name: "storage", UID: "uid"}, Phase: "Lost"}
			if kind == "PersistentVolume" {
				o.Namespace, o.Phase = "", "Failed"
			}

			source := &fakeSource{objects: []health.Object{o}}
			if failed {
				source.err = errors.New("storage collection unavailable")
			}

			repo := &incident.Memory{Records: map[string]incident.Record{"existing": {Value: incident.Incident{
				Identity:  incident.Identity{Observer: "fixture", Anchor: o.Resource, Family: "storage-availability"},
				Resources: []health.Resource{o.Resource}, Lifecycle: incident.Lifecycle{State: "open"}}}}}
			ai := &fakeAI{}

			var out, diag bytes.Buffer

			r := Runner{Config: options(), Source: source, Analyzer: ai, Incidents: incident.Reconciler{Repository: repo},
				Output: &output.Writer{Out: &out, Err: &diag}}
			want, calls := 0, int32(1)

			if failed {
				want, calls = 1, 0
			}

			if code := r.Once(t.Context()); code != want || ai.calls.Load() != calls ||
				len(repo.Records) != 1 || repo.Records["existing"].Value.Lifecycle.State != "open" {
				t.Fatal("storage no-action/outage changed a stored incident or bypassed AI")
			}
		}
	}
}

// TestStorageRecoveryAndFreshness requires continuous Bound observations and rejects changes after inspection.
func TestStorageRecoveryAndFreshness(t *testing.T) {
	cfg := options()
	base := time.Now()
	o := health.Object{Resource: health.Resource{Kind: "PersistentVolumeClaim", Namespace: "apps", Name: "data", UID: "uid"},
		Phase: "Bound", Version: "10"}
	record := incident.Record{Value: incident.Incident{Identity: incident.Identity{Anchor: o.Resource, Family: "storage-availability"},
		Resources: []health.Resource{o.Resource}, Lifecycle: incident.Lifecycle{State: "open"}}}
	open := []incident.Record{record}
	snapshot := health.Snapshot{At: base, Objects: []health.Object{o}}

	contexts, err := incident.BuildInvestigationContexts(snapshot, cfg.Policy, open, 100)
	if err != nil {
		t.Fatal(err)
	}

	states := map[string]*candidate{}
	updateCandidates(states, contexts, open, base, cfg)

	key := incident.Ref(o.Resource)
	c := states[key]

	if !c.recoverySince.Equal(base) {
		t.Fatal("fresh storage recovery was not tracked")
	}

	source := &fakeSource{objects: []health.Object{o}}
	r := Runner{Config: cfg, Source: source}
	in := contexts[key]
	in.RecoveryEligible = true
	in.ToolVersions = map[string]string{key: "10"}

	if !r.current(in, open) {
		t.Fatal("fresh unchanged storage evidence rejected")
	}

	source.err = errors.New("storage observation unknown")

	if r.current(in, open) {
		t.Fatal("collection outage allowed storage recovery publication")
	}

	source.err = nil
	source.objects[0].Version = "11"

	if r.current(in, open) {
		t.Fatal("changed inspected storage accepted")
	}

	source.objects[0].Version, source.objects[0].Phase = "10", "Lost"

	if r.current(in, open) {
		t.Fatal("lost binding accepted as healthy recovery")
	}

	snapshot.Objects[0].Phase = "Lost"

	contexts, err = incident.BuildInvestigationContexts(snapshot, cfg.Policy, open, 100)
	if err != nil {
		t.Fatal(err)
	}

	updateCandidates(states, contexts, open, base.Add(time.Minute), cfg)

	if !c.recoverySince.IsZero() {
		t.Fatal("recurring claim loss did not reset continuous recovery")
	}

	snapshot.Objects[0].Phase = "Bound"

	contexts, err = incident.BuildInvestigationContexts(snapshot, cfg.Policy, open, 100)
	if err != nil {
		t.Fatal(err)
	}

	updateCandidates(states, contexts, open, base.Add(2*time.Minute), cfg)

	if !c.recoverySince.Equal(base.Add(2 * time.Minute)) {
		t.Fatal("storage recovery reused an interrupted interval")
	}
}
