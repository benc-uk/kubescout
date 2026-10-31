package runner

import (
	"io"
	"testing"
	"time"

	"k8s-ops-agent/internal/health"
	"k8s-ops-agent/internal/incident"
)

func TestDayOfStableIncidentReviewsCreatesOnlyOneCR(t *testing.T) {
	cfg := options()
	cfg.BatchWindow = 5 * time.Second
	base := time.Now()
	pod := failedPod()
	inputs, err := incident.Inputs(health.Snapshot{At: base, Objects: []health.Object{pod}}, cfg.Policy, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	key := incident.Ref(pod.Resource)
	in := inputs[key]
	action := incident.Action{Operation: "raise", Family: "container-failure", ResourceRefs: []string{key}, Title: "Crash loop", Severity: "medium", Impact: "degraded",
		Diagnosis: "repeated-exit", Explanation: "Repeated workload failure", EvidenceIDs: []string{"snapshot"}, Recommendations: []string{"Inspect configuration"},
		Uncertainty: "Cause not yet confirmed", MaterialChange: true}
	repo := &incident.Memory{Log: io.Discard}
	reconciler := incident.Reconciler{Repository: repo, Observer: "fixture"}
	changes, err := reconciler.Apply(t.Context(), in, incident.Decision{Outcome: "decisions", Reason: "AI recognised a problem", Actions: []incident.Action{action}}, func() bool { return true })
	if err != nil || len(changes) != 1 {
		t.Fatal("initial incident failed")
	}
	mutations := len(changes)
	open, err := repo.ListOpen(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	states := map[string]*candidate{}
	updateCandidates(states, inputs, open, base, cfg)
	states[key].lastReview, states[key].lastRevision, states[key].next = base, in.Revision, base.Add(cfg.ReviewInterval)
	reviews := 0
	for cycle := 1; cycle <= 288; cycle++ {
		now := base.Add(time.Duration(cycle) * 5 * time.Minute)
		// Brief disconnected epochs reset recovery, not incident identity.
		states[key].recoverySince = time.Time{}
		in.Signals = append(inputs[key].Signals, incident.Signal{ID: "changing-event", Rule: "event.warning", Hint: "info", Evidence: []string{"count=changed"}})
		in.Revision = incident.Fingerprint(in)
		updateCandidates(states, map[string]incident.Input{key: in}, open, now, cfg)
		if now.Before(states[key].next) {
			continue
		}
		reviews++
		in.Incidents, err = reconciler.Existing(t.Context(), pod.Resource)
		if err != nil {
			t.Fatal(err)
		}
		action.Operation, action.IncidentID, action.Explanation = "update", changes[0].Name, "Different wording for unchanged facts"
		result, err := reconciler.Apply(t.Context(), in, incident.Decision{Outcome: "decisions", Reason: "Same conclusion", Actions: []incident.Action{action}}, func() bool { return true })
		if err != nil {
			t.Fatal(err)
		}
		mutations += len(result)
		states[key].lastReview, states[key].next = now, now.Add(cfg.ReviewInterval)
	}
	if reviews != 96 || mutations != 1 || len(repo.Records) != 1 {
		t.Fatalf("reviews=%d mutations=%d records=%d", reviews, mutations, len(repo.Records))
	}
}
