/*
Purpose: Test runner scope, no-action handling and review scheduling offline.
Flow: Fake collection and AI sources feed once/watch execution and candidate
updates, checking failure output, nonblocking collection, counter-noise
suppression and recovery timing without live model or Kubernetes calls.
*/
package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/output"
)

type fakeSource struct {
	mu      sync.Mutex
	objects []health.Object
	err     error
	events  chan struct{}
}

type fakeAI struct {
	calls   atomic.Int32
	fail    bool
	started chan struct{}
	release chan struct{}
}

// Run keeps the fixture source alive until the runner cancels collection.
func (s *fakeSource) Run(ctx context.Context) { <-ctx.Done() }

// Changes exposes the fixture's collection-update notifications.
func (s *fakeSource) Changes() <-chan struct{} { return s.events }

// Err returns the injected collection failure under the source lock.
func (s *fakeSource) Err() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }

// Snapshot copies the fixture object slice and returns the currently injected collection error.
func (s *fakeSource) Snapshot(at time.Time) (health.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return health.Snapshot{At: at, Objects: append([]health.Object{}, s.objects...)}, s.err
}

// Collect supplies the same in-memory snapshot for bounded once assessments.
func (s *fakeSource) Collect(_ context.Context, at time.Time) (health.Snapshot, error) {
	return s.Snapshot(at)
}

// set replaces collection fixtures under lock and sends a nonblocking change notification.
func (s *fakeSource) set(objects []health.Object, err error) {
	s.mu.Lock()
	s.objects, s.err = objects, err
	s.mu.Unlock()

	select {
	case s.events <- struct{}{}:
	default:
	}
}

// Investigate counts fixture calls, optionally blocks, and returns no-action or an injected failure.
func (a *fakeAI) Investigate(ctx context.Context, in incident.InvestigationContext) (incident.Decision, error) {
	a.calls.Add(1)

	if a.started != nil {
		a.started <- struct{}{}
	}

	if a.release != nil {
		select {
		case <-a.release:
		case <-ctx.Done():
			return incident.Decision{}, ctx.Err()
		}
	}

	if a.fail {
		return incident.Decision{}, errors.New("model unavailable")
	}

	return incident.Decision{Outcome: "no_action", Reason: "expected test fixture", Actions: []incident.Action{}}, nil
}

// options supplies bounded runner settings with short polling intervals for offline tests.
func options() config.Config {
	return config.Config{ObserverID: "fixture", Model: "fixture", NamespaceWatch: []string{"apps"}, MaxObjects: 100, RunTimeout: time.Second,
		BatchWindow: 5 * time.Millisecond, CheckInterval: 10 * time.Millisecond, Cooldown: 2 * time.Minute, RecoveryWindow: 2 * time.Minute, ReviewInterval: 15 * time.Minute, Policy: health.DefaultPolicy()}
}

// failedPod builds a stable-identity failed Pod fixture old enough for health evaluation.
func failedPod() health.Object {
	return health.Object{Resource: health.Resource{Kind: "Pod", Namespace: "apps", Name: "bad", UID: "id"}, Phase: "Failed", Created: time.Now().Add(-time.Hour)}
}

// TestStoredIncidentNamespaceScope checks scope rejection without removing or resolving stored incidents.
func TestStoredIncidentNamespaceScope(t *testing.T) {
	for _, tt := range []struct {
		name    string
		scope   config.Config
		ns      string
		invalid bool
	}{
		{name: "all", ns: "payments"},
		{name: "watch multiple", scope: config.Config{NamespaceWatch: []string{"apps", "payments"}}, ns: "payments"},
		{name: "watch excludes", scope: config.Config{NamespaceWatch: []string{"apps"}}, ns: "payments", invalid: true},
		{name: "ignore permits others", scope: config.Config{NamespaceIgnore: []string{"system"}}, ns: "payments"},
		{name: "ignore retains outside scope", scope: config.Config{NamespaceIgnore: []string{"payments"}}, ns: "payments", invalid: true},
		{name: "nodes always in scope", scope: config.Config{NamespaceWatch: []string{"apps"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resource := failedPod().Resource

			resource.Namespace = tt.ns
			if tt.ns == "" {
				resource.Kind = "Node"
			}

			record := incident.Record{Value: incident.Incident{
				Identity:  incident.Identity{Observer: "fixture", Anchor: resource, Family: "container-failure"},
				Resources: []health.Resource{resource}, Lifecycle: incident.Lifecycle{State: "open"}}}
			repo := &incident.Memory{Records: map[string]incident.Record{"existing": record}}
			r := Runner{Config: tt.scope, Incidents: incident.Reconciler{Repository: repo}}

			open, err := r.loadOpen(t.Context())
			if (err != nil) != tt.invalid || err == nil && len(open) != 1 {
				t.Fatalf("unexpected scope reconciliation: %v", err)
			}

			if len(repo.Records) != 1 || repo.Records["existing"].Value.Lifecycle.State != "open" {
				t.Fatal("scope change removed or resolved an unobserved incident")
			}
		})
	}
}

// TestOnceNoActionAndFailuresNeverCreateIncidents checks empty execution output and exit codes for non-decisions.
func TestOnceNoActionAndFailuresNeverCreateIncidents(t *testing.T) {
	for _, test := range []struct {
		name              string
		collection, model bool
		want              int
	}{
		{"AI no action", false, false, 0}, {"collection failure", true, false, 1}, {"AI failure", false, true, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var out, diag bytes.Buffer

			source := &fakeSource{objects: []health.Object{failedPod()}}
			if test.collection {
				source.err = errors.New("disconnected")
			}

			repo := &incident.Memory{}
			ai := &fakeAI{fail: test.model}

			r := Runner{Config: options(), Source: source, Analyzer: ai, Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: output.Writer{Out: &out, Err: &diag}}
			if code := r.Once(t.Context()); code != test.want {
				t.Fatalf("code=%d", code)
			}

			var result output.Result
			if bytes.Count(out.Bytes(), []byte("\n")) != 1 || json.Unmarshal(out.Bytes(), &result) != nil || len(result.Changes) != 0 || len(repo.Records) != 0 {
				t.Fatal("no-action/failure created incident output")
			}
		})
	}
}

// TestModelDoesNotBlockFreshnessOrCauseRenewalReports injects collection failure during blocked AI work.
// It checks graceful watch shutdown without publishing collection or model transitions as incidents.
func TestModelDoesNotBlockFreshnessOrCauseRenewalReports(t *testing.T) {
	source := &fakeSource{objects: []health.Object{failedPod()}, events: make(chan struct{}, 1)}
	ai := &fakeAI{started: make(chan struct{}, 1), release: make(chan struct{})}
	repo := &incident.Memory{}

	var out bytes.Buffer

	r := Runner{Config: options(), Source: source, Analyzer: ai, Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: output.Writer{Out: &out, Err: io.Discard}}
	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan int, 1)
	go func() { done <- r.Watch(ctx) }()

	select {
	case <-ai.started:
	case <-time.After(time.Second):
		t.Fatal("AI did not start")
	}

	source.set(nil, errors.New("watch ended"))
	time.Sleep(20 * time.Millisecond)
	close(ai.release)
	time.Sleep(20 * time.Millisecond)
	cancel()

	if code := <-done; code != 0 {
		t.Fatalf("shutdown=%d", code)
	}

	if out.Len() != 0 || len(repo.Records) != 0 {
		t.Fatal("model/collection transitions created incidents")
	}
}

// TestSchedulingBoundariesAndDayOfCounterChurn checks batching, noise suppression, and recovery timing.
// It also verifies recovery resets and that AI no-action does not trigger immediate re-review.
func TestSchedulingBoundariesAndDayOfCounterChurn(t *testing.T) {
	cfg := options()
	cfg.BatchWindow = 5 * time.Second
	base := time.Now()
	pod := failedPod()

	investigations, err := incident.BuildInvestigationContexts(health.Snapshot{At: base, Objects: []health.Object{pod}}, cfg.Policy, nil, 100)
	if err != nil {
		t.Fatal(err)
	}

	states := map[string]*candidate{}
	updateCandidates(states, investigations, nil, base, cfg)

	key := incident.Ref(pod.Resource)

	c := states[key]
	if !c.next.Equal(base.Add(5 * time.Second)) {
		t.Fatal("batch deadline incorrect")
	}

	c.lastReview, c.lastRevision, c.next = base, investigations[key].Revision, base.Add(cfg.ReviewInterval)
	for cycle := 1; cycle <= 288; cycle++ {
		at := base.Add(time.Duration(cycle) * 5 * time.Minute)
		in := investigations[key]
		in.Signals = append(in.Signals, incident.Signal{ID: "event", Rule: "event.warning", Hint: "info", Evidence: []string{"count changed"}})
		in.Revision = incident.Fingerprint(in)
		updateCandidates(states, map[string]incident.InvestigationContext{key: in}, nil, at, cfg)

		if c.lastRevision != in.Revision || !c.next.Equal(base.Add(cfg.ReviewInterval)) {
			t.Fatal("counter change rescheduled AI")
		}
	}

	open := []incident.Record{{Value: incident.Incident{Identity: incident.Identity{Observer: "fixture", Anchor: pod.Resource, Family: "container-failure"}}}}
	empty := investigations[key]
	empty.Signals = nil
	empty.Revision = incident.Fingerprint(empty)
	updateCandidates(states, map[string]incident.InvestigationContext{key: empty}, open, base, cfg)

	if c.recoverySince.IsZero() {
		t.Fatal("recovery without active signals was not scheduled")
	}

	if !base.Add(2*time.Minute - time.Nanosecond).Before(c.recoverySince.Add(cfg.RecoveryWindow)) {
		t.Fatal("recovery eligible too early")
	}

	if base.Add(2 * time.Minute).Before(c.recoverySince.Add(cfg.RecoveryWindow)) {
		t.Fatal("recovery ineligible at boundary")
	}

	c.recoverySince = time.Time{}

	updateCandidates(states, map[string]incident.InvestigationContext{key: empty}, open, base.Add(time.Minute), cfg)

	if !c.recoverySince.Equal(base.Add(time.Minute)) {
		t.Fatal("disconnection did not reset continuous recovery")
	}

	c.lastReview = base.Add(4 * time.Minute)
	c.next = c.lastReview.Add(cfg.ReviewInterval)
	updateCandidates(states, map[string]incident.InvestigationContext{key: empty}, open, base.Add(5*time.Minute), cfg)

	if !c.next.Equal(c.lastReview.Add(cfg.ReviewInterval)) {
		t.Fatal("AI no-action during recovery caused an immediate re-review loop")
	}
}
