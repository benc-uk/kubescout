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

	"k8s-ops-agent/internal/config"
	"k8s-ops-agent/internal/health"
	"k8s-ops-agent/internal/incident"
	"k8s-ops-agent/internal/report"
)

type fakeSource struct {
	mu      sync.Mutex
	objects []health.Object
	err     error
	events  chan struct{}
}

func (s *fakeSource) Run(ctx context.Context)  { <-ctx.Done() }
func (s *fakeSource) Changes() <-chan struct{} { return s.events }
func (s *fakeSource) Err() error               { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *fakeSource) Snapshot(at time.Time) (health.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return health.Snapshot{At: at, Objects: append([]health.Object{}, s.objects...)}, s.err
}
func (s *fakeSource) Collect(_ context.Context, at time.Time) (health.Snapshot, error) {
	return s.Snapshot(at)
}
func (s *fakeSource) set(objects []health.Object, err error) {
	s.mu.Lock()
	s.objects, s.err = objects, err
	s.mu.Unlock()
	select {
	case s.events <- struct{}{}:
	default:
	}
}

type fakeAI struct {
	calls   atomic.Int32
	fail    bool
	started chan struct{}
	release chan struct{}
}

func (a *fakeAI) Investigate(ctx context.Context, in incident.Input) (incident.Decision, error) {
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
func options() config.Config {
	return config.Config{ObserverID: "fixture", Model: "fixture", NamespaceWatch: []string{"apps"}, MaxObjects: 100, RunTimeout: time.Second,
		BatchWindow: 5 * time.Millisecond, CheckInterval: 10 * time.Millisecond, Cooldown: 2 * time.Minute, RecoveryWindow: 2 * time.Minute, ReviewInterval: 15 * time.Minute, Policy: health.DefaultPolicy()}
}
func failedPod() health.Object {
	return health.Object{Resource: health.Resource{Kind: "Pod", Namespace: "apps", Name: "bad", UID: "id"}, Phase: "Failed", Created: time.Now().Add(-time.Hour)}
}

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
				Resources: []health.Resource{resource}, Lifecycle: incident.Lifecycle{Phase: "open"}}}
			repo := &incident.Memory{Records: map[string]incident.Record{"existing": record}}
			r := Runner{Config: tt.scope, Incidents: incident.Reconciler{Repository: repo}}
			open, err := r.loadOpen(t.Context())
			if (err != nil) != tt.invalid || err == nil && len(open) != 1 {
				t.Fatalf("unexpected scope reconciliation: %v", err)
			}
			if len(repo.Records) != 1 || repo.Records["existing"].Value.Lifecycle.Phase != "open" {
				t.Fatal("scope change removed or resolved an unobserved incident")
			}
		})
	}
}

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
			repo := &incident.Memory{Log: &diag}
			ai := &fakeAI{fail: test.model}
			r := Runner{Config: options(), Source: source, Analyzer: ai, Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: report.Writer{Out: &out, Err: &diag}}
			if code := r.Once(t.Context()); code != test.want {
				t.Fatalf("code=%d", code)
			}
			var result report.Result
			if bytes.Count(out.Bytes(), []byte("\n")) != 1 || json.Unmarshal(out.Bytes(), &result) != nil || len(result.Changes) != 0 || len(repo.Records) != 0 {
				t.Fatal("no-action/failure created incident output")
			}
		})
	}
}
func TestModelDoesNotBlockFreshnessOrCauseRenewalReports(t *testing.T) {
	source := &fakeSource{objects: []health.Object{failedPod()}, events: make(chan struct{}, 1)}
	ai := &fakeAI{started: make(chan struct{}, 1), release: make(chan struct{})}
	repo := &incident.Memory{Log: io.Discard}
	var out bytes.Buffer
	r := Runner{Config: options(), Source: source, Analyzer: ai, Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: report.Writer{Out: &out, Err: io.Discard}}
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
func TestSchedulingBoundariesAndDayOfCounterChurn(t *testing.T) {
	cfg := options()
	cfg.BatchWindow = 5 * time.Second
	base := time.Now()
	pod := failedPod()
	inputs, err := incident.Inputs(health.Snapshot{At: base, Objects: []health.Object{pod}}, cfg.Policy, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	states := map[string]*candidate{}
	updateCandidates(states, inputs, nil, base, cfg)
	key := incident.Ref(pod.Resource)
	c := states[key]
	if !c.next.Equal(base.Add(5 * time.Second)) {
		t.Fatal("batch deadline incorrect")
	}
	c.lastReview, c.lastRevision, c.next = base, inputs[key].Revision, base.Add(cfg.ReviewInterval)
	for cycle := 1; cycle <= 288; cycle++ {
		at := base.Add(time.Duration(cycle) * 5 * time.Minute)
		in := inputs[key]
		in.Signals = append(in.Signals, incident.Signal{ID: "event", Rule: "event.warning", Hint: "info", Evidence: []string{"count changed"}})
		in.Revision = incident.Fingerprint(in)
		updateCandidates(states, map[string]incident.Input{key: in}, nil, at, cfg)
		if c.lastRevision != in.Revision || !c.next.Equal(base.Add(cfg.ReviewInterval)) {
			t.Fatal("counter change rescheduled AI")
		}
	}
	open := []incident.Record{{Value: incident.Incident{Identity: incident.Identity{Observer: "fixture", Anchor: pod.Resource, Family: "container-failure"}}}}
	empty := inputs[key]
	empty.Signals = nil
	empty.Revision = incident.Fingerprint(empty)
	updateCandidates(states, map[string]incident.Input{key: empty}, open, base, cfg)
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
	updateCandidates(states, map[string]incident.Input{key: empty}, open, base.Add(time.Minute), cfg)
	if !c.recoverySince.Equal(base.Add(time.Minute)) {
		t.Fatal("disconnection did not reset continuous recovery")
	}
	c.lastReview = base.Add(4 * time.Minute)
	c.next = c.lastReview.Add(cfg.ReviewInterval)
	updateCandidates(states, map[string]incident.Input{key: empty}, open, base.Add(5*time.Minute), cfg)
	if !c.next.Equal(c.lastReview.Add(cfg.ReviewInterval)) {
		t.Fatal("AI no-action during recovery caused an immediate re-review loop")
	}
}
