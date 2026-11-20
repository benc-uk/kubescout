/*
Purpose: Test runner scope, no-action handling and review scheduling offline.
Flow: Fake collection and AI sources feed once/watch execution and candidate
updates, checking failure output, nonblocking collection, counter-noise
suppression and recovery timing without live model or Kubernetes calls.
Fixture index reads have explicit deadlines for the independent refresh worker.
Startup failures must not claim collection or Incident-index readiness.
Read-only fixtures omit Incident repositories entirely and check proposal/no-action
coalescing, stale-result rejection and visible collection/model/logging failures.
Normal proposal capture and its opt-out never replace CRD lifecycle; file failures warn without blocking writes.
*/
package runner

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
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
	mu              sync.Mutex
	objects         []health.Object
	err             error
	events          chan struct{}
	continuity      uint64
	recoveryChanges map[health.Resource]uint64
}

type fakeAI struct {
	calls   atomic.Int32
	fail    bool
	started chan struct{}
	release chan struct{}
}

type staleProposalAI struct {
	source *fakeSource
	ai     *writeAI
}

// Investigate returns a fixture proposal after changing its evidence so freshness must reject it.
func (a staleProposalAI) Investigate(ctx context.Context, in incident.InvestigationContext) (incident.Decision, error) {
	decision, err := a.ai.Investigate(ctx, in)
	a.source.set(nil, nil)

	return decision, err
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

	return health.Snapshot{At: at, Objects: append([]health.Object{}, s.objects...), Continuity: s.continuity,
		RecoveryChanges: maps.Clone(s.recoveryChanges)}, s.err
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
	return config.Config{ObserverID: "fixture", Model: "fixture", NamespaceWatch: []string{"apps"}, MaxObjects: 100, MaxToolCalls: config.DefaultMaxToolCalls, RunTimeout: time.Second,
		RequestTimeout: time.Second, BatchWindow: 5 * time.Millisecond, CheckInterval: 10 * time.Millisecond, Cooldown: 2 * time.Minute, RecoveryWindow: 2 * time.Minute, ReviewInterval: 15 * time.Minute, Policy: health.DefaultPolicy()}
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

// TestOnceNoActionAndFailuresNeverCreateIncidents checks readable outcomes and zero stored incidents for non-decisions.
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

			r := Runner{Config: options(), Source: source, Analyzer: ai, Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: &output.Writer{Out: &out, Err: &diag}}
			if code := r.Once(t.Context()); code != test.want {
				t.Fatalf("code=%d", code)
			}

			if !strings.Contains(out.String()+diag.String(), "[GO][EXECUTION]") || strings.Contains(out.String(), "schema_version") || len(repo.Records) != 0 {
				t.Fatal("missing summary or no-action/failure created an incident")
			}

			if test.want == 0 && !strings.Contains(out.String(), "[AI][DECISION] inv=1 Pod apps/bad no_action") {
				t.Fatal("AI no-action provenance missing")
			}

			if test.collection && strings.Contains(out.String(), "Collection ready:") {
				t.Fatal("failed collection claimed readiness")
			}
		})
	}
}

// TestStartupIndexFailureDoesNotClaimReadiness rejects out-of-scope fixture records
// before once/watch collection or AI work and never logs a successful startup load.
func TestStartupIndexFailureDoesNotClaimReadiness(t *testing.T) {
	for _, mode := range []string{"once", "watch"} {
		t.Run(mode, func(t *testing.T) {
			resource := failedPod().Resource
			resource.Namespace = "outside"
			repo := &incident.Memory{Records: map[string]incident.Record{"existing": {
				Value: incident.Incident{Identity: incident.Identity{Anchor: resource}, Lifecycle: incident.Lifecycle{State: "open"}},
			}}}
			ai := &fakeAI{}

			var out, diag bytes.Buffer

			r := Runner{Config: options(), Source: &fakeSource{objects: []health.Object{failedPod()}}, Analyzer: ai,
				Incidents: incident.Reconciler{Repository: repo}, Output: &output.Writer{Out: &out, Err: &diag}}

			run := r.Once
			if mode == "watch" {
				run = r.Watch
			}

			if run(t.Context()) != 1 || ai.calls.Load() != 0 || len(repo.Records) != 1 {
				t.Fatal("failed startup continued AI work or changed stored incidents")
			}

			if strings.Contains(out.String(), "Incident index ready:") ||
				strings.Contains(strings.ToLower(out.String()), "collection ready:") || diag.Len() == 0 {
				t.Fatalf("startup failure milestones were misleading: %s %s", &out, &diag)
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

	r := Runner{Config: options(), Source: source, Analyzer: ai, Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: &output.Writer{Out: &out, Err: io.Discard}}
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

	if strings.Contains(out.String(), "[GO][INCIDENT]") || len(repo.Records) != 0 {
		t.Fatal("model/collection transitions created incidents")
	}
}

// TestWatchActivityCoalesces verifies unchanged collection ticks emit one summary and never invent AI no-action.
func TestWatchActivityCoalesces(t *testing.T) {
	source := &fakeSource{objects: []health.Object{{Resource: health.Resource{Kind: "Node", Name: "ready", UID: "node"},
		Conditions: []health.Condition{{Type: "Ready", Status: "True"}}}}}
	ai := &fakeAI{}
	cfg := options()
	cfg.CheckInterval, cfg.BatchWindow = time.Millisecond, time.Millisecond

	var out, diag bytes.Buffer

	r := Runner{Config: cfg, Source: source, Analyzer: ai,
		Incidents: incident.Reconciler{Repository: &incident.Memory{}, Observer: "fixture"},
		Output:    &output.Writer{Out: &out, Err: &diag}}

	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
	defer cancel()

	if r.Watch(ctx) != 0 || ai.calls.Load() != 0 {
		t.Fatal("unchanged healthy evidence triggered AI or failed shutdown")
	}

	if strings.Count(out.String(), "[GO][CHECK]") != 1 || strings.Count(out.String(), "Watching:") != 1 ||
		strings.Contains(out.String(), "[AI]") || diag.Len() != 0 {
		t.Fatalf("unchanged ticks flooded logs or invented a model opinion: %s %s", &out, &diag)
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

// TestNormalProposalCapturePreservesPersistence logs normal once/watch proposals
// while retaining real fixture writes when capture is disabled or its files are unavailable.
func TestNormalProposalCapturePreservesPersistence(t *testing.T) {
	for _, mode := range []string{"once", "watch"} {
		for _, capture := range []string{"enabled", "disabled", "unavailable", "no-action"} {
			t.Run(mode+"/"+capture, func(t *testing.T) {
				cfg := options()
				cfg.ProposalLog = capture != "disabled"
				source := &fakeSource{objects: []health.Object{failedPod()}}
				repo := &recoveryRepository{memory: incident.Memory{}}

				var ai Analyzer = &writeAI{}

				if capture == "no-action" {
					ai = &fakeAI{}
				}

				dir := t.TempDir()
				if capture == "unavailable" {
					dir = filepath.Join(dir, "blocked")
					if err := os.WriteFile(dir, []byte("retain"), 0600); err != nil {
						t.Fatal(err)
					}
				}

				var out, diag bytes.Buffer

				sink := &output.Writer{Out: &out, Err: &diag}

				r := Runner{Config: cfg, Source: source, Analyzer: ai, Output: sink,
					Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}}
				if cfg.ProposalLog {
					r.Proposals = output.NewProposalLog(dir, sink)
				}

				ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
				defer cancel()

				run := r.Once
				if mode == "watch" {
					run = r.Watch
				}

				if run(ctx) != 0 {
					t.Fatalf("proposal capture changed normal execution outcome: %s", &diag)
				}

				wantRecords := 1
				if capture == "no-action" {
					wantRecords = 0
				}

				if len(repo.memory.Records) != wantRecords {
					t.Fatal("proposal capture replaced or blocked normal incident persistence")
				}

				data, err := os.ReadFile(filepath.Join(dir, "ai-proposals.log"))
				if capture == "enabled" || capture == "no-action" {
					if err != nil || !bytes.Contains(data, []byte("Mode: CRD-backed; proposal capture does not confirm persistence")) ||
						bytes.Contains(data, []byte("no Incident API access")) {
						t.Fatal("normal proposals were omitted or falsely labelled read-only")
					}
				} else if err == nil {
					t.Fatal("disabled or failed capture pretended to write proposals")
				}

				if capture == "unavailable" && !strings.Contains(diag.String(), "AI proposal capture incomplete") {
					t.Fatal("normal proposal log failure was silent")
				}
			})
		}
	}
}

// TestReadOnlyAssessmentsBypassIncidentStorage exercises once/watch without any
// repository, retaining no-action suppression and rejecting stale or failed diagnostic output.
func TestReadOnlyAssessmentsBypassIncidentStorage(t *testing.T) {
	for _, mode := range []string{"once", "watch"} {
		for _, outcome := range []string{"proposal", "no-action", "stale", "log failure", "collection failure", "model failure"} {
			t.Run(mode+"/"+outcome, func(t *testing.T) {
				cfg := options()
				cfg.ReadOnly = true
				source := &fakeSource{objects: []health.Object{failedPod()}}
				source.objects[0].UID = "host-uid-must-not-be-logged"
				proposalAI := &writeAI{}
				noActionAI := &fakeAI{fail: outcome == "model failure"}

				var ai Analyzer = proposalAI

				if outcome == "no-action" || outcome == "model failure" {
					ai = noActionAI
				} else if outcome == "stale" {
					ai = staleProposalAI{source: source, ai: proposalAI}
				}

				if outcome == "collection failure" {
					source.err = errors.New("fixture collection unavailable")
				}

				dir := t.TempDir()
				if outcome == "log failure" {
					dir = filepath.Join(dir, "blocked")
					if err := os.WriteFile(dir, []byte("retain"), 0600); err != nil {
						t.Fatal(err)
					}
				}

				var out, diag bytes.Buffer

				log := output.NewProposalLog(dir, nil)
				r := Runner{Config: cfg, Source: source, Analyzer: ai, Proposals: log,
					Output: &output.Writer{Out: &out, Err: &diag}}

				ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
				defer cancel()

				run := r.Once
				if mode == "watch" {
					run = r.Watch
				}

				code := run(ctx)
				success := outcome == "proposal" || outcome == "no-action"

				wantCode := 1
				if success || mode == "watch" && outcome != "log failure" {
					wantCode = 0
				}

				if code != wantCode || proposalAI.calls.Load()+noActionAI.calls.Load() > 1 {
					t.Fatalf("read-only failure or unchanged evidence retriggered AI: code=%d stderr=%s", code, &diag)
				}

				data, err := os.ReadFile(log.Path())
				if success && (err != nil || len(data) == 0) || !success && err == nil {
					t.Fatalf("read-only diagnostic outcome incorrect: success=%t file error=%v", success, err)
				}

				if outcome == "proposal" {
					for _, detail := range []string{"Title: Workload failed", "Diagnosis: workload-failed",
						"Recommendation: Inspect the workload.", `Affected resource: group="" Pod apps/bad`} {
						if !bytes.Contains(data, []byte(detail)) {
							t.Fatalf("proposal log omitted approved incident detail %q", detail)
						}
					}
				} else if outcome == "no-action" && !bytes.Contains(data, []byte("Outcome: no_action")) {
					t.Fatal("read-only no-action was not recorded as a model conclusion")
				}

				if bytes.Contains(data, []byte("host-uid-must-not-be-logged")) ||
					strings.Contains(out.String(), "persisted raise") || strings.Contains(out.String(), "Incident index ready:") {
					t.Fatal("read-only assessment exposed a UID or pretended to persist/load Incidents")
				}

				if !success && diag.Len() == 0 {
					t.Fatal("failed read-only assessment was silent")
				}
			})
		}
	}
}
