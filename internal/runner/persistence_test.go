/*
Purpose: Verify watch remains responsive through failed and ambiguous Incident writes.
Flow: Offline collector, AI and repository fixtures retain one proposal, pause new
model calls, preserve partial progress and reject stale/recovery-discontinuous retries.
Graceful cancellation and exact storage backoff are checked without live resources.
*/
package runner

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/output"
)

type watchWriteRepository struct {
	recoveryRepository
	fail       atomic.Bool
	getDenied  atomic.Bool
	failFamily string
	lostReply  bool
	attempts   chan incident.Record
}

type writeAI struct {
	calls atomic.Int32
	multi bool
}

type countingSource struct {
	*fakeSource
	snapshots atomic.Int32
}

type persistenceOutput struct {
	mu        sync.Mutex
	buffer    bytes.Buffer
	recovered chan struct{}
}

// Write captures fixture output and signals confirmed recovery without racing live buffer reads.
func (w *persistenceOutput) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if bytes.Contains(data, []byte("incident_persistence_recovered")) {
		select {
		case w.recovered <- struct{}{}:
		default:
		}
	}

	return w.buffer.Write(data)
}

// String reads captured fixture output after or during watch execution under the writer lock.
func (w *persistenceOutput) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.buffer.String()
}

// Get injects a failed ambiguous-write reread while preserving the synchronized fixture store.
func (r *watchWriteRepository) Get(ctx context.Context, name string) (*incident.Record, error) {
	if r.getDenied.Load() {
		return nil, errors.New("fixture incident lookup unavailable")
	}

	return r.recoveryRepository.Get(ctx, name)
}

// Save captures proposed revisions and optionally loses the reply to a committed write.
func (r *watchWriteRepository) Save(ctx context.Context, record incident.Record) error {
	failing := r.fail.Load() && (r.failFamily == "" || r.failFamily == record.Value.Identity.Family)
	r.attempts <- record

	if failing {
		if r.lostReply {
			r.mu.Lock()
			err := r.memory.Save(ctx, record)
			r.mu.Unlock()

			if err != nil {
				return err
			}

			r.getDenied.Store(true)
		}

		return errors.New("fixture Incident write unavailable; check CRD/RBAC")
	}

	return r.recoveryRepository.Save(ctx, record)
}

// Investigate returns an offline validated material proposal and counts every model assessment.
func (a *writeAI) Investigate(_ context.Context, in incident.InvestigationContext) (incident.Decision, error) {
	a.calls.Add(1)

	action := incident.Action{Operation: "raise", Family: "container-failure", ResourceRefs: []string{in.Anchor},
		Title: "Workload failed", Severity: "medium", Impact: "degraded", Diagnosis: "workload-failed",
		Explanation: "Observed workload failure.", EvidenceIDs: []string{"snapshot"},
		Recommendations: []string{"Inspect the workload."}, Uncertainty: "Root cause unconfirmed.", MaterialChange: true}

	actions := []incident.Action{action}
	if a.multi {
		action.Family = "scheduling"
		actions = append(actions, action)
	}

	decision := incident.Decision{Outcome: "decisions", Reason: "Offline AI fixture approves the incident", Actions: actions}
	if err := incident.Validate(decision, in, map[string]bool{"snapshot": true}); err != nil {
		return incident.Decision{}, err
	}

	return decision, nil
}

// Snapshot counts continuing observations while storage retries are pending.
func (s *countingSource) Snapshot(at time.Time) (health.Snapshot, error) {
	s.snapshots.Add(1)

	return s.fakeSource.Snapshot(at)
}

// startWriteWatch starts only offline workers and returns an idempotent, checked shutdown.
func startWriteWatch(t *testing.T, r *Runner) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan int, 1)
	go func() { done <- r.Watch(ctx) }()

	var once sync.Once

	stop := func() {
		once.Do(func() {
			cancel()

			select {
			case code := <-done:
				if code != 0 {
					t.Errorf("watch terminated on persistence failure: exit=%d", code)
				}
			case <-time.After(3 * time.Second):
				t.Error("watch did not stop promptly")
			}
		})
	}
	t.Cleanup(stop)

	return stop
}

// awaitWrite waits for an expected bounded fixture attempt or fails without accessing a cluster.
func awaitWrite(t *testing.T, attempts <-chan incident.Record) incident.Record {
	t.Helper()

	select {
	case record := <-attempts:
		return record
	case <-time.After(3 * time.Second):
		t.Fatal("expected Incident write attempt did not occur")
		return incident.Record{}
	}
}

// TestWatchPersistenceRecovery retains exact proposals, continues observation and repairs storage without repeated AI calls.
func TestWatchPersistenceRecovery(t *testing.T) {
	repo := &watchWriteRepository{attempts: make(chan incident.Record, 32)}
	repo.saved = make(chan incident.Record, 16)
	repo.fail.Store(true)

	ai := &writeAI{}
	source := &countingSource{fakeSource: &fakeSource{objects: []health.Object{failedPod()}, events: make(chan struct{}, 1)}}
	cfg := options()
	cfg.ReviewInterval = 50 * time.Millisecond
	out := persistenceOutput{recovered: make(chan struct{}, 1)}

	var diag bytes.Buffer

	r := Runner{Config: cfg, Source: source, Analyzer: ai,
		Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: &output.Writer{Out: &out, Err: &diag}}
	stop := startWriteWatch(t, &r)

	desired := awaitWrite(t, repo.attempts)
	for range 2 {
		attempt := awaitWrite(t, repo.attempts)
		if !reflect.DeepEqual(attempt.Value, desired.Value) {
			t.Fatal("immediate retry changed the desired revision")
		}
	}

	snapshots, lists := source.snapshots.Load(), repo.lists.Load()

	time.Sleep(100 * time.Millisecond)

	if ai.calls.Load() != 1 || source.snapshots.Load() <= snapshots || repo.lists.Load() <= lists {
		t.Fatal("storage failure reran AI or stopped independent observations/index reads")
	}

	repo.fail.Store(false)
	saved := awaitWrite(t, repo.saved)

	stop()

	if saved.Name != desired.Name || !reflect.DeepEqual(saved.Value, desired.Value) || ai.calls.Load() != 1 ||
		!strings.Contains(diag.String(), "incident_persistence_deferred") ||
		!strings.Contains(out.String(), "incident_persistence_recovered") {
		t.Fatalf("watch recovery did not retain the exact AI proposal: %s %s", &out, &diag)
	}
}

// TestWatchReconcilesDelayedAcknowledgement confirms one committed record after lookup recovers, without another create or model request.
func TestWatchReconcilesDelayedAcknowledgement(t *testing.T) {
	repo := &watchWriteRepository{attempts: make(chan incident.Record, 32), lostReply: true}
	repo.fail.Store(true)

	ai := &writeAI{}
	source := &fakeSource{objects: []health.Object{failedPod()}, events: make(chan struct{}, 1)}
	cfg := options()
	cfg.ReviewInterval = 50 * time.Millisecond
	out := persistenceOutput{recovered: make(chan struct{}, 1)}

	var diag bytes.Buffer

	r := Runner{Config: cfg, Source: source, Analyzer: ai,
		Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: &output.Writer{Out: &out, Err: &diag}}
	stop := startWriteWatch(t, &r)
	_ = awaitWrite(t, repo.attempts)

	time.Sleep(150 * time.Millisecond)
	repo.getDenied.Store(false)

	select {
	case <-out.recovered:
	case <-time.After(3 * time.Second):
		t.Fatal("pending acknowledgement did not recover")
	}

	stop()

	if ai.calls.Load() != 1 || len(repo.attempts) != 0 ||
		!strings.Contains(out.String(), "persisted raise") || !strings.Contains(out.String(), "incident_persistence_recovered") {
		t.Fatalf("ambiguous acknowledgement repeated AI/writes or was not confirmed: %s %s", &out, &diag)
	}
}

// TestWatchDropsChangedProposal does not wait for storage backoff before discarding stale evidence.
func TestWatchDropsChangedProposal(t *testing.T) {
	repo := &watchWriteRepository{attempts: make(chan incident.Record, 32)}
	repo.fail.Store(true)

	ai := &writeAI{}
	source := &fakeSource{objects: []health.Object{failedPod()}, events: make(chan struct{}, 1)}
	cfg := options()

	var out, diag bytes.Buffer

	r := Runner{Config: cfg, Source: source, Analyzer: ai,
		Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: &output.Writer{Out: &out, Err: &diag}}

	stop := startWriteWatch(t, &r)
	for range 3 {
		_ = awaitWrite(t, repo.attempts)
	}

	healthy := failedPod()
	healthy.Phase = "Succeeded"
	source.set([]health.Object{healthy}, nil)
	repo.fail.Store(false)
	time.Sleep(100 * time.Millisecond)
	stop()

	records, err := repo.ListOpen(t.Context())
	if err != nil || len(records) != 0 || ai.calls.Load() != 1 ||
		!strings.Contains(diag.String(), "pending application superseded") || strings.Contains(diag.String(), "Stopped with pending") {
		t.Fatalf("stale proposal was persisted or remained parked for two seconds: %s", &diag)
	}
}

// TestWatchRetainsPartialWrites resumes only unfinished families while keeping confirmed index changes.
func TestWatchRetainsPartialWrites(t *testing.T) {
	repo := &watchWriteRepository{attempts: make(chan incident.Record, 32), failFamily: "scheduling"}
	repo.saved = make(chan incident.Record, 16)
	repo.fail.Store(true)

	ai := &writeAI{multi: true}
	source := &fakeSource{objects: []health.Object{failedPod()}, events: make(chan struct{}, 1)}
	cfg := options()
	cfg.ReviewInterval = 50 * time.Millisecond

	var out, diag bytes.Buffer

	r := Runner{Config: cfg, Source: source, Analyzer: ai,
		Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: &output.Writer{Out: &out, Err: &diag}}
	stop := startWriteWatch(t, &r)

	first := awaitWrite(t, repo.saved)
	for range 4 {
		_ = awaitWrite(t, repo.attempts)
	}

	repo.fail.Store(false)
	second := awaitWrite(t, repo.saved)

	stop()

	records, err := repo.ListOpen(t.Context())
	if err != nil || len(records) != 2 || first.Name == second.Name || ai.calls.Load() != 1 ||
		strings.Count(out.String(), "persisted raise "+first.Name) != 1 ||
		strings.Count(out.String(), "persisted raise "+second.Name) != 1 {
		t.Fatalf("partial progress was lost or replayed: %s %s", &out, &diag)
	}
}

// TestPersistenceBackoff preserves exact existing retry thresholds and explicitly pauses model work.
func TestPersistenceBackoff(t *testing.T) {
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)

	for _, tt := range []struct {
		failures int
		delay    time.Duration
	}{{0, 2 * time.Second}, {1, 4 * time.Second}, {2, 15 * time.Minute}, {8, 15 * time.Minute}} {
		var out, diag bytes.Buffer

		r := Runner{Config: options(), Output: &output.Writer{Out: &out, Err: &diag}}

		pending := &pendingWrite{result: completion{ctx: t.Context()}, failures: tt.failures}
		if err := r.deferPersistence(pending, errors.New("fixture write denied"), now); err != nil ||
			!pending.next.Equal(now.Add(tt.delay)) || !strings.Contains(diag.String(), "new AI calls paused") || out.Len() != 0 {
			t.Fatalf("wrong persistence backoff for failure count=%d: %s", tt.failures, &diag)
		}
	}
}

// TestPendingResolutionRequiresOriginalRecoveryWindow rejects cached resolutions after observation continuity is reset.
func TestPendingResolutionRequiresOriginalRecoveryWindow(t *testing.T) {
	cfg := options()
	record := recoveryRecord()
	source := &fakeSource{continuity: 1, objects: []health.Object{{Resource: record.Value.Identity.Anchor,
		Phase: "Running", Conditions: []health.Condition{{Type: "Ready", Status: "True"}}}}}
	repo := &recoveryRepository{}
	repo.put(record)
	open := []incident.Record{record}

	snapshot, err := source.Snapshot(time.Now())
	if err != nil {
		t.Fatal(err)
	}

	contexts, err := incident.BuildInvestigationContexts(snapshot, cfg.Policy, open, cfg.MaxObjects)
	if err != nil {
		t.Fatal(err)
	}

	key := incident.Ref(record.Value.Identity.Anchor)
	in := contexts[key]

	in.Incidents, err = (incident.Reconciler{Repository: repo, Observer: "fixture"}).Existing(t.Context(), record.Value.Identity.Anchor)
	if err != nil {
		t.Fatal(err)
	}

	since := time.Now().Add(-cfg.RecoveryWindow)
	in.RecoveryEligible = true
	in.Recovery.Since = &since
	decision := recoveryDecision(in)

	application, err := (incident.Reconciler{Repository: repo, Observer: "fixture"}).NewApplication(in, decision)
	if err != nil {
		t.Fatal(err)
	}

	pending := &pendingWrite{result: completion{ctx: t.Context(), investigation: in, decision: decision}, application: application}
	c := &candidate{investigation: in, open: true, recoverySince: since}
	resetRecovery(map[string]*candidate{key: c})

	r := Runner{Config: cfg, Source: source}

	changes, err := r.applyPending(t.Context(), pending, c, open, func() bool { return true })
	if err == nil || len(changes) != 0 {
		t.Fatal("lost recovery continuity permitted a cached resolution")
	}

	records, err := repo.ListOpen(t.Context())
	if err != nil || len(records) != 1 || records[0].Value.Lifecycle.State != "open" {
		t.Fatal("stale resolution changed the original open Incident")
	}
}
