/*
Purpose: Test unified recovery scheduling, index refresh and stale-result rejection.
Flow: Offline clock-controlled candidates and synchronized repository/AI fixtures
cover exact five-minute boundaries, replacement and collection continuity, late
Incident discovery, index outages and AI no-action/failure. No live resources,
model requests or deterministic Incident resolutions are used.
Bounded read cancellation, pre-write index responses and once/restart tracking are exercised explicitly.
Recovery no-action logs distinguish retained open Incidents from confirmed resolution.
*/
package runner

import (
	"context"
	"errors"
	"io"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/output"
)

type recoveryRepository struct {
	mu         sync.Mutex
	memory     incident.Memory
	failAfter  int32
	blockAfter int32
	block      chan struct{}
	blocked    chan struct{}
	lists      atomic.Int32
	failures   atomic.Int32
	active     atomic.Int32
	concurrent atomic.Bool
	saved      chan incident.Record
}

type recoveryAI struct {
	calls    chan incident.InvestigationContext
	release  chan struct{}
	noAction bool
	fail     bool
}

// ListOpen provides synchronized complete reads or explicit fixture failures and detects overlapping refreshes.
func (r *recoveryRepository) ListOpen(ctx context.Context) ([]incident.Record, error) {
	count := r.lists.Add(1)
	if r.active.Add(1) > 1 {
		r.concurrent.Store(true)
	}
	defer r.active.Add(-1)

	r.mu.Lock()
	if r.failAfter != 0 && count > r.failAfter {
		r.failures.Add(1)
		r.mu.Unlock()

		return nil, errors.New("fixture incident index unavailable")
	}

	records, err := r.memory.ListOpen(ctx)
	block, blockAfter, blocked := r.block, r.blockAfter, r.blocked
	r.mu.Unlock()

	if block != nil && count >= blockAfter {
		select {
		case blocked <- struct{}{}:
		default:
		}

		select {
		case <-block:
		case <-ctx.Done():
			r.failures.Add(1)
			return nil, ctx.Err()
		}
	}

	return records, err
}

// Get reads a fixture record under the same lock used by periodic lists and AI-approved writes.
func (r *recoveryRepository) Get(ctx context.Context, name string) (*incident.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.memory.Get(ctx, name)
}

// Save records only reconciler writes and exposes confirmed changes to asynchronous assertions.
func (r *recoveryRepository) Save(ctx context.Context, record incident.Record) error {
	r.mu.Lock()
	err := r.memory.Save(ctx, record)
	r.mu.Unlock()

	if err == nil && r.saved != nil {
		r.saved <- record
	}

	return err
}

// put adds an existing Incident fixture without presenting it as a runner-approved write.
func (r *recoveryRepository) put(record incident.Record) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.memory.Records == nil {
		r.memory.Records = map[string]incident.Record{}
	}

	r.memory.Records[record.Name] = record
}

// recoverIndex permits later complete reads after the configured periodic-read failure.
func (r *recoveryRepository) recoverIndex() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.failAfter = 0
}

// Investigate emits a validated fixture resolution, explicit no-action or a model failure.
func (a *recoveryAI) Investigate(ctx context.Context, in incident.InvestigationContext) (incident.Decision, error) {
	select {
	case a.calls <- in:
	case <-ctx.Done():
		return incident.Decision{}, ctx.Err()
	}

	if a.release != nil {
		select {
		case <-a.release:
		case <-ctx.Done():
			return incident.Decision{}, ctx.Err()
		}
	}

	if a.fail {
		return incident.Decision{}, errors.New("fixture AI unavailable")
	}

	if a.noAction {
		return incident.Decision{Outcome: "no_action", Reason: "Recovery impact remains uncertain", Actions: []incident.Action{}}, nil
	}

	decision := recoveryDecision(in)
	if err := incident.Validate(decision, in, map[string]bool{"snapshot": true}); err != nil {
		return incident.Decision{}, err
	}

	return decision, nil
}

// recoveryDecision supplies a strict resolution referencing the reviewed Incident and snapshot citation.
func recoveryDecision(in incident.InvestigationContext) incident.Decision {
	actions := []incident.Action{}

	for _, existing := range in.Incidents {
		if existing.Lifecycle.State == "open" {
			actions = append(actions, incident.Action{Operation: "resolve", IncidentID: existing.ID, Family: existing.Family,
				ResourceRefs: []string{in.Anchor}, Title: "Previous condition ended", Severity: "medium", Impact: "limited",
				Diagnosis: "previous-condition-ended", Explanation: "The reviewed condition no longer applies after stable observation",
				EvidenceIDs: []string{"snapshot"}, Recommendations: []string{"Continue observation"}, Uncertainty: "Application traffic is not tested",
				MaterialChange: true})
		}
	}

	return incident.Decision{Outcome: "decisions", Reason: "AI fixture approves resolution after reviewing recovery", Actions: actions}
}

// recoveryRecord builds an old but still-open Incident; its age never establishes recovery eligibility.
func recoveryRecord() incident.Record {
	identity := incident.Identity{Observer: "fixture", Anchor: failedPod().Resource, Family: "container-failure"}

	return incident.Record{Name: incident.Name(identity), Version: "1", UID: "incident-uid", Value: incident.Incident{
		Identity: identity, Resources: []health.Resource{identity.Anchor},
		Analysis: incident.Analysis{Title: "Previous container failure", Severity: "medium", Impact: "degraded", Diagnosis: "container-failed",
			Explanation: "Previously observed failure", EvidenceIDs: []string{"snapshot"}, Recommendations: []string{"Investigate"},
			Uncertainty: "Cause unknown", Model: "fixture", PromptVersion: incident.PromptVersion},
		Lifecycle: incident.Lifecycle{State: "open", Revision: 1, RaisedAt: time.Now().Add(-24 * time.Hour)}}}
}

// setSnapshot atomically changes fixture continuity and history before issuing a coalesced notification.
func (s *fakeSource) setSnapshot(snapshot health.Snapshot, err error) {
	s.mu.Lock()
	s.objects, s.err = snapshot.Objects, err
	s.continuity, s.recoveryChanges = snapshot.Continuity, maps.Clone(snapshot.RecoveryChanges)
	s.mu.Unlock()

	select {
	case s.events <- struct{}{}:
	default:
	}
}

// startRecoveryWatch owns cancellation and joins the watch and all its collection, index and AI workers.
func startRecoveryWatch(t *testing.T, r *Runner) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan int, 1)
	go func() { done <- r.Watch(ctx) }()

	t.Cleanup(func() {
		cancel()

		if code := <-done; code != 0 {
			t.Errorf("watch failed: exit=%d", code)
		}
	})
}

// awaitRecoveryCall waits only for an expected asynchronous fixture review, with a bounded test deadline.
func awaitRecoveryCall(t *testing.T, ai *recoveryAI) incident.InvestigationContext {
	t.Helper()

	select {
	case in := <-ai.calls:
		return in
	case <-time.After(2 * time.Second):
		t.Fatal("recovery AI review did not start")
		return incident.InvestigationContext{}
	}
}

// TestUnifiedRecoveryWindowBoundaries exercises the actual eligibility helper and reconciler at the exact five-minute boundary.
func TestUnifiedRecoveryWindowBoundaries(t *testing.T) {
	for _, present := range []bool{true, false} {
		t.Run(map[bool]string{true: "present", false: "absent"}[present], func(t *testing.T) {
			cfg := options()
			cfg.RecoveryWindow = 5 * time.Minute
			base := time.Date(2026, time.November, 7, 12, 0, 0, 0, time.UTC)
			record := recoveryRecord()
			repo := &recoveryRepository{}
			repo.put(record)
			open := []incident.Record{record}

			snapshot := health.Snapshot{At: base, Continuity: 1}
			if present {
				snapshot.Objects = []health.Object{{Resource: record.Value.Identity.Anchor, Phase: "Running",
					Conditions: []health.Condition{{Type: "Ready", Status: "True"}}}}
			}

			contexts, err := incident.BuildInvestigationContexts(snapshot, cfg.Policy, open, cfg.MaxObjects)
			if err != nil {
				t.Fatal(err)
			}

			states := map[string]*candidate{}
			updateCandidates(states, contexts, open, base, cfg)

			key := incident.Ref(record.Value.Identity.Anchor)
			in := contexts[key]
			reconciler := incident.Reconciler{Repository: repo, Observer: "fixture"}

			in.Incidents, err = reconciler.Existing(t.Context(), record.Value.Identity.Anchor)
			if err != nil {
				t.Fatal(err)
			}

			decision := recoveryDecision(in)

			in.RecoveryEligible = recoveryEligible(states[key], base.Add(cfg.RecoveryWindow-time.Nanosecond), cfg.RecoveryWindow)
			if in.RecoveryEligible || incident.Validate(decision, in, map[string]bool{"snapshot": true}) == nil {
				t.Fatal("resolution accepted before continuous window elapsed")
			}

			if _, err := reconciler.Apply(t.Context(), in, decision, func() bool { return true }); err == nil {
				t.Fatal("persistence bypassed recovery timing")
			}

			in.RecoveryEligible = recoveryEligible(states[key], base.Add(cfg.RecoveryWindow), cfg.RecoveryWindow)
			if err := incident.Validate(decision, in, map[string]bool{"snapshot": true}); err != nil {
				t.Fatal(err)
			}

			changes, err := reconciler.Apply(t.Context(), in, decision, func() bool { return true })
			if err != nil || len(changes) != 1 || changes[0].Name != record.Name ||
				changes[0].Incident.Lifecycle.State != "resolved" || changes[0].Incident.Lifecycle.Revision != 2 {
				t.Fatalf("boundary resolution: changes=%+v err=%v", changes, err)
			}
		})
	}
}

// TestRecoveryResetsOnFlapsAndContinuity checks renewed failure, hidden transitions, rebuilding, replacement and reappearance.
func TestRecoveryResetsOnFlapsAndContinuity(t *testing.T) {
	cfg := options()
	cfg.RecoveryWindow = 5 * time.Minute
	base := time.Now()
	record := recoveryRecord()
	open := []incident.Record{record}
	pod := health.Object{Resource: record.Value.Identity.Anchor, Phase: "Running", Conditions: []health.Condition{{Type: "Ready", Status: "True"}}}
	name := pod.Resource
	name.UID = ""
	states := map[string]*candidate{}
	key := incident.Ref(pod.Resource)
	snapshot := health.Snapshot{At: base, Objects: []health.Object{pod}, Continuity: 1, RecoveryChanges: map[health.Resource]uint64{name: 0}}

	for step := 0; step <= 8; step++ {
		now := base.Add(time.Duration(step) * time.Minute)
		snapshot.At = now

		switch step {
		case 2:
			snapshot.Objects[0].Phase = "Failed"
		case 3:
			snapshot.Objects[0].Phase = "Running"
		case 4:
			snapshot.RecoveryChanges[name] = 2
		case 5:
			snapshot.Continuity++
		case 6:
			snapshot.Objects[0].UID = "replacement"
		case 7:
			snapshot.Objects = nil
		case 8:
			snapshot.Objects = []health.Object{pod}
		}

		contexts, err := incident.BuildInvestigationContexts(snapshot, cfg.Policy, open, cfg.MaxObjects)
		if err != nil {
			t.Fatal(err)
		}

		updateCandidates(states, contexts, open, now, cfg)

		c := states[key]
		if step == 2 {
			if !c.recoverySince.IsZero() {
				t.Fatal("active failure retained recovery eligibility")
			}
		} else if step == 1 {
			if !c.recoverySince.Equal(base) {
				t.Fatal("unchanged fresh state reset recovery")
			}
		} else if !c.recoverySince.Equal(now) {
			t.Fatalf("step %d did not reset tracking: since=%s now=%s", step, c.recoverySince, now)
		}

		if recoveryEligible(c, now, cfg.RecoveryWindow) {
			t.Fatalf("step %d became eligible too early", step)
		}
	}
}

// TestRecoveryCurrentRejectsHiddenChanges invalidates reviewed absence even when semantic fingerprints stayed unchanged.
func TestRecoveryCurrentRejectsHiddenChanges(t *testing.T) {
	record := recoveryRecord()
	open := []incident.Record{record}
	name := record.Value.Identity.Anchor
	name.UID = ""
	initial := health.Snapshot{At: time.Now(), Continuity: 1, RecoveryChanges: map[health.Resource]uint64{name: 0}}
	r := Runner{Config: options(), Source: &fakeSource{events: make(chan struct{}, 1)}}
	source := r.Source.(*fakeSource)
	source.setSnapshot(initial, nil)

	contexts, err := incident.BuildInvestigationContexts(initial, r.Config.Policy, open, r.Config.MaxObjects)
	if err != nil {
		t.Fatal(err)
	}

	in := contexts[incident.Ref(record.Value.Identity.Anchor)]

	in.RecoveryEligible = true
	if !r.current(in, open) {
		t.Fatal("fresh continued absence was rejected")
	}

	for _, mode := range []string{"epoch", "history", "replacement", "outage"} {
		t.Run(mode, func(t *testing.T) {
			snapshot := initial
			snapshot.RecoveryChanges = maps.Clone(initial.RecoveryChanges)

			var err error

			switch mode {
			case "epoch":
				snapshot.Continuity++
			case "history":
				snapshot.RecoveryChanges[name]++
			case "replacement":
				replacement := record.Value.Identity.Anchor
				replacement.UID = "new"
				snapshot.Objects = []health.Object{{Resource: replacement, Phase: "Running"}}
			case "outage":
				err = errors.New("collection unavailable")
			}

			source.setSnapshot(snapshot, err)

			if r.current(in, open) {
				t.Fatal("changed or unknown recovery observation accepted")
			}
		})
	}
}

// TestPeriodicIncidentRefreshFindsOldAbsentAnchor discovers an Incident without resource findings and persists only the AI resolution.
func TestPeriodicIncidentRefreshFindsOldAbsentAnchor(t *testing.T) {
	cfg := options()
	cfg.RecoveryWindow = 30 * time.Millisecond
	source := &fakeSource{events: make(chan struct{}, 1)}
	repo := &recoveryRepository{saved: make(chan incident.Record, 4)}
	ai := &recoveryAI{calls: make(chan incident.InvestigationContext, 8)}
	r := Runner{Config: cfg, Source: source, Analyzer: ai,
		Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: &output.Writer{Out: io.Discard, Err: io.Discard}}
	startRecoveryWatch(t, &r)

	deadline := time.Now().Add(time.Second)
	for repo.lists.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	if repo.lists.Load() < 2 {
		t.Fatal("Incident index was not refreshed independently of AI")
	}

	discovered := time.Now()
	record := recoveryRecord()
	repo.put(record)

	in := awaitRecoveryCall(t, ai)
	if !in.RecoveryEligible || in.Recovery.AnchorState != "absent" || in.Recovery.Since == nil ||
		in.Recovery.Since.Before(discovered) || in.ObservedAt.Before(in.Recovery.Since.Add(cfg.RecoveryWindow)) {
		t.Fatalf("old Incident age bypassed fresh recovery timing: %+v", in.Recovery)
	}

	select {
	case saved := <-repo.saved:
		if saved.Name != record.Name || saved.Value.Lifecycle.State != "resolved" || saved.Value.Lifecycle.Revision != 2 {
			t.Fatal("AI resolution lost Incident identity or revision")
		}
	case <-time.After(time.Second):
		t.Fatal("AI-approved resolution was not persisted")
	}

	if repo.concurrent.Load() {
		t.Fatal("overlapping Incident index refreshes")
	}
}

// TestIncidentIndexOutageRestartsRecovery pauses reviews on failed refreshes and requires a new complete recovery window.
func TestIncidentIndexOutageRestartsRecovery(t *testing.T) {
	cfg := options()
	cfg.RecoveryWindow = 100 * time.Millisecond
	repo := &recoveryRepository{failAfter: 1, saved: make(chan incident.Record, 4)}
	repo.put(recoveryRecord())

	ai := &recoveryAI{calls: make(chan incident.InvestigationContext, 8)}
	r := Runner{Config: cfg, Source: &fakeSource{events: make(chan struct{}, 1)}, Analyzer: ai,
		Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: &output.Writer{Out: io.Discard, Err: io.Discard}}
	startRecoveryWatch(t, &r)

	select {
	case <-ai.calls:
		t.Fatal("index outage was treated as fresh recovery")
	case <-time.After(2 * cfg.RecoveryWindow):
	}

	if repo.failures.Load() == 0 {
		t.Fatal("periodic index failure was not exercised")
	}

	recovered := time.Now()

	repo.recoverIndex()

	in := awaitRecoveryCall(t, ai)
	if in.Recovery.Since == nil || in.Recovery.Since.Before(recovered) || in.ObservedAt.Before(in.Recovery.Since.Add(cfg.RecoveryWindow)) {
		t.Fatal("index outage time counted toward recovery")
	}
}

// TestRecoveryNoActionAndModelFailureLeaveIncidentOpen preserves records and avoids immediate review loops.
func TestRecoveryNoActionAndModelFailureLeaveIncidentOpen(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-action", true: "failure"}[fail], func(t *testing.T) {
			cfg := options()
			cfg.RecoveryWindow = 20 * time.Millisecond
			repo := &recoveryRepository{saved: make(chan incident.Record, 4)}
			record := recoveryRecord()
			repo.put(record)

			ai := &recoveryAI{calls: make(chan incident.InvestigationContext, 8), noAction: !fail, fail: fail}
			r := Runner{Config: cfg, Source: &fakeSource{events: make(chan struct{}, 1)}, Analyzer: ai,
				Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: &output.Writer{Out: io.Discard, Err: io.Discard}}
			startRecoveryWatch(t, &r)
			awaitRecoveryCall(t, ai)

			select {
			case <-ai.calls:
				t.Fatal("recovery no-action/failure caused immediate repeated AI work")
			case <-repo.saved:
				t.Fatal("no-action/failure mutated the Incident")
			case <-time.After(80 * time.Millisecond):
			}

			stored, err := repo.Get(t.Context(), record.Name)
			if err != nil || stored == nil || stored.Value.Lifecycle.State != "open" || stored.Value.Lifecycle.Revision != 1 {
				t.Fatal("no-action/failure changed the stored lifecycle")
			}
		})
	}
}

// TestRecoveryResultRejectedAfterReappearance blocks AI until the absent resource returns with an active failure.
func TestRecoveryResultRejectedAfterReappearance(t *testing.T) {
	cfg := options()
	cfg.RecoveryWindow = 20 * time.Millisecond
	repo := &recoveryRepository{saved: make(chan incident.Record, 4)}
	repo.put(recoveryRecord())

	source := &fakeSource{events: make(chan struct{}, 1)}
	ai := &recoveryAI{calls: make(chan incident.InvestigationContext, 8), release: make(chan struct{})}
	r := Runner{Config: cfg, Source: source, Analyzer: ai,
		Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: &output.Writer{Out: io.Discard, Err: io.Discard}}
	startRecoveryWatch(t, &r)

	in := awaitRecoveryCall(t, ai)
	if !in.RecoveryEligible {
		t.Fatal("fixture did not reach recovery review")
	}

	source.set([]health.Object{failedPod()}, nil)
	close(ai.release)

	select {
	case <-repo.saved:
		t.Fatal("stale AI resolution was persisted after reappearance")
	case <-time.After(80 * time.Millisecond):
	}
}

// TestKnownReplacementDoesNotInvalidateOriginalAbsence allows only the already-observed replacement incarnation and version.
func TestKnownReplacementDoesNotInvalidateOriginalAbsence(t *testing.T) {
	old := recoveryRecord().Value.Identity.Anchor
	current := old
	current.UID = "current"
	ref := incident.Ref(old)
	in := incident.InvestigationContext{Identities: map[string]health.Resource{ref: old},
		Objects: map[string]health.Object{incident.Ref(current): {Resource: current, Version: "7"}}, ToolVersions: map[string]string{ref: ""}}

	snapshot := health.Snapshot{Objects: []health.Object{{Resource: current, Version: "7"}}}
	if !toolsCurrent(in, snapshot) {
		t.Fatal("reviewed replacement was mistaken for a new reappearance")
	}

	snapshot.Objects[0].Version = "8"
	if toolsCurrent(in, snapshot) {
		t.Fatal("changed replacement was accepted")
	}

	snapshot.Objects[0].Version, snapshot.Objects[0].UID = "7", "another"
	if toolsCurrent(in, snapshot) {
		t.Fatal("different replacement incarnation was accepted")
	}
}

// TestOriginalReappearanceInvalidatesNegativeRead rejects the original UID even when it existed in the earlier snapshot.
func TestOriginalReappearanceInvalidatesNegativeRead(t *testing.T) {
	resource := recoveryRecord().Value.Identity.Anchor
	ref := incident.Ref(resource)
	object := health.Object{Resource: resource, Version: "7"}

	in := incident.InvestigationContext{Identities: map[string]health.Resource{ref: resource},
		Objects: map[string]health.Object{ref: object}, ToolVersions: map[string]string{ref: ""}}
	if toolsCurrent(in, health.Snapshot{Objects: []health.Object{object}}) {
		t.Fatal("original incarnation reappeared after the negative tool read")
	}
}

// TestOlderIndexRefreshCannotUndoConfirmedResolution releases a pre-write list after the AI-approved CR change.
func TestOlderIndexRefreshCannotUndoConfirmedResolution(t *testing.T) {
	cfg := options()
	cfg.RecoveryWindow = 30 * time.Millisecond
	repo := &recoveryRepository{saved: make(chan incident.Record, 4), blockAfter: 2, block: make(chan struct{}), blocked: make(chan struct{}, 1)}
	repo.put(recoveryRecord())

	ai := &recoveryAI{calls: make(chan incident.InvestigationContext, 8)}
	r := Runner{Config: cfg, Source: &fakeSource{events: make(chan struct{}, 1)}, Analyzer: ai,
		Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: &output.Writer{Out: io.Discard, Err: io.Discard}}
	startRecoveryWatch(t, &r)

	select {
	case <-repo.blocked:
	case <-time.After(time.Second):
		t.Fatal("periodic pre-write index refresh did not start")
	}

	awaitRecoveryCall(t, ai)

	select {
	case <-repo.saved:
	case <-time.After(time.Second):
		t.Fatal("resolution did not proceed while a bounded read was pending")
	}

	close(repo.block)

	select {
	case <-ai.calls:
		t.Fatal("older index response resurrected a confirmed resolved candidate")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestIndexRefreshOverallDeadlineKeepsCollectionRunning checks timed read cancellation and one in-flight refresh.
func TestIndexRefreshOverallDeadlineKeepsCollectionRunning(t *testing.T) {
	cfg := options()
	cfg.RequestTimeout = 20 * time.Millisecond
	cfg.RecoveryWindow = time.Second
	repo := &recoveryRepository{blockAfter: 2, block: make(chan struct{}), blocked: make(chan struct{}, 1)}
	repo.put(recoveryRecord())

	ai := &recoveryAI{calls: make(chan incident.InvestigationContext, 8)}
	source := &fakeSource{events: make(chan struct{}, 1)}
	r := Runner{Config: cfg, Source: source, Analyzer: ai,
		Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: &output.Writer{Out: io.Discard, Err: io.Discard}}
	startRecoveryWatch(t, &r)

	select {
	case <-repo.blocked:
	case <-time.After(time.Second):
		t.Fatal("bounded index refresh did not start")
	}

	source.set([]health.Object{failedPod()}, nil)

	deadline := time.Now().Add(time.Second)
	for repo.failures.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	if repo.failures.Load() == 0 || repo.concurrent.Load() {
		t.Fatal("index deadline or one-in-flight bound was not enforced")
	}

	snapshot, err := source.Snapshot(time.Now())
	if err != nil || len(snapshot.Objects) != 1 {
		t.Fatal("blocked index refresh prevented resource collection")
	}
}

// TestOnceAndRestartDoNotInheritRecovery requires fresh watch tracking regardless of stored Incident age.
func TestOnceAndRestartDoNotInheritRecovery(t *testing.T) {
	cfg := options()
	cfg.RecoveryWindow = 5 * time.Minute
	repo := &recoveryRepository{saved: make(chan incident.Record, 4)}
	record := recoveryRecord()
	repo.put(record)

	ai := &recoveryAI{calls: make(chan incident.InvestigationContext, 8), noAction: true}

	r := Runner{Config: cfg, Source: &fakeSource{}, Analyzer: ai, Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"},
		Output: &output.Writer{Out: io.Discard, Err: io.Discard}}
	if code := r.Once(t.Context()); code != 0 {
		t.Fatalf("bounded no-action assessment failed: exit=%d", code)
	}

	in := <-ai.calls
	if in.RecoveryEligible || in.Recovery.Since != nil || in.Recovery.WindowSeconds != cfg.RecoveryWindow.Seconds() {
		t.Fatal("once fabricated continuous recovery")
	}

	now := time.Now()
	open := []incident.Record{record}

	contexts, err := incident.BuildInvestigationContexts(health.Snapshot{At: now}, cfg.Policy, open, cfg.MaxObjects)
	if err != nil {
		t.Fatal(err)
	}

	states := map[string]*candidate{}
	updateCandidates(states, contexts, open, now, cfg)

	c := states[incident.Ref(record.Value.Identity.Anchor)]
	if !c.recoverySince.Equal(now) || recoveryEligible(c, now, cfg.RecoveryWindow) {
		t.Fatal("restart inherited historic recovery time")
	}
}

// TestRecoveryNoActionReportsRetention logs the model's refusal and next review eligibility without closing an Incident.
func TestRecoveryNoActionReportsRetention(t *testing.T) {
	cfg := options()
	cfg.RecoveryWindow = 20 * time.Millisecond
	repo := &recoveryRepository{}
	repo.put(recoveryRecord())

	ai := &recoveryAI{calls: make(chan incident.InvestigationContext, 8), noAction: true}
	notice := &recoveryWriteNotice{confirmed: make(chan struct{}, 1)}
	r := Runner{Config: cfg, Source: &fakeSource{events: make(chan struct{}, 1)}, Analyzer: ai,
		Incidents: incident.Reconciler{Repository: repo, Observer: "fixture"}, Output: &output.Writer{Out: notice, Err: notice}}
	startRecoveryWatch(t, &r)

	in := awaitRecoveryCall(t, ai)
	if !strings.Contains(in.Reason, "obsolescence") {
		t.Fatal("absent-anchor review did not explain its purpose")
	}

	deadline := time.Now().Add(time.Second)
	for !strings.Contains(notice.activity(), "AI chose no-action; open incidents retained") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	activity := notice.activity()
	if strings.Count(activity, "AI chose no-action; open incidents retained") != 1 ||
		!strings.Contains(activity, "next review eligible at=") || strings.Contains(activity, "persisted resolve") {
		t.Fatalf("recovery disposition was missing or misleading: %s", activity)
	}
}
