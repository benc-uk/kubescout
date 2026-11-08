/*
Purpose: Coordinate bounded once assessments and continuous incident review.
Flow: Collections and stored incidents become investigation contexts; private
deterministic signals schedule serial AI work. Freshness and recovery checks
gate reconciliation. Provenance-labelled text explains once/watch activity;
CRDs alone store incidents. Transcript dispositions and safe failure details
describe outcomes without changing the existing per-candidate retry schedule.
Restart scope permits cluster PVs and attachments while retaining namespace boundaries for claims.
Snapshot reads share freshness guards, including absence and recreated names.
Periodic bounded Incident reads refresh candidates independently of AI work.
One continuous recovery window uses collection generations and semantic change
markers; index outages pause review and AI alone approves lifecycle changes.
Absent-anchor reviews assess obsolescence; AI no-action explicitly retains open Incidents and reports next eligibility.
Extra-resource scope and API-group identity participate in restart and stale-result checks.
Unavailable CEL assessments pause investigations and reset recovery without stopping watch or creating incidents.
One process-local pending application pauses new AI work on persistence failures;
bounded retries retain exact desired revisions while collection and index refresh continue.
*/
package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/output"
	"github.com/benc-uk/kubescout/internal/resourcepolicy"
)

type Analyzer interface {
	Investigate(context.Context, incident.InvestigationContext) (incident.Decision, error)
}

type Source interface {
	Run(context.Context)
	Changes() <-chan struct{}
	Err() error
	Snapshot(time.Time) (health.Snapshot, error)
	Collect(context.Context, time.Time) (health.Snapshot, error)
}

type Runner struct {
	Config     config.Config
	Source     Source
	Analyzer   Analyzer
	Incidents  incident.Reconciler
	Output     *output.Writer
	Transcript *output.Transcript
	sequence   uint64
}

type candidate struct {
	investigation                   incident.InvestigationContext
	lastRevision                    string
	lastReview, next, recoverySince time.Time
	recoveryToken                   string
	failures                        int
	open                            bool
	scope                           string
	recoveryAnnounced               bool
}

type completion struct {
	ctx           context.Context
	investigation incident.InvestigationContext
	decision      incident.Decision
	err           error
}

type indexResult struct {
	records  []incident.Record
	revision uint64
	err      error
}

type pendingWrite struct {
	result      completion
	application *incident.Application
	next        time.Time
	failures    int
}

// Once runs a bounded AI assessment with readable progress and a completion summary.
// Accepted decisions are reconciled only while their evidence remains current.
func (r *Runner) Once(ctx context.Context) (code int) {
	ctx, cancel := context.WithTimeout(ctx, r.Config.RunTimeout)
	defer cancel()

	reviews, writes := 0, 0

	defer func() {
		status, level := "completed", "info"
		if code != 0 {
			status, level = "incomplete", "error"
		}

		_ = r.Output.Log(level, "GO", "EXECUTION", fmt.Sprintf("Assessment %s: %d investigations, %d persisted incident changes", status, reviews, writes))
		if r.Output.Error() != nil {
			code = 1
		}
	}()

	_ = r.Output.Log("info", "GO", "COLLECT", "Loading incident index and collecting scoped Kubernetes evidence")

	open, err := r.loadOpen(ctx)
	if err != nil {
		return r.fail("INCIDENT", err)
	}

	snapshot, err := r.Source.Collect(ctx, time.Now())
	if err != nil {
		return r.fail("COLLECT", err)
	}

	_ = r.Output.Log("info", "GO", "COLLECT", fmt.Sprintf("Collection ready: %d scoped objects", len(snapshot.Objects)))

	investigations, err := incident.BuildInvestigationContexts(snapshot, r.Config.Policy, open, r.Config.MaxObjects)
	if err != nil {
		return r.fail("CHECK", err)
	}

	r.checkSummary(investigations)

	for _, key := range sortedKeys(investigations) {
		in := investigations[key]

		in.Reason = "manual assessment"
		if in.Recovery != nil {
			in.Recovery.WindowSeconds = r.Config.RecoveryWindow.Seconds()
		}

		in.Incidents, err = r.Incidents.Existing(ctx, in.Identities[in.Anchor])
		if err != nil {
			return r.fail("INCIDENT", err)
		}

		work := r.startInvestigation(ctx, in)
		reviews++

		decision, err := r.Analyzer.Investigate(work, in)
		if err != nil {
			r.disposition(work, "Investigation failed: "+err.Error())
			return r.fail("MODEL", err)
		}

		r.decision(work, decision)
		changes, err := r.Incidents.Apply(ctx, in, decision, func() bool {
			fresh, err := r.Source.Collect(ctx, time.Now())
			if err != nil {
				return false
			}

			current, err := incident.BuildInvestigationContexts(fresh, r.Config.Policy, open, r.Config.MaxObjects)

			return err == nil && fresh.Continuity == in.Continuity && current[in.Anchor].Revision == in.Revision && toolsCurrent(in, fresh)
		})
		writes += len(changes)
		r.persisted(work, changes)

		if err != nil {
			r.disposition(work, "Not fully applied: "+err.Error())
			return r.fail("INCIDENT", err)
		}
	}

	return 0
}

// Watch collects continuously while scheduling serial AI reviews and rejecting stale results.
// It publishes material incident changes, separates diagnostics, and waits for workers on exit.
func (r *Runner) Watch(ctx context.Context) (code int) {
	ctx, cancel := context.WithCancel(ctx)

	var (
		workers sync.WaitGroup
		pending *pendingWrite
	)

	defer func() {
		cancel()
		workers.Wait()

		if pending != nil {
			r.disposition(pending.result.ctx, "Process stopped with an unfinished application; no unconfirmed write is reported as persisted. The pending decision is process-local.")
			_ = r.Output.Log("warn", "GO", "INCIDENT", "Stopped with pending incident persistence; reviewed decisions are not retained across process restarts")
		}

		_ = r.Output.Log("info", "GO", "SHUTDOWN", "Watch stopped")
		if r.Output.Error() != nil {
			code = 1
		}
	}()

	open, err := r.loadOpen(ctx)
	if err != nil {
		return r.fail("INCIDENT", err)
	}

	workers.Go(func() { r.Source.Run(ctx) })

	results := make(chan completion, 1)
	indexResults := make(chan indexResult, 1)
	states := map[string]*candidate{}

	ticker := time.NewTicker(min(r.Config.CheckInterval, r.Config.BatchWindow))
	defer ticker.Stop()

	busy := false
	indexBusy := false

	var indexFresh atomic.Bool
	indexFresh.Store(true)

	indexRevision := uint64(0)
	nextIndex := time.Now().Add(r.Config.CheckInterval)
	indexError := ""
	lastError := ""
	lastCheckError := ""
	lastSummary := time.Time{}
	revisions := map[string]string{}

	wake := make(chan struct{}, 1)
	wake <- struct{}{}

	for {
		if r.Output.Error() != nil {
			return 1
		}

		select {
		case <-ctx.Done():
			return 0
		case <-r.Source.Changes():
		case <-ticker.C:
		case <-wake:
		case result := <-indexResults:
			indexBusy = false

			if result.err != nil {
				indexFresh.Store(false)

				resetRecovery(states)

				if indexError != result.err.Error() {
					if err := r.Output.Diagnostic("INCIDENT", "incident_index_unknown: "+result.err.Error()+"; investigations paused until a complete refresh"); err != nil {
						return 1
					}

					indexError = result.err.Error()
				}
			} else if result.revision == indexRevision {
				open = result.records

				indexFresh.Store(true)

				if indexError != "" {
					if err := r.Output.Log("info", "GO", "INCIDENT", "incident_index_recovered: complete scoped index refreshed; recovery tracking restarts"); err != nil {
						return 1
					}

					indexError = ""
				}
			} else {
				nextIndex = time.Time{}
			}
		case result := <-results:
			busy = false

			c := states[result.investigation.Anchor]
			if c == nil {
				r.disposition(result.ctx, "Superseded: candidate disappeared")

				if err := r.Output.Diagnostic("VALIDATE", "investigation_superseded: candidate disappeared during investigation"); err != nil {
					return 1
				}

				continue
			}

			now := time.Now()

			current := func() bool {
				return indexFresh.Load() && r.current(result.investigation, open) &&
					reviewRecoveryCurrent(result.investigation, c, time.Now(), r.Config.RecoveryWindow)
			}
			if result.err == nil && !current() {
				result.err = errors.New("AI investigation superseded by changed or incomplete evidence")
			}

			if result.err != nil {
				r.disposition(result.ctx, "Deferred: "+result.err.Error())

				if err := r.deferInvestigation(c, result.err, now); err != nil {
					return 1
				}

				continue
			}

			r.decision(result.ctx, result.decision)

			application, err := r.Incidents.NewApplication(result.investigation, result.decision)
			if err != nil {
				r.disposition(result.ctx, "Rejected application: "+err.Error())

				if e := r.deferInvestigation(c, err, now); e != nil {
					return 1
				}

				continue
			}

			pending = &pendingWrite{result: result, application: application}
		}

		now := time.Now()

		if !indexBusy && !now.Before(nextIndex) {
			indexBusy = true
			nextIndex = now.Add(r.Config.CheckInterval)
			revision := indexRevision

			workers.Go(func() {
				records, err := r.loadOpen(ctx)
				if err != nil {
					indexFresh.Store(false)
				}

				select {
				case indexResults <- indexResult{records: records, revision: revision, err: err}:
				case <-ctx.Done():
				}
			})
		}

		snapshot, err := r.Source.Snapshot(now)
		if err != nil {
			resetRecovery(states)

			if lastError != err.Error() {
				if e := r.Output.Diagnostic("COLLECT", "collection_unknown: "+err.Error()); e != nil {
					return 1
				}

				lastError = err.Error()
			}

			continue
		}

		if lastError != "" {
			if err := r.Output.Log("info", "GO", "COLLECT", "collection_recovered: fresh complete collection resumed"); err != nil {
				return 1
			}

			lastError = ""
		}

		if !indexFresh.Load() {
			continue
		}

		investigations, err := incident.BuildInvestigationContexts(snapshot, r.Config.Policy, open, r.Config.MaxObjects)
		if err != nil {
			var evaluation *resourcepolicy.EvaluationError
			if errors.As(err, &evaluation) {
				resetRecovery(states)

				if lastCheckError != err.Error() {
					if e := r.Output.Diagnostic("CHECK", err.Error()+"; investigations paused until complete assessment"); e != nil {
						return 1
					}

					lastCheckError = err.Error()
				}

				continue
			}

			return r.fail("CHECK", err)
		}

		if lastCheckError != "" {
			if err := r.Output.Log("info", "GO", "CHECK", "checks_recovered: complete extra-resource assessment resumed"); err != nil {
				return 1
			}

			lastCheckError = ""
		}

		if err := r.refreshCandidates(states, investigations, open, now); err != nil {
			return 1
		}

		changed := len(revisions) != len(investigations)
		for key, in := range investigations {
			changed = changed || revisions[key] != in.Revision
		}

		if changed || lastSummary.IsZero() {
			r.checkSummary(investigations)

			revisions = map[string]string{}
			for key, in := range investigations {
				revisions[key] = in.Revision
			}
		}

		if lastSummary.IsZero() || now.Sub(lastSummary) >= time.Minute {
			_ = r.Output.Log("info", "GO", "COLLECT", fmt.Sprintf("Watching: %d scoped objects, %d candidates, AI busy=%t pending_application=%t", len(snapshot.Objects), len(states), busy, pending != nil))
			lastSummary = now
		}

		if pending != nil {
			result := pending.result
			c := states[result.investigation.Anchor]

			unchanged := c != nil && c.investigation.Revision == result.investigation.Revision &&
				c.investigation.Continuity == result.investigation.Continuity && toolsCurrent(result.investigation, snapshot) &&
				reviewRecoveryCurrent(result.investigation, c, now, r.Config.RecoveryWindow)
			if unchanged && now.Before(pending.next) {
				continue
			}

			changes, err := r.applyPending(ctx, pending, c, open, indexFresh.Load)
			r.persisted(result.ctx, changes)

			if len(changes) > 0 {
				open = applyOpenChanges(open, changes)
				indexRevision++
				nextIndex = time.Time{}
			}

			if err != nil {
				var persistence *incident.PersistenceError
				if errors.As(err, &persistence) {
					if e := r.deferPersistence(pending, err, time.Now()); e != nil {
						return 1
					}

					continue
				}

				r.disposition(result.ctx, "Application discarded: "+err.Error())

				if c != nil {
					if e := r.deferInvestigation(c, err, time.Now()); e != nil {
						return 1
					}
				} else if e := r.Output.Diagnostic("VALIDATE", "pending application superseded: "+err.Error()); e != nil {
					return 1
				}

				pending = nil

				continue
			}

			c.lastReview, c.lastRevision, c.failures = time.Now(), result.investigation.Revision, 0
			c.next = c.lastReview.Add(r.Config.ReviewInterval)

			if result.investigation.RecoveryEligible && result.decision.Outcome == "no_action" {
				if err := r.Output.Log("info", "GO", "RECOVERY", fmt.Sprintf("%s AI chose no-action; open incidents retained; next review eligible at=%s (not guaranteed)",
					output.Scope(result.ctx), c.next.UTC().Format(time.RFC3339))); err != nil {
					return 1
				}
			}

			if pending.failures > 0 {
				if err := r.Output.Log("info", "GO", "INCIDENT", output.Scope(result.ctx)+" incident_persistence_recovered: pending application completed without another AI call"); err != nil {
					return 1
				}
			}

			pending = nil

			continue
		}

		if busy {
			continue
		}

		keys := sortedKeys(investigations)
		sort.SliceStable(keys, func(i, j int) bool { return states[keys[i]].next.Before(states[keys[j]].next) })

		for _, key := range keys {
			c := states[key]
			if now.Before(c.next) {
				continue
			}

			in := c.investigation

			in.RecoveryEligible = c.open && recoveryEligible(c, now, r.Config.RecoveryWindow)
			if c.open && !incident.Active(in) && !in.RecoveryEligible {
				continue
			}

			in.Reason = "new or changed evidence"
			if in.RecoveryEligible {
				in.Reason = "stable recovery review"
				if in.Recovery != nil && in.Recovery.AnchorState == "absent" {
					in.Reason = "stable recovery review: assess original incident obsolescence after continuous anchor absence"
				}
			} else if in.Revision == c.lastRevision {
				in.Reason = "scheduled incident review"
			}

			if in.Recovery != nil {
				recovery := *in.Recovery
				recovery.WindowSeconds = r.Config.RecoveryWindow.Seconds()

				if !c.recoverySince.IsZero() {
					since := c.recoverySince
					recovery.Since = &since
				}

				in.Recovery = &recovery
			}

			in.Incidents, err = r.Incidents.Existing(ctx, in.Identities[in.Anchor])
			if err != nil {
				r.disposition(ctx, "Incident lookup deferred before AI: "+err.Error())

				if e := r.deferInvestigation(c, err, now); e != nil {
					return 1
				}

				break
			}

			busy = true
			work := r.startInvestigation(ctx, in)
			c.scope = output.Scope(work)

			workers.Go(func() {
				decision, err := r.Analyzer.Investigate(work, in)
				select {
				case results <- completion{work, in, decision, err}:
				case <-ctx.Done():
				}
			})

			break
		}
	}
}

// deferInvestigation preserves failure backoff and logs safe details and next retry eligibility.
// Retry hints describe local/provider limits; they do not override the scheduler.
func (r *Runner) deferInvestigation(c *candidate, failure error, now time.Time) error {
	c.failures++

	c.next = now.Add(retryDelay(c.failures, r.Config.ReviewInterval))

	return r.Output.Diagnostic("SCHEDULE", fmt.Sprintf("%s investigation_deferred: %s; consecutive_failures=%d retry_after_seconds=%.0f next_retry_at=%s (eligibility, not a guaranteed request)",
		c.scope, failure.Error(), c.failures, c.next.Sub(now).Seconds(), c.next.UTC().Format(time.RFC3339)))
}

// applyPending retries an exact proposal only while observation, index and continuous recovery remain current.
func (r *Runner) applyPending(ctx context.Context, pending *pendingWrite, c *candidate, open []incident.Record, indexFresh func() bool) ([]incident.Change, error) {
	if c == nil {
		return []incident.Change{}, errors.New("candidate disappeared before pending persistence")
	}

	result := pending.result

	current := func() bool {
		return indexFresh() && r.current(result.investigation, open) &&
			reviewRecoveryCurrent(result.investigation, c, time.Now(), r.Config.RecoveryWindow)
	}
	if !current() {
		return []incident.Change{}, errors.New("pending AI conclusion superseded by changed or incomplete evidence")
	}

	ctx, cancel := context.WithTimeout(ctx, r.Config.RequestTimeout)
	defer cancel()

	return pending.application.Apply(ctx, current)
}

// deferPersistence reports bounded storage backoff while retaining one proposal and pausing new AI work.
func (r *Runner) deferPersistence(pending *pendingWrite, failure error, now time.Time) error {
	pending.failures++
	pending.next = now.Add(retryDelay(pending.failures, r.Config.ReviewInterval))
	r.disposition(pending.result.ctx, "Persistence deferred; exact desired revisions retained: "+failure.Error())

	return r.Output.Log("error", "GO", "INCIDENT", fmt.Sprintf("%s incident_persistence_deferred: %s; consecutive_failures=%d retry_after_seconds=%.0f next_retry_at=%s; collection continues, new AI calls paused",
		output.Scope(pending.result.ctx), failure.Error(), pending.failures, pending.next.Sub(now).Seconds(), pending.next.UTC().Format(time.RFC3339)))
}

// retryDelay shares the existing two-second, four-second, then review-interval backoff across failures.
func retryDelay(failures int, reviewInterval time.Duration) time.Duration {
	if failures >= 3 {
		return reviewInterval
	}

	return min(time.Duration(1<<min(failures, 6))*time.Second, reviewInterval)
}

// reviewRecoveryCurrent prevents a reviewed resolution from outliving its exact continuous observation window.
func reviewRecoveryCurrent(in incident.InvestigationContext, c *candidate, now time.Time, window time.Duration) bool {
	return !in.RecoveryEligible || c != nil && recoveryEligible(c, now, window) &&
		in.Recovery != nil && in.Recovery.Since != nil && c.recoverySince.Equal(*in.Recovery.Since)
}

// fail reports a Go execution failure without manufacturing an incident.
func (r *Runner) fail(phase string, err error) int {
	_ = r.Output.Log("error", "GO", phase, err.Error())
	return 1
}

// startInvestigation gives each review local correlation and explains the deterministic trigger.
func (r *Runner) startInvestigation(ctx context.Context, in incident.InvestigationContext) context.Context {
	r.sequence++
	anchor := in.Identities[in.Anchor]

	resource := anchor.Kind + " " + anchor.Name
	if anchor.Namespace != "" {
		resource = anchor.Kind + " " + anchor.Namespace + "/" + anchor.Name
	}

	ctx = output.WithInvestigation(ctx, fmt.Sprint(r.sequence), resource)
	_ = r.Output.Log("info", "GO", "SCHEDULE", output.Scope(ctx)+": "+in.Reason)

	return ctx
}

// checkSummary describes private deterministic signals without implying an incident or AI assessment.
func (r *Runner) checkSummary(investigations map[string]incident.InvestigationContext) {
	for _, check := range health.Checks {
		_ = r.Output.Log("debug", "GO", "CHECK", "Evaluated "+check.Name+" deterministic checks")
	}

	signals := 0
	for _, in := range investigations {
		signals += len(in.Signals)
	}

	_ = r.Output.Log("info", "GO", "CHECK", fmt.Sprintf("%d deterministic signals in %d candidates; not AI conclusions", signals, len(investigations)))
	if len(investigations) == 0 {
		_ = r.Output.Log("info", "GO", "SCHEDULE", "No investigation candidates; no AI request needed (not a model no-action decision)")
	}

	for _, key := range sortedKeys(investigations) {
		in := investigations[key]
		for _, signal := range in.Signals {
			resource := in.Identities[signal.Resource]
			_ = r.Output.Log("debug", "GO", "CHECK", fmt.Sprintf("%s %s/%s rule=%s hint=%s: %s",
				resource.Kind, resource.Namespace, resource.Name, signal.Rule, signal.Hint, signal.Summary))
		}
	}
}

// decision identifies model-authored conclusions separately from deterministic validation and writes.
func (r *Runner) decision(ctx context.Context, decision incident.Decision) {
	_ = r.Output.Log("info", "AI", "DECISION", output.Scope(ctx)+" "+decision.Outcome+": "+decision.Reason)
	for _, action := range decision.Actions {
		_ = r.Output.Log("info", "AI", "DECISION", fmt.Sprintf("%s proposed %s [%s]: %s", output.Scope(ctx), action.Operation, action.Severity, action.Title))
	}

	_ = r.Output.Log("info", "GO", "VALIDATE", output.Scope(ctx)+" decision contract accepted; freshness and persistence checks still apply")
}

// persisted reports only changes returned by reconciliation, never speculative model actions.
func (r *Runner) persisted(ctx context.Context, changes []incident.Change) {
	for _, change := range changes {
		message := fmt.Sprintf("%s persisted %s %s [%s]: %s", output.Scope(ctx), change.Operation, change.Name,
			change.Incident.Analysis.Severity, change.Incident.Analysis.Title)
		_ = r.Output.Log("info", "GO", "INCIDENT", message)
		r.disposition(ctx, message)
	}

	if len(changes) == 0 {
		r.disposition(ctx, "No incident mutations returned by reconciliation")
	}
}

// disposition records the host's actual application outcome separately from the model response.
func (r *Runner) disposition(ctx context.Context, message string) {
	r.Transcript.Record(output.Scope(ctx), "GO", "DISPOSITION", message)
}

// current rechecks evidence revisions, inspected resources, and recovery against a fresh snapshot.
func (r *Runner) current(in incident.InvestigationContext, open []incident.Record) bool {
	snapshot, err := r.Source.Snapshot(time.Now())
	if err != nil {
		return false
	}

	investigations, err := incident.BuildInvestigationContexts(snapshot, r.Config.Policy, open, r.Config.MaxObjects)
	if err != nil {
		return false
	}

	current, ok := investigations[in.Anchor]

	return ok && current.Continuity == in.Continuity && current.Revision == in.Revision && toolsCurrent(in, snapshot) &&
		(!in.RecoveryEligible || !incident.Active(current) && recoveryToken(current) == recoveryToken(in))
}

// toolsCurrent checks inspected versions and original-incarnation absence.
// Unreviewed recreation invalidates negative reads; supplied replacements must still match.
func toolsCurrent(in incident.InvestigationContext, snapshot health.Snapshot) bool {
	if len(in.ToolVersions) == 0 {
		return true
	}

	versions := map[string]string{}
	for _, o := range snapshot.Objects {
		versions[incident.Ref(o.Resource)] = o.Version
	}

	for ref, version := range in.ToolVersions {
		if version == "" {
			identity := in.Identities[ref]
			for _, object := range snapshot.Objects {
				if object.Kind == identity.Kind && object.APIGroup == identity.APIGroup &&
					object.Namespace == identity.Namespace && object.Name == identity.Name {
					if object.UID == identity.UID {
						return false
					}

					known, ok := in.Objects[incident.Ref(object.Resource)]

					expected := known.Version
					if inspected, read := in.ToolVersions[incident.Ref(object.Resource)]; read {
						expected = inspected
					}

					if !ok || known.Resource != object.Resource || expected != object.Version {
						return false
					}
				}
			}

			continue
		}

		if versions[ref] != version {
			return false
		}
	}

	return true
}

// updateCandidates refreshes review schedules and continuous-recovery tracking from current evidence.
// Continuity and semantic markers reset recovery; batching, cooldowns and no-action scheduling remain bounded.
func updateCandidates(states map[string]*candidate, investigations map[string]incident.InvestigationContext, open []incident.Record, now time.Time, cfg config.Config) {
	active := map[string]bool{}
	for _, i := range open {
		active[incident.Ref(i.Value.Identity.Anchor)] = true
	}

	for key, in := range investigations {
		c, ok := states[key]
		if !ok {
			c = &candidate{next: now.Add(cfg.BatchWindow)}
			states[key] = c
		}

		changed := c.investigation.Revision != "" && c.investigation.Revision != in.Revision
		urgent := urgency(in) > urgency(c.investigation)
		discontinuous := c.investigation.Continuity != in.Continuity || !c.open && active[key]
		c.investigation, c.open = in, active[key]

		since := c.recoverySince

		token := recoveryToken(in)
		if incident.Active(in) {
			c.recoverySince = time.Time{}
		} else if c.recoverySince.IsZero() || token != c.recoveryToken || discontinuous {
			c.recoverySince = now
		}

		if !since.Equal(c.recoverySince) {
			c.recoveryAnnounced = false
		}

		c.recoveryToken = token
		if changed {
			c.failures = 0

			c.next = maxTime(now.Add(cfg.BatchWindow), c.lastReview.Add(cfg.Cooldown))
			if urgent {
				c.next = now.Add(cfg.BatchWindow)
			}
		}

		if c.open && c.failures == 0 && !c.recoverySince.IsZero() &&
			c.lastReview.Before(c.recoverySince.Add(cfg.RecoveryWindow)) && c.next.After(c.recoverySince.Add(cfg.RecoveryWindow)) {
			c.next = c.recoverySince.Add(cfg.RecoveryWindow)
		}
	}

	for key := range states {
		if _, ok := investigations[key]; !ok {
			delete(states, key)
		}
	}
}

// resetRecovery discards continuous recovery after unknown observation or an unavailable Incident index.
func resetRecovery(states map[string]*candidate) {
	for _, c := range states {
		c.recoverySince = time.Time{}
		c.recoveryToken = ""
		c.recoveryAnnounced = false
	}
}

// recoveryEligible includes the exact window boundary and requires uninterrupted tracking.
func recoveryEligible(c *candidate, now time.Time, window time.Duration) bool {
	return !c.recoverySince.IsZero() && !now.Before(c.recoverySince.Add(window))
}

// refreshCandidates logs recovery transitions once while preserving pure candidate scheduling.
func (r *Runner) refreshCandidates(states map[string]*candidate, investigations map[string]incident.InvestigationContext, open []incident.Record, now time.Time) error {
	previous := map[string]time.Time{}
	for key, c := range states {
		previous[key] = c.recoverySince
	}

	updateCandidates(states, investigations, open, now, r.Config)

	for _, key := range sortedKeys(states) {
		c := states[key]
		if !c.open {
			continue
		}

		a := c.investigation.Identities[key]

		resource := a.Kind + " " + a.Name
		if a.Namespace != "" {
			resource = a.Kind + " " + a.Namespace + "/" + a.Name
		}

		if !previous[key].Equal(c.recoverySince) {
			message := resource + ": recovery tracking reset; active problem signals returned"

			if !c.recoverySince.IsZero() {
				state := "cleared signals"
				if c.investigation.Recovery != nil {
					state = "anchor " + c.investigation.Recovery.AnchorState + ", no active problem signals"
				}

				message = fmt.Sprintf("%s: recovery tracking started/reset (%s); continuous window=%s", resource, state, r.Config.RecoveryWindow)
			}

			if err := r.Output.Log("info", "GO", "RECOVERY", message); err != nil {
				return err
			}
		}

		if recoveryEligible(c, now, r.Config.RecoveryWindow) && !c.recoveryAnnounced {
			if err := r.Output.Log("info", "GO", "RECOVERY", resource+": recovery review eligible; an AI resolution decision is still required"); err != nil {
				return err
			}

			c.recoveryAnnounced = true
		}
	}

	return nil
}

// applyOpenChanges updates the private index from confirmed writes without accepting an older concurrent list.
// Record metadata is refreshed by later complete reads and is never used here as a write precondition.
func applyOpenChanges(open []incident.Record, changes []incident.Change) []incident.Record {
	records := map[string]incident.Record{}
	for _, record := range open {
		records[record.Name] = record
	}

	for _, change := range changes {
		if change.Incident.Lifecycle.State == "resolved" {
			delete(records, change.Name)
			continue
		}

		record := records[change.Name]
		record.Name, record.Value = change.Name, change.Incident
		records[change.Name] = record
	}

	out := make([]incident.Record, 0, len(records))
	for _, name := range sortedKeys(records) {
		out = append(out, records[name])
	}

	return out
}

// urgency ranks the strongest deterministic hint for scheduling, not incident severity.
func urgency(in incident.InvestigationContext) int {
	value := 0

	for _, s := range in.Signals {
		if s.Hint == "critical" {
			value = max(value, 2)
		} else if s.Hint == "warning" {
			value = max(value, 1)
		}
	}

	return value
}

// loadOpen loads unresolved incidents and rejects stored resources outside the configured scope.
func (r *Runner) loadOpen(ctx context.Context) ([]incident.Record, error) {
	ctx, cancel := context.WithTimeout(ctx, r.Config.RequestTimeout)
	defer cancel()

	open, err := r.Incidents.Repository.ListOpen(ctx)
	if err != nil {
		return nil, err
	}

	for _, record := range open {
		resources := append([]health.Resource{record.Value.Identity.Anchor}, record.Value.Resources...)
		for _, resource := range resources {
			if !r.Config.ResourceInScope(resource) {
				return nil, errors.New("stored incident belongs to a different observation scope; use a unique observer-id")
			}
		}
	}

	return open, nil
}

// recoveryToken detects semantic transitions, replacement, and batched changes in relevant resources.
// Generic non-anchor Node context and informational heartbeat churn do not reset recovery.
func recoveryToken(in incident.InvestigationContext) string {
	type observed struct {
		Ref, State string
		Change     uint64
	}

	values := []observed{}

	for _, resource := range in.Resources {
		o, ok := in.Objects[resource.Ref]
		if resource.Kind == "Node" && resource.Ref != in.Anchor &&
			(in.Recovery == nil || resource.Ref != in.Recovery.ReplacementRef) {
			continue
		}

		v := observed{Ref: resource.Ref, Change: in.RecoveryChanges[resource.Ref]}
		if ok {
			v.State = health.RecoveryFingerprint(o)
		}

		values = append(values, v)
	}

	data, _ := json.Marshal(values)

	return string(data)
}

// maxTime selects the later deadline when combining scheduling constraints.
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}

	return b
}

// sortedKeys returns map keys in lexical order for deterministic investigation traversal.
func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}
