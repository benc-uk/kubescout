/*
Purpose: Coordinate bounded once assessments and continuous incident review.
Flow: Collections and stored incidents become investigation contexts; private
deterministic signals schedule serial AI work. Freshness and recovery checks
gate reconciliation. Provenance-labelled text explains once/watch activity;
CRDs alone store incidents. Transcript dispositions and safe failure details
describe outcomes without changing the existing per-candidate retry schedule.
*/
package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/output"
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
}

type completion struct {
	ctx           context.Context
	investigation incident.InvestigationContext
	decision      incident.Decision
	err           error
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

			return err == nil && current[in.Anchor].Revision == in.Revision && toolsCurrent(in, fresh)
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

	var workers sync.WaitGroup

	defer func() {
		cancel()
		workers.Wait()

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
	states := map[string]*candidate{}

	ticker := time.NewTicker(min(r.Config.CheckInterval, r.Config.BatchWindow))
	defer ticker.Stop()

	busy := false
	lastError := ""
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
				return r.current(result.investigation, open) && (!result.investigation.RecoveryEligible ||
					(!c.recoverySince.IsZero() && !time.Now().Before(c.recoverySince.Add(r.Config.RecoveryWindow))))
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
			changes, err := r.Incidents.Apply(ctx, result.investigation, result.decision, current)
			r.persisted(result.ctx, changes)

			if err != nil {
				r.disposition(result.ctx, "Persistence failed: "+err.Error())

				if e := r.Output.Log("error", "GO", "INCIDENT", "incident_persistence_failed: "+err.Error()); e != nil {
					return 1
				}
				// The reconciler has already retried the same decision, without
				// another model call. Do not loop into duplicate AI investigations.
				return 1
			}

			c.lastReview, c.lastRevision, c.failures = now, result.investigation.Revision, 0

			c.next = now.Add(r.Config.ReviewInterval)

			open, err = r.loadOpen(ctx)
			if err != nil {
				return r.fail("INCIDENT", err)
			}
		}

		now := time.Now()

		snapshot, err := r.Source.Snapshot(now)
		if err != nil {
			for _, c := range states {
				c.recoverySince = time.Time{}
				c.recoveryToken = ""
			}

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

		investigations, err := incident.BuildInvestigationContexts(snapshot, r.Config.Policy, open, r.Config.MaxObjects)
		if err != nil {
			return r.fail("CHECK", err)
		}

		updateCandidates(states, investigations, open, now, r.Config)

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
			_ = r.Output.Log("info", "GO", "COLLECT", fmt.Sprintf("Watching: %d scoped objects, %d candidates, AI busy=%t", len(snapshot.Objects), len(states), busy))
			lastSummary = now
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

			in.RecoveryEligible = !c.recoverySince.IsZero() && !now.Before(c.recoverySince.Add(r.Config.RecoveryWindow))
			if c.open && !incident.Active(in) && !in.RecoveryEligible {
				continue
			}

			in.Reason = "new or changed evidence"
			if in.RecoveryEligible {
				in.Reason = "stable recovery review"
			} else if in.Revision == c.lastRevision {
				in.Reason = "scheduled incident review"
			}

			in.Incidents, err = r.Incidents.Existing(ctx, in.Identities[in.Anchor])
			if err != nil {
				return r.fail("INCIDENT", err)
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

	c.next = now.Add(min(time.Duration(1<<min(c.failures, 6))*time.Second, r.Config.ReviewInterval))
	if c.failures >= 3 {
		c.next = now.Add(r.Config.ReviewInterval)
	}

	return r.Output.Diagnostic("SCHEDULE", fmt.Sprintf("%s investigation_deferred: %s; consecutive_failures=%d retry_after_seconds=%.0f next_retry_at=%s (eligibility, not a guaranteed request)",
		c.scope, failure.Error(), c.failures, c.next.Sub(now).Seconds(), c.next.UTC().Format(time.RFC3339)))
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

	return ok && current.Revision == in.Revision && toolsCurrent(in, snapshot) && (!in.RecoveryEligible || !incident.Active(current))
}

// toolsCurrent requires every tool-inspected resource to retain its observed version.
func toolsCurrent(in incident.InvestigationContext, snapshot health.Snapshot) bool {
	if len(in.ToolVersions) == 0 {
		return true
	}

	versions := map[string]string{}
	for _, o := range snapshot.Objects {
		versions[incident.Ref(o.Resource)] = o.Version
	}

	for ref, version := range in.ToolVersions {
		if versions[ref] != version {
			return false
		}
	}

	return true
}

// updateCandidates refreshes review schedules and continuous-recovery tracking from current evidence.
// It applies batching, cooldowns, and urgency changes, and removes disappeared candidates.
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
		c.investigation, c.open = in, active[key]

		token := recoveryToken(in)
		if incident.Active(in) {
			c.recoverySince = time.Time{}
		} else if c.recoverySince.IsZero() || token != c.recoveryToken {
			c.recoverySince = now
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
	open, err := r.Incidents.Repository.ListOpen(ctx)
	if err != nil {
		return nil, err
	}

	for _, record := range open {
		resources := append([]health.Resource{record.Value.Identity.Anchor}, record.Value.Resources...)
		for _, resource := range resources {
			if resource.Kind == "Node" && resource.Namespace == "" {
				continue
			}

			if !r.Config.WatchesNamespace(resource.Namespace) {
				return nil, errors.New("stored incident belongs to a different observation scope; use a unique observer-id")
			}
		}
	}

	return open, nil
}

// recoveryToken encodes observed identities, Pod phases, and restart counts to detect recovery resets.
func recoveryToken(in incident.InvestigationContext) string {
	type observed struct {
		Ref, Phase string
		Restarts   []int32
	}

	values := []observed{}

	for _, resource := range in.Resources {
		o, ok := in.Objects[resource.Ref]
		if !ok {
			continue
		}

		v := observed{Ref: resource.Ref, Phase: o.Phase}
		for _, c := range o.Containers {
			v.Restarts = append(v.Restarts, c.Restarts)
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
