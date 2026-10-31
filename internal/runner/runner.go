package runner

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"k8s-ops-agent/internal/config"
	"k8s-ops-agent/internal/health"
	"k8s-ops-agent/internal/incident"
	"k8s-ops-agent/internal/report"
)

type Analyzer interface {
	Investigate(context.Context, incident.Input) (incident.Decision, error)
}
type Source interface {
	Run(context.Context)
	Changes() <-chan struct{}
	Err() error
	Snapshot(time.Time) (health.Snapshot, error)
	Collect(context.Context, time.Time) (health.Snapshot, error)
}

type Runner struct {
	Config    config.Config
	Source    Source
	Analyzer  Analyzer
	Incidents incident.Reconciler
	Output    report.Writer
}

type candidate struct {
	input                           incident.Input
	lastRevision                    string
	lastReview, next, recoverySince time.Time
	recoveryToken                   string
	failures                        int
	open                            bool
}

func (r *Runner) Once(ctx context.Context) int {
	ctx, cancel := context.WithTimeout(ctx, r.Config.RunTimeout)
	defer cancel()
	out := report.Result{SchemaVersion: "3.0", Type: "execution", Status: "completed", Changes: []incident.Change{}, Errors: []string{}}
	fail := func(err error) {
		out.Status = "incomplete"
		out.Errors = append(out.Errors, err.Error())
	}
	open, err := r.loadOpen(ctx)
	if err != nil {
		fail(err)
	} else {
		snapshot, err := r.Source.Collect(ctx, time.Now())
		if err != nil {
			fail(err)
		} else {
			inputs, err := incident.Inputs(snapshot, r.Config.Policy, open, r.Config.MaxObjects)
			if err != nil {
				fail(err)
			} else {
				for _, key := range sortedKeys(inputs) {
					in := inputs[key]
					in.Reason = "manual assessment"
					// A single snapshot cannot establish continuous stable recovery.
					in.Incidents, err = r.Incidents.Existing(ctx, in.Identities[in.Anchor])
					if err != nil {
						fail(err)
						break
					}
					decision, err := r.Analyzer.Investigate(ctx, in)
					if err != nil {
						fail(err)
						break
					}
					changes, err := r.Incidents.Apply(ctx, in, decision, func() bool {
						fresh, err := r.Source.Collect(ctx, time.Now())
						if err != nil {
							return false
						}
						current, err := incident.Inputs(fresh, r.Config.Policy, open, r.Config.MaxObjects)
						return err == nil && current[in.Anchor].Revision == in.Revision && toolsCurrent(in, fresh)
					})
					out.Changes = append(out.Changes, changes...)
					if err != nil {
						fail(err)
						break
					}
				}
			}
		}
	}
	if err := r.Output.WriteResult(out); err != nil {
		return 1
	}
	if out.Status != "completed" {
		return 1
	}
	return 0
}

type completion struct {
	input    incident.Input
	decision incident.Decision
	err      error
}

func (r *Runner) Watch(ctx context.Context) (code int) {
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() {
		if ctx.Err() != nil {
			code = 0
		}
		cancel()
		workers.Wait()
	}()
	open, err := r.loadOpen(ctx)
	if err != nil {
		_ = r.Output.Diagnostic("incident_index_failed", err.Error())
		return 1
	}
	workers.Go(func() { r.Source.Run(ctx) })
	results := make(chan completion, 1)
	states := map[string]*candidate{}
	ticker := time.NewTicker(min(r.Config.CheckInterval, r.Config.BatchWindow))
	defer ticker.Stop()
	busy := false
	lastError := ""
	wake := make(chan struct{}, 1)
	wake <- struct{}{}
	for {
		select {
		case <-ctx.Done():
			return 0
		case <-r.Source.Changes():
		case <-ticker.C:
		case <-wake:
		case result := <-results:
			busy = false
			c := states[result.input.Anchor]
			if c == nil {
				if err := r.Output.Diagnostic("investigation_superseded", "candidate disappeared during investigation"); err != nil {
					return 1
				}
				continue
			}
			now := time.Now()
			current := func() bool {
				return r.current(result.input, open) && (!result.input.RecoveryEligible ||
					(!c.recoverySince.IsZero() && !time.Now().Before(c.recoverySince.Add(r.Config.RecoveryWindow))))
			}
			if result.err == nil && !current() {
				result.err = errors.New("AI investigation superseded by changed or incomplete evidence")
			}
			if result.err != nil {
				c.failures++
				c.next = now.Add(min(time.Duration(1<<min(c.failures, 6))*time.Second, r.Config.ReviewInterval))
				if c.failures >= 3 {
					c.next = now.Add(r.Config.ReviewInterval)
				}
				if err := r.Output.Diagnostic("investigation_deferred", result.err.Error()); err != nil {
					return 1
				}
				continue
			}
			changes, err := r.Incidents.Apply(ctx, result.input, result.decision, current)
			for _, change := range changes {
				if e := r.Output.WriteChange(change); e != nil {
					return 1
				}
			}
			if err != nil {
				if e := r.Output.Diagnostic("incident_persistence_failed", err.Error()); e != nil {
					return 1
				}
				// The reconciler has already retried the same decision, without
				// another model call. Do not loop into duplicate AI investigations.
				return 1
			}
			c.lastReview, c.lastRevision, c.failures = now, result.input.Revision, 0
			c.next = now.Add(r.Config.ReviewInterval)
			if result.decision.Outcome == "no_action" {
				if err := r.Output.Diagnostic("no_action", result.decision.Reason); err != nil {
					return 1
				}
			}
			open, err = r.loadOpen(ctx)
			if err != nil {
				_ = r.Output.Diagnostic("incident_index_failed", err.Error())
				return 1
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
				if e := r.Output.Diagnostic("collection_unknown", err.Error()); e != nil {
					return 1
				}
				lastError = err.Error()
			}
			continue
		}
		if lastError != "" {
			if err := r.Output.Diagnostic("collection_recovered", "fresh complete collection resumed"); err != nil {
				return 1
			}
			lastError = ""
		}
		inputs, err := incident.Inputs(snapshot, r.Config.Policy, open, r.Config.MaxObjects)
		if err != nil {
			_ = r.Output.Diagnostic("candidate_limit", err.Error())
			return 1
		}
		updateCandidates(states, inputs, open, now, r.Config)
		if busy {
			continue
		}
		keys := sortedKeys(inputs)
		sort.SliceStable(keys, func(i, j int) bool { return states[keys[i]].next.Before(states[keys[j]].next) })
		for _, key := range keys {
			c := states[key]
			if now.Before(c.next) {
				continue
			}
			in := c.input
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
				_ = r.Output.Diagnostic("incident_lookup_failed", err.Error())
				return 1
			}
			busy = true
			workers.Go(func() {
				decision, err := r.Analyzer.Investigate(ctx, in)
				select {
				case results <- completion{in, decision, err}:
				case <-ctx.Done():
				}
			})
			break
		}
	}
}

func (r *Runner) current(in incident.Input, open []incident.Record) bool {
	snapshot, err := r.Source.Snapshot(time.Now())
	if err != nil {
		return false
	}
	inputs, err := incident.Inputs(snapshot, r.Config.Policy, open, r.Config.MaxObjects)
	if err != nil {
		return false
	}
	current, ok := inputs[in.Anchor]
	return ok && current.Revision == in.Revision && toolsCurrent(in, snapshot) && (!in.RecoveryEligible || !incident.Active(current))
}

func toolsCurrent(in incident.Input, snapshot health.Snapshot) bool {
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

func updateCandidates(states map[string]*candidate, inputs map[string]incident.Input, open []incident.Record, now time.Time, cfg config.Config) {
	active := map[string]bool{}
	for _, i := range open {
		active[incident.Ref(i.Value.Identity.Anchor)] = true
	}
	for key, in := range inputs {
		c, ok := states[key]
		if !ok {
			c = &candidate{next: now.Add(cfg.BatchWindow)}
			states[key] = c
		}
		changed := c.input.Revision != "" && c.input.Revision != in.Revision
		urgent := urgency(in) > urgency(c.input)
		c.input, c.open = in, active[key]
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
		if _, ok := inputs[key]; !ok {
			delete(states, key)
		}
	}
}

func urgency(in incident.Input) int {
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

func recoveryToken(in incident.Input) string {
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
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
