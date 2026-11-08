/*
Purpose: Resume one AI-approved decision without regenerating desired Incident revisions.
Flow: Private process-local plans retain stable names, expected UID/content and exact
AI conclusions across bounded storage retries. Every attempt checks freshness and
rereads ambiguous writes; completed actions are never replayed, and no model is called.
*/
package incident

import (
	"context"
	"errors"
	"reflect"
	"time"
)

type Application struct {
	reconciler Reconciler
	input      InvestigationContext
	actions    []Action
	cursor     int
	pending    *plannedAction
}

type plannedAction struct {
	expected *Record
	desired  Record
}

type PersistenceError struct {
	Operation string
	Err       error
}

// Error reports the repository's sanitized failure without model evidence or raw upstream bodies.
func (e *PersistenceError) Error() string {
	return "incident " + e.Operation + " unavailable: " + e.Err.Error()
}

// Unwrap preserves failure classification through application and runner diagnostics.
func (e *PersistenceError) Unwrap() error { return e.Err }

// NewApplication validates attached evidence and retains one decision for restart-free storage retries.
func (s Reconciler) NewApplication(in InvestigationContext, d Decision) (*Application, error) {
	if err := ValidateEvidence(d.Evidence); err != nil {
		return nil, err
	}

	in.Evidence = d.Evidence
	if in.Evidence != nil {
		for _, description := range in.Evidence.Descriptions {
			resource, ok := in.Identities[description.Resource.Ref]
			if !ok || resource.Kind != description.Resource.Kind || resource.APIGroup != description.Resource.APIGroup ||
				resource.Name != description.Resource.Name || resource.Namespace != description.Resource.Namespace {
				return nil, errors.New("description resource outside investigation scope")
			}
		}
	}

	return &Application{reconciler: s, input: in, actions: d.Actions}, nil
}

// Apply resumes unfinished actions under a bounded request context and fresh-evidence predicate.
// Partial confirmed changes are returned even on failure; exact ambiguous writes are reread on the next call.
func (s *Application) Apply(ctx context.Context, current func() bool) ([]Change, error) {
	changes := []Change{}

	for s.cursor < len(s.actions) {
		action := s.actions[s.cursor]

		change, err := s.applyAction(ctx, action, current)
		if err != nil {
			return changes, err
		}

		if change != nil {
			changes = append(changes, *change)
		}

		s.cursor++
		s.pending = nil
	}

	return changes, nil
}

// applyAction retains a single desired revision and rejects conflicting content or replaced Incident UIDs.
func (s *Application) applyAction(ctx context.Context, action Action, current func() bool) (*Change, error) {
	identity := Identity{s.reconciler.Observer, s.input.Identities[s.input.Anchor], action.Family}

	name := Name(identity)
	if action.IncidentID != "" && action.IncidentID != name {
		return nil, errors.New("incident action identity mismatch")
	}

	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, &PersistenceError{Operation: "reconciliation", Err: err}
		}

		if !current() {
			return nil, errors.New("AI conclusion superseded before persistence")
		}

		prior, err := s.reconciler.Repository.Get(ctx, name)
		if err != nil {
			return nil, &PersistenceError{Operation: "lookup", Err: err}
		}

		if s.pending == nil {
			if prior != nil && (prior.Value.Identity != identity || prior.Name != name) {
				return nil, errors.New("incident identity conflict")
			}

			if !matchesInput(prior, action, s.input) {
				return nil, errors.New("incident revision changed since AI review")
			}

			desired, err := next(prior, identity, action, s.input, s.reconciler.Model, time.Now().UTC())
			if err != nil {
				return nil, err
			}

			s.pending = &plannedAction{expected: prior, desired: desired}
		} else {
			if prior != nil && reflect.DeepEqual(prior.Value, s.pending.desired.Value) &&
				(s.pending.expected == nil || prior.UID == s.pending.expected.UID) {
				return s.pending.change(action), nil
			}

			if !sameValue(prior, s.pending.expected) {
				return nil, errors.New("incident changed during persistence reconciliation")
			}
		}

		if prior != nil && reflect.DeepEqual(prior.Value, s.pending.desired.Value) {
			return nil, nil
		}

		if prior != nil {
			s.pending.desired.Version, s.pending.desired.UID = prior.Version, prior.UID
		}

		if err := ctx.Err(); err != nil {
			return nil, &PersistenceError{Operation: "reconciliation", Err: err}
		}

		if !current() {
			return nil, errors.New("AI conclusion superseded before persistence")
		}

		if err := s.reconciler.Repository.Save(ctx, s.pending.desired); err != nil {
			if attempt == 2 {
				return nil, &PersistenceError{Operation: "write", Err: err}
			}

			select {
			case <-ctx.Done():
				return nil, &PersistenceError{Operation: "write", Err: ctx.Err()}
			case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
			}

			continue
		}

		return s.pending.change(action), nil
	}

	return nil, errors.New("incident reconciliation exhausted without a confirmed result")
}

// change describes only a confirmed material revision, preserving recurrence as reopen.
func (p *plannedAction) change(action Action) *Change {
	if p.expected != nil && reflect.DeepEqual(p.expected.Value, p.desired.Value) {
		return nil
	}

	operation := action.Operation
	if p.expected != nil && p.expected.Value.Lifecycle.State == "resolved" && p.desired.Value.Lifecycle.State == "open" {
		operation = "reopen"
	}

	return &Change{Operation: operation, Name: p.desired.Name, Incident: p.desired.Value}
}
