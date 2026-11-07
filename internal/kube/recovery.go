/*
Purpose: Retain bounded host-only recovery change history for projected resources.
Flow: Initial lists establish verified owner links; meaningful watch transitions
advance per-name markers through known owners before notifications are coalesced.
Deleted slots retain brief appearances until bounded eviction or cache rebuilding.
Eviction advances continuity so discarded history cannot fake stable absence.
History errors invalidate collection, never an Incident.
*/
package kube

import (
	"errors"

	"github.com/benc-uk/kubescout/internal/health"
)

type recoveryEntry struct {
	Resource, Owner health.Resource
	State           string
	Change          uint64
	Present         bool
}

// observeRecovery records semantic transitions and verified owner markers within a bounded cache epoch.
// Initial lists and heartbeat-only updates preserve markers; deletion and replacement advance them.
func (e *epoch) observeRecovery(o health.Object, deleted, initial bool, limit int) error {
	if o.Kind == "Event" {
		return nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	name := o.Resource
	name.UID = ""

	prior, err := e.recoverySlot(name, limit)
	if err != nil {
		return err
	}

	state := health.RecoveryFingerprint(o)
	if !initial && !deleted && prior.Present && prior.Resource == o.Resource && prior.State == state {
		return nil
	}

	entry := recoveryEntry{Resource: o.Resource, Owner: o.Owner, State: state, Change: prior.Change, Present: !deleted}

	if !initial {
		e.recoverySequence++
		entry.Change = e.recoverySequence
	}

	e.recovery[name] = entry

	if initial {
		return nil
	}

	parent := o.Owner
	for depth := 0; depth < 3 && parent.UID != ""; depth++ {
		name := parent
		name.UID = ""

		entry, err := e.recoverySlot(name, limit)
		if err != nil {
			return err
		}

		if entry.Resource.UID == "" {
			entry.Resource = parent
		}

		entry.Change = e.recoverySequence
		e.recovery[name] = entry

		if entry.Resource != parent {
			break
		}

		parent = entry.Owner
	}

	return nil
}

// recoverySlot reserves bounded history, evicting only absent slots and invalidating recovery continuity.
// The caller holds the epoch lock; current observed resources never lose their owner/change markers.
func (e *epoch) recoverySlot(name health.Resource, limit int) (recoveryEntry, error) {
	if entry, ok := e.recovery[name]; ok {
		return entry, nil
	}

	if len(e.recovery) >= limit {
		var (
			oldest health.Resource
			change uint64
		)

		for key, entry := range e.recovery {
			if !entry.Present && (oldest.Kind == "" || entry.Change < change) {
				oldest, change = key, entry.Change
			}
		}

		if oldest.Kind == "" {
			return recoveryEntry{}, errors.New("recovery change history limit exceeded; rebuilding scoped caches")
		}

		delete(e.recovery, oldest)
		e.generation = e.generations.Add(1)
	}

	return recoveryEntry{}, nil
}
