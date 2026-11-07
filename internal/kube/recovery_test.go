/*
Purpose: Test bounded semantic recovery history and collector continuity offline.
Flow: Projected fixtures verify heartbeat suppression, batched flaps, verified
owner propagation and deletion-capacity reuse. Local HTTP watch fixtures verify
fresh cache generations change on renewal; no live resources or AI are used.
*/
package kube

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
)

// recoveryEpoch creates an isolated bounded-history fixture with a real continuity counter.
func recoveryEpoch() *epoch {
	generations := &atomic.Uint64{}
	return &epoch{generation: generations.Add(1), generations: generations, recovery: map[health.Resource]recoveryEntry{}}
}

// TestRecoveryHistoryPreservesBatchedFlaps records transitions even when the final projected state is unchanged.
func TestRecoveryHistoryPreservesBatchedFlaps(t *testing.T) {
	e := recoveryEpoch()
	o := health.Object{Resource: health.Resource{Kind: "Node", Name: "node", UID: "node"},
		Conditions: []health.Condition{{Type: "Ready", Status: "True"}}}
	name := o.Resource
	name.UID = ""

	if err := e.observeRecovery(o, false, true, 4); err != nil {
		t.Fatal(err)
	}

	o.Version = "new-heartbeat"

	o.Conditions[0].Updated = time.Now()
	if err := e.observeRecovery(o, false, false, 4); err != nil || e.recovery[name].Change != 0 {
		t.Fatal("heartbeat advanced recovery history")
	}

	o.Conditions[0].Status = "False"
	if err := e.observeRecovery(o, false, false, 4); err != nil {
		t.Fatal(err)
	}

	o.Conditions[0].Status = "True"
	if err := e.observeRecovery(o, false, false, 4); err != nil || e.recovery[name].Change != 2 {
		t.Fatal("batched unhealthy-to-healthy transition was lost")
	}
}

// TestRecoveryHistoryPropagatesTemporaryChildren marks the verified controller after an unseen replica appears and disappears.
func TestRecoveryHistoryPropagatesTemporaryChildren(t *testing.T) {
	e := recoveryEpoch()
	deployment := health.Object{Resource: health.Resource{Kind: "Deployment", Namespace: "apps", Name: "app", UID: "deployment"}}
	rs := health.Object{Resource: health.Resource{Kind: "ReplicaSet", Namespace: "apps", Name: "rs", UID: "rs"}, Owner: deployment.Resource}

	pod := health.Object{Resource: health.Resource{Kind: "Pod", Namespace: "apps", Name: "temporary", UID: "pod"}, Owner: rs.Resource, Phase: "Failed"}
	for _, o := range []health.Object{deployment, rs} {
		if err := e.observeRecovery(o, false, true, 4); err != nil {
			t.Fatal(err)
		}
	}

	for _, deleted := range []bool{false, true} {
		if err := e.observeRecovery(pod, deleted, false, 4); err != nil {
			t.Fatal(err)
		}
	}

	name := deployment.Resource

	name.UID = ""
	if e.recovery[name].Change != 2 {
		t.Fatal("temporary failed replica did not reset controller recovery")
	}

	pod.Owner.UID = "unverified-rs"
	if err := e.observeRecovery(pod, false, false, 4); err != nil || e.recovery[name].Change != 2 {
		t.Fatal("mismatched owner UID propagated to a different controller incarnation")
	}
}

// TestRecoveryHistoryEvictionBreaksContinuity preserves live admission capacity without treating discarded absence history as continuous.
func TestRecoveryHistoryEvictionBreaksContinuity(t *testing.T) {
	e := recoveryEpoch()
	old := health.Object{Resource: health.Resource{Kind: "Pod", Namespace: "apps", Name: "old", UID: "old"}}
	current := health.Object{Resource: health.Resource{Kind: "Pod", Namespace: "apps", Name: "current", UID: "current"}}

	if err := e.observeRecovery(old, false, true, 1); err != nil {
		t.Fatal(err)
	}

	if err := e.observeRecovery(old, true, false, 1); err != nil {
		t.Fatal(err)
	}

	if err := e.observeRecovery(current, false, false, 1); err != nil || e.generation != 2 || len(e.recovery) != 1 {
		t.Fatal("deleted history blocked capacity reuse or preserved false continuity")
	}

	if err := e.observeRecovery(old, false, false, 1); err == nil {
		t.Fatal("live recovery history exceeded its hard bound")
	}
}

// TestSnapshotContinuityChangesOnWatchRenewal exercises real local HTTP streams and fresh cache rebuilding.
func TestSnapshotContinuityChangesOnWatchRenewal(t *testing.T) {
	var watches atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.URL.Query().Get("watch") == "true" {
			watches.Add(1)
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()

			return
		}

		items := []any{}
		if strings.HasSuffix(r.URL.Path, "/nodes") {
			items = append(items, node())
		}

		_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{"resourceVersion": "1"}, "items": items})
	}))
	defer server.Close()

	cfg := testConfig()
	cfg.WatchTimeout, cfg.RequestTimeout = 300*time.Millisecond, 100*time.Millisecond
	source := NewSource(clientFor(t, server), cfg)
	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() { defer close(done); source.Run(ctx) }()

	defer func() { cancel(); <-done }()

	var first uint64

	until(t, func() bool {
		snapshot, err := source.Snapshot(time.Now())
		if err == nil {
			first = snapshot.Continuity
		}

		return err == nil && first != 0
	})
	until(t, func() bool {
		snapshot, err := source.Snapshot(time.Now())
		return err == nil && snapshot.Continuity > first
	})

	if watches.Load() < 32 {
		t.Fatal("cache continuity advanced without renewing the resource streams")
	}
}
