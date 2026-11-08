/*
Purpose: Verify shared extra-resource CEL assessment cost and private evidence bounds.
Flow: Offline compiled policies and projected fixtures measure actual evaluator cost,
check snapshot thresholds and retain explicit missing-policy failures without incidents.
*/
package health

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/resourcepolicy"
)

// TestExtraResourceSnapshotCost verifies measured cost thresholds independently of race-detector timing overhead.
func TestExtraResourceSnapshotCost(t *testing.T) {
	definitions, err := resourcepolicy.Parse(`version: 1
resources:
  - id: tests
    group: example.io
    version: v1
    resource: widgets
    kind: Widget
    scope: Cluster
    fields:
      - name: values
        path: [status, values]
        type: strings
    rules:
      - id: identical
        expression: fields.values.all(a, fields.values.all(b, a == b))
        severity: info
        summary: Values match.
`)
	if err != nil {
		t.Fatal(err)
	}

	values := make([]any, 7)
	for i := range values {
		values[i] = strings.Repeat("x", 512)
	}

	facts := map[string]any{"values": values}
	now := time.Now()

	_, cost, err := definitions[0].Evaluate(t.Context(), facts, now, now, false)
	if err != nil || cost == 0 || cost > resourcepolicy.MaxRuleCost {
		t.Fatalf("cannot measure per-object cost: %d %v", cost, err)
	}

	count := int(resourcepolicy.MaxSnapshotCost / cost)
	policy := DefaultPolicy()
	policy.ExtraResources = definitions
	o := Object{Resource: Resource{Kind: "Widget", APIGroup: "example.io", Name: "widget", UID: "widget"}, Extra: facts}
	snapshot := Snapshot{At: now, Objects: make([]Object, count)}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	for i := range snapshot.Objects {
		snapshot.Objects[i] = o
	}

	if _, err := evaluateExtraResources(ctx, snapshot, policy); err != nil {
		t.Fatalf("cost below threshold failed: %v", err)
	}

	snapshot.Objects = append(snapshot.Objects, o)
	_, err = evaluateExtraResources(ctx, snapshot, policy)

	var evaluation *resourcepolicy.EvaluationError

	if !errors.As(err, &evaluation) || evaluation.Reason != "snapshot_cost_or_deadline" {
		t.Fatalf("cost above threshold was not rejected: %v", err)
	}
}
