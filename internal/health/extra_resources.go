/*
Purpose: Evaluate configured extra-resource wellness rules as private AI evidence.
Flow: Projected facts and the observation clock feed compiled local CEL checks under
snapshot cost/deadline limits. Errors block assessment and recovery, never publish incidents.
*/
package health

import (
	"context"
	"encoding/json"
	"time"

	"github.com/benc-uk/kubescout/internal/resourcepolicy"
)

// ExtraResources evaluates configured resource facts, preserving explicit failures and shared execution bounds.
func ExtraResources(snapshot Snapshot, policy Policy) ([]Finding, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	return evaluateExtraResources(ctx, snapshot, policy)
}

// evaluateExtraResources applies the shared measured cost limit under the supplied assessment deadline.
func evaluateExtraResources(ctx context.Context, snapshot Snapshot, policy Policy) ([]Finding, error) {
	findings := []Finding{}
	cost := uint64(0)

	for _, object := range snapshot.Objects {
		if object.Extra == nil {
			continue
		}

		var definition *resourcepolicy.Resource

		for i := range policy.ExtraResources {
			r := &policy.ExtraResources[i]
			if r.Group == object.APIGroup && r.Kind == object.Kind {
				definition = r
				break
			}
		}

		if definition == nil {
			return nil, &resourcepolicy.EvaluationError{Reason: "missing_policy"}
		}

		rules, used, err := definition.Evaluate(ctx, object.Extra, snapshot.At, object.Created, object.Deleting)
		cost += used

		if err != nil {
			return nil, err
		}

		if cost > resourcepolicy.MaxSnapshotCost || ctx.Err() != nil {
			return nil, &resourcepolicy.EvaluationError{Reason: "snapshot_cost_or_deadline"}
		}

		data, err := json.Marshal(object.Extra)
		if err != nil || len(data) > resourcepolicy.MaxFactsBytes {
			return nil, &resourcepolicy.EvaluationError{Reason: "invalid_projected_evidence"}
		}

		evidence := "facts=" + string(data)

		for _, rule := range rules {
			findings = append(findings, NewFinding("extra."+definition.ID+"."+rule.ID, rule.Severity,
				object.Resource, "", rule.Summary, evidence))
		}
	}

	Sort(findings)

	return findings, nil
}
