/*
Purpose: Derive Deployment rollout, replica shortfall and failure evidence.
Flow: Projected conditions, generations and counts are evaluated with rollout
grace and pause handling. The resulting deterministic findings inform AI
assessment without raising or resolving incidents directly.
*/
package health

import "fmt"

// Deployments evaluates observed generations, rollout failures and replica shortfalls,
// applying progress-aware grace and pause handling while skipping scaled-to-zero workloads.
func Deployments(s Snapshot, p Policy) []Finding {
	var out []Finding

	for _, o := range s.Objects {
		if o.Kind != "Deployment" || o.Desired == 0 {
			continue
		}

		progress, _ := condition(o, "Progressing")
		available, _ := condition(o, "Available")
		failure, _ := condition(o, "ReplicaFailure")

		last := o.Created
		for _, at := range []struct {
			use bool
			c   Condition
		}{{true, progress}, {available.Status == "False", available}} {
			if at.use && at.c.Transition.After(last) {
				last = at.c.Transition
			}
		}

		if progress.Updated.After(last) {
			last = progress.Updated
		}

		severity := "info"
		if expired(s.At, last, p.RolloutGrace) {
			severity = "warning"
		}

		evidence := []string{
			fmt.Sprintf("desired=%d", o.Desired), fmt.Sprintf("available=%d", o.Available),
			fmt.Sprintf("updated=%d", o.Updated), fmt.Sprintf("generation=%d", o.Generation),
			fmt.Sprintf("observed_generation=%d", o.Observed),
		}
		if o.Observed < o.Generation {
			out = append(out, NewFinding("deployment.unobserved", severity, o.Resource, "", "Deployment controller has not observed the desired generation", evidence...))
			continue
		}

		if !o.Paused && progress.Status == "False" && progress.Reason == "ProgressDeadlineExceeded" {
			out = append(out, NewFinding("deployment.progress_deadline", "critical", o.Resource, "", "Deployment exceeded its Kubernetes progress deadline"))
		}

		if failure.Status == "True" {
			out = append(out, NewFinding("deployment.replica_failure", "critical", o.Resource, "", "Deployment reports a replica failure", "reason="+failure.Reason))
		}

		if o.Available < o.Desired || (!o.Paused && o.Updated < o.Desired) {
			out = append(out, NewFinding("deployment.replicas", severity, o.Resource, "", "Deployment has not reached the desired available and updated replica counts", evidence...))
		}
	}

	return out
}
