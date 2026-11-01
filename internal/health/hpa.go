/*
Purpose: Assess HPA conditions and provide safe scaling relationships and copies.
Flow: Projected HPA failures yield grace-aware private evidence for AI review.
Scale targets are same-namespace context, not ownership; normal limits and
disabled scaling do not independently imply incidents.
*/
package health

import "slices"

// MatchesHPATarget matches a supported current apps/v1 workload by name and namespace, never by metric selectors.
func MatchesHPATarget(hpa, target Object) bool {
	return hpa.Kind == "HorizontalPodAutoscaler" && hpa.Namespace == target.Namespace && target.UID != "" &&
		hpa.HPA.TargetVersion == "apps/v1" && slices.Contains([]string{"Deployment", "StatefulSet", "ReplicaSet"}, target.Kind) &&
		hpa.HPA.TargetKind == target.Kind && hpa.HPA.TargetName == target.Name
}

// HPAs emits grace-aware condition failures without treating scaling limits, disabled scaling or replica gaps as outages.
func HPAs(s Snapshot, p Policy) []Finding {
	var out []Finding

	for _, o := range s.Objects {
		if o.Kind != "HorizontalPodAutoscaler" || o.Deleting {
			continue
		}

		severity := "info"
		if expired(s.At, o.Created, p.StartupGrace) {
			severity = "warning"
		}

		for _, c := range o.Conditions {
			if c.Status != "False" {
				continue
			}

			rule, summary := "", ""

			switch c.Type {
			case "AbleToScale":
				rule, summary = "hpa.unable_to_scale", "HPA reports it cannot read or update its scale target; application impact is not established"
			case "ScalingActive":
				if c.Reason != "ScalingDisabled" {
					rule, summary = "hpa.inactive", "HPA reports scaling is inactive; this does not by itself establish an application outage"
				}
			}

			if rule != "" {
				out = append(out, NewFinding(rule, severity, o.Resource, "", summary, "condition="+c.Type))
			}
		}
	}

	return out
}

// Clone copies HPA facts so cache consumers cannot mutate pointer values or projected lists.
func (h HPAState) Clone() HPAState {
	if h.ObservedGeneration != nil {
		h.ObservedGeneration = new(*h.ObservedGeneration)
	}

	if h.LastScale != nil {
		h.LastScale = new(*h.LastScale)
	}

	h.Metrics, h.CurrentMetrics = cloneMetrics(h.Metrics), cloneMetrics(h.CurrentMetrics)
	h.ScaleUp, h.ScaleDown = cloneScalingRules(h.ScaleUp), cloneScalingRules(h.ScaleDown)

	return h
}

// cloneMetrics copies the approved metric list and optional utilization values.
func cloneMetrics(metrics []Metric) []Metric {
	out := slices.Clone(metrics)
	for i := range out {
		if out[i].Utilization != nil {
			out[i].Utilization = new(*out[i].Utilization)
		}
	}

	return out
}

// cloneScalingRules preserves omitted behavior while copying configured windows and policies.
func cloneScalingRules(rules *ScalingRules) *ScalingRules {
	if rules == nil {
		return nil
	}

	copy := *rules
	copy.Policies = slices.Clone(rules.Policies)

	if rules.Stabilization != nil {
		copy.Stabilization = new(*rules.Stabilization)
	}

	return &copy
}
