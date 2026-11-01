/*
Purpose: Return bounded approved HPA state through the existing inspection tool.
Flow: A typed HPA projection becomes redacted metric and behavior facts with
an optional already-authorized target handle. No selector, arbitrary target
GET, metrics API request or scaling write is permitted.
*/
package investigation

import (
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/kube"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// inspectHPA returns approved scaling observations and a verified supplied target handle.
func inspectHPA(obj *unstructured.Unstructured, r health.Resource, s *Session, result map[string]any) error {
	o, err := kube.ProjectResource(obj, r.Kind, r.Namespace)
	if err != nil {
		return err
	}

	conditions, err := conditionFields(obj)
	if err != nil {
		return err
	}

	fields := map[string]any{
		"scale_target": map[string]any{"api_version": redactValue(o.HPA.TargetVersion), "kind": redactValue(o.HPA.TargetKind), "name": redactValue(o.HPA.TargetName)},
		"generation":   o.Generation,
		"min_replicas": o.HPA.Min, "max_replicas": o.HPA.Max, "current_replicas": o.HPA.Current, "desired_replicas": o.HPA.Desired,
		"metrics": metricFields(o.HPA.Metrics), "current_metrics": metricFields(o.HPA.CurrentMetrics), "conditions": conditions,
	}

	if o.HPA.ObservedGeneration != nil {
		fields["observed_generation"] = *o.HPA.ObservedGeneration
	}

	if o.HPA.LastScale != nil {
		fields["last_scale_time"] = o.HPA.LastScale
	}

	if o.HPA.ScaleUp != nil {
		fields["scale_up"] = scalingFields(o.HPA.ScaleUp)
	}

	if o.HPA.ScaleDown != nil {
		fields["scale_down"] = scalingFields(o.HPA.ScaleDown)
	}

	for ref, target := range s.Investigation.Objects {
		if identity, authorized := s.Investigation.Identities[ref]; authorized && identity == target.Resource && health.MatchesHPATarget(o, target) {
			fields["scale_target_ref"] = ref
		}
	}

	result["state"] = fields

	return nil
}

// metricFields redacts approved identifiers and returns numeric values without metric label selectors.
func metricFields(metrics []health.Metric) []map[string]any {
	out := []map[string]any{}

	for _, metric := range metrics {
		value := map[string]any{"type": redactValue(metric.Type), "name": redactValue(metric.Name)}

		for key, text := range map[string]string{"container": metric.Container, "target_type": metric.TargetType,
			"value": metric.Value, "average_value": metric.AverageValue} {
			if text != "" {
				value[key] = redactValue(text)
			}
		}

		if metric.ObjectKind != "" {
			value["described_object"] = map[string]any{"api_version": redactValue(metric.ObjectVersion),
				"kind": redactValue(metric.ObjectKind), "name": redactValue(metric.ObjectName)}
		}

		if metric.Utilization != nil {
			value["average_utilization"] = *metric.Utilization
		}

		out = append(out, value)
	}

	return out
}

// scalingFields returns only optional stabilization, tolerance and approved numeric scaling policies.
func scalingFields(rules *health.ScalingRules) map[string]any {
	out := map[string]any{}
	policies := []map[string]any{}

	for _, policy := range rules.Policies {
		policies = append(policies, map[string]any{"type": redactValue(policy.Type), "value": policy.Value, "period_seconds": policy.PeriodSeconds})
	}

	out["policies"] = policies
	if rules.Stabilization != nil {
		out["stabilization_window_seconds"] = *rules.Stabilization
	}

	if rules.SelectPolicy != "" {
		out["select_policy"] = redactValue(rules.SelectPolicy)
	}

	if rules.Tolerance != "" {
		out["tolerance"] = redactValue(rules.Tolerance)
	}

	return out
}
