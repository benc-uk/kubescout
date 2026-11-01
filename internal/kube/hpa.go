/*
Purpose: Project autoscaling/v2 HPA state into bounded approved facts.
Flow: Typed API fields become scaling settings, numeric metric values and
condition metadata. Metric selectors, annotations and raw object references
are excluded; projection never queries metrics APIs or changes scale targets.
*/
package kube

import (
	"errors"

	"github.com/benc-uk/kubescout/internal/health"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
)

// projectHPA retains approved HPA settings and status after enforcing shared per-object projection bounds.
func projectHPA(hpa autoscalingv2.HorizontalPodAutoscaler, o *health.Object) error {
	if len(hpa.Spec.Metrics) > 32 || len(hpa.Status.CurrentMetrics) > 32 || len(hpa.Status.Conditions) > 32 ||
		len(hpa.Spec.ScaleTargetRef.Name) > 256 || len(hpa.Spec.ScaleTargetRef.Kind) > 128 || len(hpa.Spec.ScaleTargetRef.APIVersion) > 128 {
		return errors.New("HPA projection exceeds limit")
	}

	o.Generation = hpa.Generation
	o.HPA = health.HPAState{TargetVersion: hpa.Spec.ScaleTargetRef.APIVersion, TargetKind: hpa.Spec.ScaleTargetRef.Kind,
		TargetName: hpa.Spec.ScaleTargetRef.Name, Min: 1, Max: hpa.Spec.MaxReplicas,
		Current: hpa.Status.CurrentReplicas, Desired: hpa.Status.DesiredReplicas, ObservedGeneration: hpa.Status.ObservedGeneration}

	if hpa.Spec.MinReplicas != nil {
		o.HPA.Min = *hpa.Spec.MinReplicas
	}

	if hpa.Status.ObservedGeneration != nil {
		o.Observed = *hpa.Status.ObservedGeneration
	}

	if hpa.Status.LastScaleTime != nil {
		o.HPA.LastScale = new(hpa.Status.LastScaleTime.Time)
	}

	for _, condition := range hpa.Status.Conditions {
		o.Conditions = append(o.Conditions, health.Condition{Type: string(condition.Type), Status: string(condition.Status),
			Reason: condition.Reason, Transition: condition.LastTransitionTime.Time})
	}

	for _, spec := range hpa.Spec.Metrics {
		metric, target, err := hpaMetricSpec(spec)
		if err != nil {
			return err
		}

		metric.TargetType = string(target.Type)
		if target.Type != autoscalingv2.ValueMetricType && target.Type != autoscalingv2.AverageValueMetricType && target.Type != autoscalingv2.UtilizationMetricType {
			return errors.New("invalid HPA metric target type")
		}

		if target.Type == autoscalingv2.ValueMetricType && target.Value == nil ||
			target.Type == autoscalingv2.AverageValueMetricType && target.AverageValue == nil ||
			target.Type == autoscalingv2.UtilizationMetricType && target.AverageUtilization == nil {
			return errors.New("HPA metric target lacks its declared value")
		}

		metric.Utilization = target.AverageUtilization
		if target.Value != nil {
			metric.Value = target.Value.String()
		}

		if target.AverageValue != nil {
			metric.AverageValue = target.AverageValue.String()
		}

		o.HPA.Metrics = append(o.HPA.Metrics, metric)
	}

	for _, status := range hpa.Status.CurrentMetrics {
		metric, current, err := hpaMetricStatus(status)
		if err != nil {
			return err
		}

		metric.Utilization = current.AverageUtilization
		if current.Value != nil {
			metric.Value = current.Value.String()
		}

		if current.AverageValue != nil {
			metric.AverageValue = current.AverageValue.String()
		}

		o.HPA.CurrentMetrics = append(o.HPA.CurrentMetrics, metric)
	}

	if hpa.Spec.Behavior != nil {
		var err error

		o.HPA.ScaleUp, err = hpaScalingRules(hpa.Spec.Behavior.ScaleUp)
		if err != nil {
			return err
		}

		o.HPA.ScaleDown, err = hpaScalingRules(hpa.Spec.Behavior.ScaleDown)
		if err != nil {
			return err
		}
	}

	return nil
}

// hpaMetricSpec selects one known metric source and excludes its label selector.
func hpaMetricSpec(spec autoscalingv2.MetricSpec) (health.Metric, autoscalingv2.MetricTarget, error) {
	metric := health.Metric{Type: string(spec.Type)}
	target := autoscalingv2.MetricTarget{}

	switch {
	case spec.Type == autoscalingv2.ResourceMetricSourceType && spec.Resource != nil:
		metric.Name, target = string(spec.Resource.Name), spec.Resource.Target
	case spec.Type == autoscalingv2.ContainerResourceMetricSourceType && spec.ContainerResource != nil:
		metric.Name, metric.Container, target = string(spec.ContainerResource.Name), spec.ContainerResource.Container, spec.ContainerResource.Target
	case spec.Type == autoscalingv2.PodsMetricSourceType && spec.Pods != nil:
		metric.Name, target = spec.Pods.Metric.Name, spec.Pods.Target
	case spec.Type == autoscalingv2.ExternalMetricSourceType && spec.External != nil:
		metric.Name, target = spec.External.Metric.Name, spec.External.Target
	case spec.Type == autoscalingv2.ObjectMetricSourceType && spec.Object != nil:
		metric.Name, target = spec.Object.Metric.Name, spec.Object.Target
		metric.ObjectVersion, metric.ObjectKind, metric.ObjectName = spec.Object.DescribedObject.APIVersion, spec.Object.DescribedObject.Kind, spec.Object.DescribedObject.Name
	default:
		return metric, target, errors.New("invalid HPA metric source")
	}

	return metric, target, validateMetricNames(metric)
}

// hpaMetricStatus selects approved numeric observations without retaining metric selectors or authorizing object reads.
func hpaMetricStatus(status autoscalingv2.MetricStatus) (health.Metric, autoscalingv2.MetricValueStatus, error) {
	metric := health.Metric{Type: string(status.Type)}
	current := autoscalingv2.MetricValueStatus{}

	switch {
	case status.Type == autoscalingv2.ResourceMetricSourceType && status.Resource != nil:
		metric.Name, current = string(status.Resource.Name), status.Resource.Current
	case status.Type == autoscalingv2.ContainerResourceMetricSourceType && status.ContainerResource != nil:
		metric.Name, metric.Container, current = string(status.ContainerResource.Name), status.ContainerResource.Container, status.ContainerResource.Current
	case status.Type == autoscalingv2.PodsMetricSourceType && status.Pods != nil:
		metric.Name, current = status.Pods.Metric.Name, status.Pods.Current
	case status.Type == autoscalingv2.ExternalMetricSourceType && status.External != nil:
		metric.Name, current = status.External.Metric.Name, status.External.Current
	case status.Type == autoscalingv2.ObjectMetricSourceType && status.Object != nil:
		metric.Name, current = status.Object.Metric.Name, status.Object.Current
		metric.ObjectVersion, metric.ObjectKind, metric.ObjectName = status.Object.DescribedObject.APIVersion, status.Object.DescribedObject.Kind, status.Object.DescribedObject.Name
	default:
		return metric, current, errors.New("invalid HPA metric source")
	}

	return metric, current, validateMetricNames(metric)
}

// validateMetricNames rejects oversized identifiers before cache admission or model-facing projection.
func validateMetricNames(metric health.Metric) error {
	for _, value := range []string{metric.Name, metric.Container, metric.ObjectVersion, metric.ObjectKind, metric.ObjectName} {
		if len(value) > 256 {
			return errors.New("HPA metric identifier exceeds limit")
		}
	}

	return nil
}

// hpaScalingRules preserves optional behavior with bounded numeric policies and no raw configuration maps.
func hpaScalingRules(rules *autoscalingv2.HPAScalingRules) (*health.ScalingRules, error) {
	if rules == nil {
		return nil, nil
	}

	if len(rules.Policies) > 16 {
		return nil, errors.New("HPA scaling policy projection exceeds limit")
	}

	out := &health.ScalingRules{Stabilization: rules.StabilizationWindowSeconds}
	if rules.SelectPolicy != nil {
		out.SelectPolicy = string(*rules.SelectPolicy)
	}

	if rules.Tolerance != nil {
		out.Tolerance = rules.Tolerance.String()
	}

	for _, policy := range rules.Policies {
		out.Policies = append(out.Policies, health.ScalingPolicy{Type: string(policy.Type), Value: policy.Value, PeriodSeconds: policy.PeriodSeconds})
	}

	return out, nil
}
