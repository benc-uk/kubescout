/*
Purpose: Implement the handle-scoped inspect_resource local tool.
Flow: Shared session validation authorizes a bounded Kubernetes GET; identity
checks and approved projections protect credential values and track inspected
resource versions or absence, consistent with automatic description reads. No raw
objects, credentials or workload writes are exposed.
Storage inspection excludes all volume sources, raw claim references and storage paths.
CronJob inspection includes approved schedule/status facts and template container specifications.
HPA inspection shares credential protection and excludes metric selectors and metrics API calls.
Ingress inspection returns configuration, not TLS validity or network reachability.
Attachment inspection returns bounded driver errors without metadata or inline volume configuration.
*/
package investigation

import (
	"context"
	"errors"
	"strings"

	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// InspectResource exposes approved current state for an authorized resource handle.
type InspectResource struct {
	Session *Session
}

// Name identifies the resource inspection function registered with the model.
func (t InspectResource) Name() string { return "inspect_resource" }

// Description tells the model that inspection returns approved fields, never raw objects.
func (t InspectResource) Description() string {
	return "Read approved current state and credential-protected condition messages for one supplied resource handle; never raw objects."
}

// Schema restricts inspection arguments to a single supplied resource handle.
func (t InspectResource) Schema() any { return resourceRefSchema() }

// ReturnSchema leaves the result shape unspecified for the tool framework.
func (t InspectResource) ReturnSchema() any { return nil }

// Call inspects an authorized handle under the investigation's shared limits and diagnostics.
func (t InspectResource) Call(ctx context.Context, args string) (any, error) {
	return t.Session.call(ctx, t.Name(), args, t.inspect)
}

// inspect reads one resource incarnation and projects approved state with credential values removed.
// Successful reads track resource versions or absence for later stale-result rejection.
func (t InspectResource) inspect(ctx context.Context, ref string, r health.Resource, result map[string]any) error {
	s := t.Session

	obj, err := s.Client.Resource(resources[r.Kind]).Namespace(r.Namespace).Get(ctx, r.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		result["exists"] = false

		if s.Investigation.ToolVersions != nil {
			if version, read := s.Investigation.ToolVersions[ref]; read && version != "" {
				return errors.New("resource disappeared between investigation reads")
			}

			s.Investigation.ToolVersions[ref] = ""
		}

		return nil
	}

	if err != nil {
		return &UnavailableError{Operation: "resource inspection"}
	}

	if string(obj.GetUID()) != r.UID || obj.GetName() != r.Name || obj.GetNamespace() != r.Namespace || obj.GetKind() != r.Kind {
		return errors.New("resource incarnation changed during investigation")
	}

	if s.Investigation.ToolVersions != nil {
		if obj.GetResourceVersion() == "" {
			return errors.New("inspection response lacks a resource version")
		}

		if version, read := s.Investigation.ToolVersions[ref]; read && version != obj.GetResourceVersion() {
			return errors.New("resource changed between investigation reads")
		}

		s.Investigation.ToolVersions[ref] = obj.GetResourceVersion()
	}

	result["exists"] = true
	fields := map[string]any{}

	if r.Kind == "VolumeAttachment" {
		return inspectAttachment(obj, r, s, result)
	}

	if r.Kind == "PersistentVolume" || r.Kind == "PersistentVolumeClaim" {
		return inspectStorage(obj, r, s, fields, result)
	}

	if r.Kind == "HorizontalPodAutoscaler" {
		return inspectHPA(obj, r, s, result)
	}

	if r.Kind == "Ingress" {
		return inspectIngress(obj, r, s, result)
	}

	paths := [][]string{{"status", "phase"}, {"status", "replicas"}, {"status", "availableReplicas"}, {"status", "updatedReplicas"}, {"status", "failed"}, {"status", "succeeded"}, {"status", "capacity"}, {"status", "allocatable"},
		{"spec", "nodeName"}, {"spec", "nodeSelector"}, {"spec", "replicas"}, {"spec", "backoffLimit"},
		{"spec", "tolerations"}, {"spec", "affinity"}, {"spec", "schedulingGates"}, {"spec", "taints"},
		{"spec", "updateStrategy"}, {"spec", "minReadySeconds"},
		{"status", "observedGeneration"}, {"status", "readyReplicas"}, {"status", "currentReplicas"},
		{"status", "currentRevision"}, {"status", "updateRevision"}, {"status", "desiredNumberScheduled"},
		{"status", "currentNumberScheduled"}, {"status", "updatedNumberScheduled"}, {"status", "numberReady"},
		{"status", "numberAvailable"}, {"status", "numberUnavailable"}, {"status", "numberMisscheduled"}}

	if r.Kind == "CronJob" {
		o, err := kube.ProjectResource(obj, r.Kind, r.Namespace)
		if err != nil {
			return err
		}

		fields["spec.schedule"] = redactValue(o.CronJob.Schedule)
		fields["spec.suspend"] = o.CronJob.Suspended
		fields["spec.concurrencyPolicy"] = redactValue(o.CronJob.ConcurrencyPolicy)
		fields["spec.successfulJobsHistoryLimit"] = o.CronJob.SuccessfulHistory
		fields["spec.failedJobsHistoryLimit"] = o.CronJob.FailedHistory
		fields["status.active_count"] = o.CronJob.Active

		if o.CronJob.TimeZone != "" {
			fields["spec.timeZone"] = redactValue(o.CronJob.TimeZone)
		}

		if o.CronJob.StartingDeadline != nil {
			fields["spec.startingDeadlineSeconds"] = *o.CronJob.StartingDeadline
		}

		if o.CronJob.LastSchedule != nil {
			fields["status.lastScheduleTime"] = o.CronJob.LastSchedule
		}

		if o.CronJob.LastSuccess != nil {
			fields["status.lastSuccessfulTime"] = o.CronJob.LastSuccess
		}

		paths = nil
	}

	for _, path := range paths {
		v, found, _ := unstructured.NestedFieldCopy(obj.Object, path...)
		if found {
			fields[strings.Join(path, ".")] = redactValue(v)
		}
	}

	safeConditions, err := conditionFields(obj)
	if err != nil {
		return err
	}

	fields["conditions"] = safeConditions
	containers, _, _ := unstructured.NestedSlice(obj.Object, "status", "containerStatuses")
	safeContainers := []map[string]any{}

	for _, raw := range containers {
		c, ok := raw.(map[string]any)
		if !ok {
			return errors.New("invalid container state")
		}

		value := map[string]any{}

		for _, k := range []string{"name", "image", "ready", "restartCount"} {
			if v, ok := c[k]; ok {
				value[k] = redactValue(v)
			}
		}

		for _, phase := range []string{"waiting", "terminated"} {
			state, _, _ := unstructured.NestedMap(c, "state", phase)
			safe := map[string]any{}

			for _, k := range []string{"reason", "exitCode", "message"} {
				if v, ok := state[k]; ok {
					safe[k] = redactValue(v)
				}
			}

			if len(safe) > 0 {
				value[phase] = safe
			}
		}

		safeContainers = append(safeContainers, value)
	}

	fields["containers"] = safeContainers

	if r.Kind == "Service" || r.Kind == "EndpointSlice" {
		projected, err := kube.ProjectResource(obj, r.Kind, r.Namespace)
		if err != nil {
			return err
		}

		ports := []map[string]any{}

		for _, port := range projected.Ports {
			value := map[string]any{"name": redactValue(port.Name), "protocol": redactValue(port.Protocol)}
			if port.Number != 0 {
				value["port"] = port.Number
			}

			if port.Target != "" {
				value["target_port"] = redactValue(port.Target)
			}

			ports = append(ports, value)
		}

		fields["ports"] = ports
		if r.Kind == "Service" {
			fields["spec.type"] = redactValue(projected.ServiceType)
			fields["spec.selector_present"] = len(projected.Selector) != 0
			fields["spec.publishNotReadyAddresses"] = projected.PublishNotReady
		} else {
			fields["address_type"] = redactValue(projected.AddressType)
			fields["endpoint_counts"] = health.CountEndpoints(projected.Endpoints)
		}
	}

	for _, kind := range []string{"containers", "initContainers"} {
		path := []string{"spec", kind}
		if r.Kind == "Deployment" || r.Kind == "StatefulSet" || r.Kind == "DaemonSet" || r.Kind == "Job" {
			path = []string{"spec", "template", "spec", kind}
		} else if r.Kind == "CronJob" {
			path = []string{"spec", "jobTemplate", "spec", "template", "spec", kind}
		}

		specs, _, _ := unstructured.NestedSlice(obj.Object, path...)
		projected := []map[string]any{}

		for _, raw := range specs {
			c, ok := raw.(map[string]any)
			if !ok {
				return errors.New("invalid container specification")
			}

			entry := map[string]any{}

			for _, key := range []string{"name", "image", "resources"} {
				if value, ok := c[key]; ok {
					entry[key] = redactValue(value)
				}

				if ports, ok := c["ports"].([]any); ok {
					safe := []map[string]any{}

					for _, raw := range ports {
						port, ok := raw.(map[string]any)
						if !ok {
							return errors.New("invalid container port specification")
						}

						value := map[string]any{}

						for _, key := range []string{"name", "containerPort", "protocol"} {
							if v, ok := port[key]; ok {
								value[key] = redactValue(v)
							}
						}

						safe = append(safe, value)
					}

					entry["ports"] = safe
				}
			}

			projected = append(projected, entry)
		}

		fields["spec."+kind] = projected
	}

	result["state"] = fields

	return nil
}

// inspectStorage returns bounded storage facts and authorized binding handles, never backend configuration.
func inspectStorage(obj *unstructured.Unstructured, r health.Resource, s *Session, fields, result map[string]any) error {
	o, err := kube.ProjectResource(obj, r.Kind, r.Namespace)
	if err != nil {
		return err
	}

	fields["status.phase"] = redactValue(o.Phase)
	fields["storage_class"] = redactValue(o.Storage.Class)
	fields["capacity"] = redactValue(o.Storage.Capacity)
	fields["volume_mode"] = redactValue(o.Storage.Mode)
	modes := []any{}

	for _, mode := range o.Storage.AccessModes {
		modes = append(modes, redactValue(mode))
	}

	fields["access_modes"] = modes
	if r.Kind == "PersistentVolumeClaim" {
		fields["requested_storage"] = redactValue(o.Storage.Requested)
	} else {
		fields["reclaim_policy"] = redactValue(o.Storage.ReclaimPolicy)
	}

	safe, err := conditionFields(obj)
	if err != nil {
		return err
	}

	if len(safe) > 32 {
		return errors.New("storage condition projection exceeds limit")
	}

	fields["conditions"] = safe

	if r.Kind == "PersistentVolume" {
		for _, key := range []string{"reason", "message"} {
			value, found, err := unstructured.NestedString(obj.Object, "status", key)
			if err != nil {
				return errors.New("invalid storage status schema")
			}

			if found {
				fields["status."+key] = redactValue(value)
			}
		}
	}

	for ref, candidate := range s.Investigation.Objects {
		if _, authorized := s.Investigation.Identities[ref]; !authorized {
			continue
		}

		if health.MatchesVolumeClaim(o, candidate) {
			fields["bound_claim_ref"] = ref
		}

		if health.MatchesVolumeClaim(candidate, o) {
			fields["bound_volume_ref"] = ref
		}
	}

	result["state"] = fields

	return nil
}

// conditionFields validates condition records and redacts the shared approved field allow-list.
func conditionFields(obj *unstructured.Unstructured) ([]map[string]any, error) {
	conditions, _, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil {
		return nil, errors.New("invalid condition schema")
	}

	out := []map[string]any{}

	for _, raw := range conditions {
		condition, ok := raw.(map[string]any)
		if !ok {
			return nil, errors.New("invalid condition schema")
		}

		value := map[string]any{}

		for _, key := range []string{"type", "status", "reason", "message", "lastTransitionTime"} {
			if v, ok := condition[key]; ok {
				value[key] = redactValue(v)
			}
		}

		out = append(out, value)
	}

	return out, nil
}
