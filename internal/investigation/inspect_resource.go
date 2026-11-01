/*
Purpose: Implement the handle-scoped inspect_resource local tool.
Flow: Shared session validation authorizes a bounded Kubernetes GET; identity
checks and approved projections return redacted state and track inspected
resource versions. No raw objects, credentials or workload writes are exposed.
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
	return "Read approved current state and redacted condition messages for one supplied resource handle; never raw objects."
}

// Schema restricts inspection arguments to a single supplied resource handle.
func (t InspectResource) Schema() any { return resourceRefSchema() }

// ReturnSchema leaves the result shape unspecified for the tool framework.
func (t InspectResource) ReturnSchema() any { return nil }

// Call inspects an authorized handle under the investigation's shared limits and diagnostics.
func (t InspectResource) Call(ctx context.Context, args string) (any, error) {
	return t.Session.call(ctx, t.Name(), args, t.inspect)
}

// inspect reads one resource incarnation and projects only approved, redacted state.
// Successful reads track resource versions for later stale-result rejection.
func (t InspectResource) inspect(ctx context.Context, ref string, r health.Resource, result map[string]any) error {
	s := t.Session

	obj, err := s.Client.Resource(resources[r.Kind]).Namespace(r.Namespace).Get(ctx, r.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		result["exists"] = false
		return nil
	}

	if err != nil {
		return errors.New("resource inspection failed")
	}

	if string(obj.GetUID()) != r.UID || obj.GetName() != r.Name || obj.GetNamespace() != r.Namespace || obj.GetKind() != r.Kind {
		return errors.New("resource incarnation changed during investigation")
	}

	if s.Investigation.ToolVersions != nil {
		if obj.GetResourceVersion() == "" {
			return errors.New("inspection response lacks a resource version")
		}

		s.Investigation.ToolVersions[ref] = obj.GetResourceVersion()
	}

	result["exists"] = true
	fields := map[string]any{}

	paths := [][]string{{"status", "phase"}, {"status", "replicas"}, {"status", "availableReplicas"}, {"status", "updatedReplicas"}, {"status", "failed"}, {"status", "succeeded"}, {"status", "capacity"}, {"status", "allocatable"},
		{"spec", "nodeName"}, {"spec", "nodeSelector"}, {"spec", "replicas"}, {"spec", "backoffLimit"},
		{"spec", "tolerations"}, {"spec", "affinity"}, {"spec", "schedulingGates"}, {"spec", "taints"},
		{"spec", "updateStrategy"}, {"spec", "minReadySeconds"},
		{"status", "observedGeneration"}, {"status", "readyReplicas"}, {"status", "currentReplicas"},
		{"status", "currentRevision"}, {"status", "updateRevision"}, {"status", "desiredNumberScheduled"},
		{"status", "currentNumberScheduled"}, {"status", "updatedNumberScheduled"}, {"status", "numberReady"},
		{"status", "numberAvailable"}, {"status", "numberUnavailable"}, {"status", "numberMisscheduled"}}
	for _, path := range paths {
		v, found, _ := unstructured.NestedFieldCopy(obj.Object, path...)
		if found {
			fields[strings.Join(path, ".")] = redactValue(v)
		}
	}

	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	safeConditions := []map[string]any{}

	for _, raw := range conditions {
		c, ok := raw.(map[string]any)
		if !ok {
			return errors.New("invalid condition schema")
		}

		value := map[string]any{}

		for _, k := range []string{"type", "status", "reason", "message", "lastTransitionTime"} {
			if v, ok := c[k]; ok {
				value[k] = redactValue(v)
			}
		}

		safeConditions = append(safeConditions, value)
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
