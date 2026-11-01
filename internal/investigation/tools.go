/*
Purpose: Provide handle-scoped resource inspection and Warning event tools.
Flow: AI tool arguments resolve through host-side identities to bounded,
read-only Kubernetes requests; approved fields and redacted text return with
operation-named citations, not incident decisions or workload mutations.
*/
package investigation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
)

type Session struct {
	Client        dynamic.Interface
	Investigation incident.InvestigationContext
	Timeout       time.Duration
	Calls, Bytes  int
	Citations     map[string]bool
	Scope         config.Config
}

type Tool struct {
	Session       *Session
	RelatedEvents bool
}

// Name selects the registered local function name for inspection or related Warning events.
func (t Tool) Name() string {
	if t.RelatedEvents {
		return "related_warning_events"
	}

	return "inspect_resource"
}

// Description tells the model which bounded, read-only evidence the selected tool exposes.
func (t Tool) Description() string {
	if t.RelatedEvents {
		return "Read bounded, redacted Warning events for one authorised resource handle."
	}

	return "Read approved current state and redacted condition messages for one supplied resource handle; never raw objects."
}

// Schema permits exactly one resource handle argument and excludes arbitrary API parameters.
func (t Tool) Schema() any {
	return map[string]any{"type": "object", "properties": map[string]any{"resource_ref": map[string]any{"type": "string"}},
		"required": []string{"resource_ref"}, "additionalProperties": false}
}

// ReturnSchema leaves the result shape unspecified for the tool framework.
func (t Tool) ReturnSchema() any { return nil }

var resources = map[string]schema.GroupVersionResource{
	"Node": {Version: "v1", Resource: "nodes"}, "Pod": {Version: "v1", Resource: "pods"},
	"Deployment":    {Group: "apps", Version: "v1", Resource: "deployments"},
	"ReplicaSet":    {Group: "apps", Version: "v1", Resource: "replicasets"},
	"Job":           {Group: "batch", Version: "v1", Resource: "jobs"},
	"StatefulSet":   {Group: "apps", Version: "v1", Resource: "statefulsets"},
	"DaemonSet":     {Group: "apps", Version: "v1", Resource: "daemonsets"},
	"Service":       {Version: "v1", Resource: "services"},
	"EndpointSlice": {Group: "discovery.k8s.io", Version: "v1", Resource: "endpointslices"},
}

// Call validates a scoped handle before bounded Kubernetes reads and returns projected, redacted evidence.
// It tracks budgets, operation-named citations unique within the session and inspected resource versions.
func (t Tool) Call(ctx context.Context, args string) (any, error) {
	s := t.Session
	if s.Client == nil {
		return nil, errors.New("investigation Kubernetes client unavailable")
	}

	if len(args) > 1024 || s.Calls >= 8 {
		return nil, errors.New("investigation tool budget exceeded")
	}

	d := json.NewDecoder(strings.NewReader(args))

	open, err := d.Token()
	if err != nil || open != json.Delim('{') {
		return nil, errors.New("invalid tool arguments")
	}

	key, err := d.Token()
	if err != nil || key != "resource_ref" {
		return nil, errors.New("invalid tool argument name")
	}

	var ref string
	if d.Decode(&ref) != nil {
		return nil, errors.New("invalid resource handle")
	}

	close, err := d.Token()
	if err != nil || close != json.Delim('}') {
		return nil, errors.New("extra or duplicate tool arguments")
	}

	if _, err := d.Token(); err != io.EOF {
		return nil, errors.New("trailing tool arguments")
	}

	r, ok := s.Investigation.Identities[ref]

	gvr, known := resources[r.Kind]
	if !ok || !known {
		return nil, errors.New("unauthorised tool resource")
	}

	if r.Kind == "Node" {
		if r.Namespace != "" {
			return nil, errors.New("unauthorised node namespace")
		}
	} else if !s.Scope.WatchesNamespace(r.Namespace) {
		return nil, errors.New("tool resource outside namespace scope")
	}

	s.Calls++

	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	result := map[string]any{"resource_ref": ref, "observed_at": time.Now().UTC(), "complete": true}

	if t.RelatedEvents {
		namespaces := []string{r.Namespace}

		selector := "type=Warning,involvedObject.uid=" + r.UID
		if r.Kind == "Node" {
			namespaces = s.Scope.NamespaceWatch
			if len(namespaces) == 0 {
				namespaces = []string{metav1.NamespaceAll}
			}

			selector = "type=Warning,involvedObject.kind=Node,involvedObject.name=" + r.Name
		}

		if len(namespaces) == 0 || len(namespaces) > 20 {
			return nil, errors.New("event namespace scope unavailable or exceeds tool budget")
		}

		events := []map[string]any{}
		pages := 0

		for _, namespace := range namespaces {
			opts := metav1.ListOptions{Limit: 20, FieldSelector: selector}
			seen := map[string]bool{}
			version := ""

			for {
				pages++
				if pages > 20 {
					return nil, errors.New("related event reads exceed the tool request limit")
				}

				page, err := s.Client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "events"}).Namespace(namespace).List(ctx, opts)
				if err != nil {
					return nil, errors.New("related event read failed")
				}

				if page.GetResourceVersion() == "" || version != "" && version != page.GetResourceVersion() {
					return nil, errors.New("related event list lacks a consistent resource version")
				}

				version = page.GetResourceVersion()
				for _, obj := range page.Items {
					if len(validation.IsDNS1123Label(obj.GetNamespace())) != 0 || namespace != "" && obj.GetNamespace() != namespace {
						return nil, errors.New("event response outside namespace scope")
					}

					if !s.Scope.WatchesNamespace(obj.GetNamespace()) {
						continue
					}

					target, _, _ := unstructured.NestedString(obj.Object, "involvedObject", "uid")
					name, _, _ := unstructured.NestedString(obj.Object, "involvedObject", "name")

					kind, _, _ := unstructured.NestedString(obj.Object, "involvedObject", "kind")
					if (r.Kind != "Node" && target != r.UID) || (r.Kind == "Node" && (name != r.Name || kind != "Node" || target != "" && target != r.UID)) {
						return nil, errors.New("event response outside authorised resource")
					}

					eventType, _, _ := unstructured.NestedString(obj.Object, "type")
					if eventType != "Warning" {
						return nil, errors.New("unexpected non-warning event")
					}

					if len(events) >= 20 {
						return nil, errors.New("related events exceed the tool result limit")
					}

					entry := map[string]any{"namespace": obj.GetNamespace()}
					for _, key := range []string{"reason", "message", "count", "lastTimestamp"} {
						v, found, _ := unstructured.NestedFieldCopy(obj.Object, key)
						if found {
							entry[key] = redactValue(v)
						}
					}

					events = append(events, entry)
				}

				token := page.GetContinue()
				if token == "" {
					break
				}

				if seen[token] {
					return nil, errors.New("repeated related event pagination token")
				}

				seen[token] = true
				opts.Continue = token
			}
		}

		result["events"] = events
	} else {
		obj, err := s.Client.Resource(gvr).Namespace(r.Namespace).Get(ctx, r.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			result["exists"] = false
		} else if err != nil {
			return nil, errors.New("resource inspection failed")
		} else {
			if string(obj.GetUID()) != r.UID || obj.GetName() != r.Name || obj.GetNamespace() != r.Namespace || obj.GetKind() != r.Kind {
				return nil, errors.New("resource incarnation changed during investigation")
			}

			if s.Investigation.ToolVersions != nil {
				if obj.GetResourceVersion() == "" {
					return nil, errors.New("inspection response lacks a resource version")
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
					return nil, errors.New("invalid condition schema")
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
					return nil, errors.New("invalid container state")
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
					return nil, err
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
						return nil, errors.New("invalid container specification")
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
									return nil, errors.New("invalid container port specification")
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
		}
	}

	id := fmt.Sprintf("%s-%d", strings.ReplaceAll(t.Name(), "_", "-"), s.Calls)
	result["evidence_id"] = id

	data, err := json.Marshal(result)
	if err != nil || len(data) > 8192 || s.Bytes+len(data) > 32768 {
		return nil, errors.New("investigation evidence byte limit exceeded")
	}

	s.Bytes += len(data)
	s.Citations[id] = true

	return result, nil
}

// A regular expression to match sensitive information patterns in strings.
var sensitive = regexp.MustCompile(`(?i)(authorization|bearer|password|passwd|api[_-]?key|secret|token|://[^/\s]+:[^@\s]+@)`)

// redactValue recursively replaces strings matching sensitive patterns and truncates long strings.
// Other scalar values pass through; arbitrary text is not guaranteed secret-free.
func redactValue(v any) any {
	switch value := v.(type) {
	case string:
		if sensitive.MatchString(value) {
			return "[redacted sensitive text]"
		}

		if len(value) > 1024 {
			return value[:1024] + " [truncated]"
		}

		return value
	case map[string]any:
		out := map[string]any{}
		for k, v := range value {
			out[k] = redactValue(v)
		}

		return out
	case []any:
		out := make([]any, len(value))
		for i, v := range value {
			out[i] = redactValue(v)
		}

		return out
	default:
		return v
	}
}
