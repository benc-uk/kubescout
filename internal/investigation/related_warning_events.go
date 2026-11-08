/*
Purpose: Implement the handle-scoped related_warning_events local tool.
Flow: Shared session validation authorizes paginated Kubernetes Warning reads;
namespace and target checks filter scoped events before bounded, credential-protected
projection. Invalid or incomplete reads return errors, not accepted evidence.
Failed reads remain errors, never accepted empty events or healthy evidence.
Configured cluster-scoped resources use the same event namespace filters and API-group target checks.
*/
package investigation

import (
	"context"
	"errors"

	"github.com/benc-uk/kubescout/internal/health"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
)

// RelatedWarningEvents exposes bounded Warning events for an authorized resource handle.
type RelatedWarningEvents struct {
	Session *Session
}

// Name identifies the related Warning event function registered with the model.
func (t RelatedWarningEvents) Name() string { return "related_warning_events" }

// Description tells the model that event reads are bounded, credential-protected and handle-scoped.
func (t RelatedWarningEvents) Description() string {
	return "Read bounded, credential-protected Warning events for one authorised resource handle."
}

// Schema restricts event arguments to a single supplied resource handle.
func (t RelatedWarningEvents) Schema() any { return resourceRefSchema() }

// ReturnSchema leaves the result shape unspecified for the tool framework.
func (t RelatedWarningEvents) ReturnSchema() any { return nil }

// Call reads related events under the investigation's shared limits and diagnostics.
func (t RelatedWarningEvents) Call(ctx context.Context, args string) (any, error) {
	return t.Session.call(ctx, t.Name(), args, t.readEvents)
}

// readEvents projects Warning events only after complete, bounded namespace and target checks.
// Cluster-resource event reads preserve all/watch/ignore scope without namespace discovery.
func (t RelatedWarningEvents) readEvents(ctx context.Context, _ string, r health.Resource, result map[string]any) error {
	s := t.Session
	namespaces := []string{r.Namespace}

	selector := "type=Warning,involvedObject.uid=" + r.UID

	clusterScoped := health.ClusterScoped(r.Kind)
	if definition, ok := s.Scope.ExtraResource(r.APIGroup, r.Kind); ok {
		clusterScoped = definition.Scope == "Cluster"
	}

	if clusterScoped {
		namespaces = s.Scope.NamespaceWatch
		if len(namespaces) == 0 {
			namespaces = []string{metav1.NamespaceAll}
		}

		selector = "type=Warning,involvedObject.kind=" + r.Kind + ",involvedObject.name=" + r.Name
	}

	if len(namespaces) == 0 {
		return errors.New("event namespace scope unavailable")
	}

	if len(namespaces) > 20 {
		return &LimitError{Kind: "local_tool_event_namespace_limit", Observed: len(namespaces), Limit: 20}
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
				return &LimitError{Kind: "local_tool_event_request_limit", Observed: pages, Limit: 20}
			}

			page, err := s.Client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "events"}).Namespace(namespace).List(ctx, opts)
			if err != nil {
				return &UnavailableError{Operation: "related event read"}
			}

			if page.GetResourceVersion() == "" || version != "" && version != page.GetResourceVersion() {
				return errors.New("related event list lacks a consistent resource version")
			}

			version = page.GetResourceVersion()
			for _, obj := range page.Items {
				if len(validation.IsDNS1123Label(obj.GetNamespace())) != 0 || namespace != "" && obj.GetNamespace() != namespace {
					return errors.New("event response outside namespace scope")
				}

				if !s.Scope.WatchesNamespace(obj.GetNamespace()) {
					continue
				}

				target, _, _ := unstructured.NestedString(obj.Object, "involvedObject", "uid")
				name, _, _ := unstructured.NestedString(obj.Object, "involvedObject", "name")

				kind, _, _ := unstructured.NestedString(obj.Object, "involvedObject", "kind")
				if (!clusterScoped && target != r.UID) || (clusterScoped && (name != r.Name || kind != r.Kind || target != "" && target != r.UID)) {
					return errors.New("event response outside authorised resource")
				}

				if _, extra := s.Scope.ExtraResource(r.APIGroup, r.Kind); extra {
					apiVersion, _, err := unstructured.NestedString(obj.Object, "involvedObject", "apiVersion")

					version, parseErr := schema.ParseGroupVersion(apiVersion)
					if err != nil || parseErr != nil || version.Group != r.APIGroup || name != r.Name || kind != r.Kind {
						return errors.New("event response outside authorised extra-resource API")
					}
				}

				eventType, _, _ := unstructured.NestedString(obj.Object, "type")
				if eventType != "Warning" {
					return errors.New("unexpected non-warning event")
				}

				if len(events) >= 20 {
					return &LimitError{Kind: "local_tool_event_result_limit", Observed: len(events) + 1, Limit: 20}
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
				return errors.New("repeated related event pagination token")
			}

			seen[token] = true
			opts.Continue = token
		}
	}

	result["events"] = events

	return nil
}
