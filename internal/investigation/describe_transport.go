/*
Purpose: Confine kubectl's independent describe reads to approved investigation scope.
Flow: A private REST transport enforces GET-only APIs, authorized incarnations,
namespace filters, complete bounded pagination, byte/request caps and the parent
deadline. Only scoped responses reach the library; upstream errors never enter its text.
Native rendering retains operational sections and HPA metric selectors.
Missing list-item kinds come from approved endpoints; identity and version checks stay strict.
Named reads prefer the describer's exact root or a supplied current incarnation, never a random historical handle.
*/
package investigation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/benc-uk/kubescout/internal/health"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
)

type describeBudget struct {
	requests int
	bytes    int
}

type describeTransport struct {
	next              http.RoundTripper
	ctx               context.Context
	session           *Session
	root              health.Resource
	budget            *describeBudget
	host              string
	basePath          string
	failure           error
	partial           bool
	eventUnavailable  bool
	relatedIncomplete bool
	rootAbsent        bool
	privateIDs        map[string]bool
}

type describePath struct {
	gvr       schema.GroupVersionResource
	kind      string
	namespace string
	name      string
}

const maxDescribeRequests = 16
const maxDescribeResponseBytes = 128 << 10
const maxDescribeReadBytes = 1 << 20

// WrappedRoundTripper lets client-go reach the underlying cancellation-capable transport.
func (t *describeTransport) WrappedRoundTripper() http.RoundTripper { return t.next }

// RoundTrip admits only scoped kubectl reads and replaces upstream errors with fixed safe statuses.
func (t *describeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	path, ok := t.parse(req)
	if !ok {
		t.partial = true
		t.relatedIncomplete = true

		return describeResponse(req, http.StatusForbidden, "Describe read outside approved scope"), nil
	}

	if path.name != "" {
		resource, ref, ok := t.identity(path, path.name, path.namespace)
		if !ok {
			t.partial = true
			t.relatedIncomplete = true

			return describeResponse(req, http.StatusForbidden, "Describe resource outside supplied handles"), nil
		}

		obj, status, err := t.read(req)
		if err != nil {
			t.partial = true
			t.relatedIncomplete = true

			return describeResponse(req, status, "Describe resource read unavailable"), nil
		}

		if status == http.StatusNotFound {
			if version, read := t.session.Investigation.ToolVersions[ref]; read && version != "" {
				t.failure = errors.New("resource disappeared during supplemental describe")
			} else if t.session.Investigation.ToolVersions != nil {
				t.session.Investigation.ToolVersions[ref] = ""
			}

			t.partial = true
			t.rootAbsent = resource == t.root

			return describeResponse(req, status, "Resource not found during describe"), nil
		}

		if err := t.checkObject(obj, resource, ref); err != nil {
			t.failure = err
			return describeResponse(req, http.StatusConflict, "Describe resource incarnation changed"), nil
		}

		return t.response(req, obj)
	}

	return t.list(req, path)
}

// parse recognizes only supported read endpoints, never discovery, subresources or arbitrary URLs.
func (t *describeTransport) parse(req *http.Request) (describePath, bool) {
	if req.Method != http.MethodGet || req.URL.Host != t.host || !strings.HasPrefix(req.URL.Path, t.basePath) {
		return describePath{}, false
	}

	for key := range req.URL.Query() {
		if !slices.Contains([]string{"fieldSelector", "labelSelector", "limit", "continue", "resourceVersion", "timeout"}, key) {
			return describePath{}, false
		}
	}

	path := strings.TrimPrefix(req.URL.Path, t.basePath)
	for kind, gvr := range resources {
		if result, ok := matchDescribePath(path, kind, gvr); ok {
			if result.namespace != "" && !t.session.Scope.WatchesNamespace(result.namespace) {
				return describePath{}, false
			}

			return result, true
		}
	}

	return matchDescribePath(path, "Event", schema.GroupVersionResource{Version: "v1", Resource: "events"})
}

// matchDescribePath matches one known GVR and optional namespace/name without subresources.
func matchDescribePath(path, kind string, gvr schema.GroupVersionResource) (describePath, bool) {
	prefix := "/api/" + gvr.Version + "/"
	if gvr.Group != "" {
		prefix = "/apis/" + gvr.Group + "/" + gvr.Version + "/"
	}

	if !strings.HasPrefix(path, prefix) {
		return describePath{}, false
	}

	parts := strings.Split(strings.TrimPrefix(path, prefix), "/")

	result := describePath{gvr: gvr, kind: kind}
	if len(parts) >= 3 && parts[0] == "namespaces" {
		result.namespace = parts[1]
		parts = parts[2:]
	}

	if len(parts) < 1 || len(parts) > 2 || parts[0] != gvr.Resource ||
		health.ClusterScoped(kind) && result.namespace != "" {
		return describePath{}, false
	}

	if len(parts) == 2 {
		result.name = parts[1]
		if len(validation.IsDNS1123Subdomain(result.name)) != 0 || kind == "Event" ||
			!health.ClusterScoped(kind) && result.namespace == "" {
			return describePath{}, false
		}
	}

	if result.namespace != "" && len(validation.IsDNS1123Label(result.namespace)) != 0 {
		return describePath{}, false
	}

	return result, true
}

// identity resolves names through the exact root or a supplied current incarnation before historical handles.
func (t *describeTransport) identity(path describePath, name, namespace string) (health.Resource, string, bool) {
	for ref, r := range t.session.Investigation.Identities {
		if r == t.root && r.Kind == path.kind && r.Name == name && r.Namespace == namespace {
			return r, ref, true
		}
	}

	for ref, r := range t.session.Investigation.Identities {
		if r.Kind == path.kind && r.Name == name && r.Namespace == namespace {
			if _, current := t.session.Investigation.Objects[ref]; current {
				return r, ref, true
			}
		}
	}

	for ref, r := range t.session.Investigation.Identities {
		if r.Kind == path.kind && r.Name == name && r.Namespace == namespace {
			return r, ref, true
		}
	}

	return health.Resource{}, "", false
}

// read counts every HTTP attempt, enforces response limits and hides raw API error bodies.
func (t *describeTransport) read(req *http.Request) (*unstructured.Unstructured, int, error) {
	if t.ctx.Err() != nil {
		return nil, http.StatusGatewayTimeout, t.ctx.Err()
	}

	if t.budget.requests >= maxDescribeRequests {
		return nil, http.StatusServiceUnavailable, errors.New("describe request limit reached")
	}

	if t.budget.bytes >= maxDescribeReadBytes {
		return nil, http.StatusServiceUnavailable, errors.New("describe cumulative response limit reached")
	}

	t.budget.requests++

	res, err := t.next.RoundTrip(req.Clone(t.ctx))
	if err != nil {
		return nil, http.StatusServiceUnavailable, errors.New("describe HTTP read failed")
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil, res.StatusCode, nil
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, http.StatusServiceUnavailable, errors.New("describe API read denied or unavailable")
	}

	limit := min(maxDescribeResponseBytes, maxDescribeReadBytes-t.budget.bytes)
	data, err := io.ReadAll(io.LimitReader(res.Body, int64(limit)+1))

	t.budget.bytes += len(data)
	if err != nil || len(data) > limit {
		return nil, http.StatusServiceUnavailable, errors.New("describe response exceeds read budget")
	}

	obj := &unstructured.Unstructured{}
	if err := json.Unmarshal(data, &obj.Object); err != nil {
		t.failure = errors.New("invalid supplemental describe response")
		return nil, http.StatusBadGateway, t.failure
	}

	return obj, res.StatusCode, nil
}

// checkObject rejects mismatched or changed incarnations and extends host freshness tracking.
func (t *describeTransport) checkObject(obj *unstructured.Unstructured, r health.Resource, ref string) error {
	if obj == nil || obj.GetKind() != r.Kind || obj.GetName() != r.Name || obj.GetNamespace() != r.Namespace ||
		string(obj.GetUID()) != r.UID || obj.GetResourceVersion() == "" {
		return errors.New("resource incarnation changed during supplemental describe")
	}

	if version, read := t.session.Investigation.ToolVersions[ref]; read && version != obj.GetResourceVersion() {
		return errors.New("resource changed between investigation and supplemental describe")
	}

	if t.session.Investigation.ToolVersions != nil {
		t.session.Investigation.ToolVersions[ref] = obj.GetResourceVersion()
	}

	return nil
}

// list completes bounded namespace-aware pagination before supplying kubectl with authorized objects.
func (t *describeTransport) list(req *http.Request, path describePath) (*http.Response, error) {
	namespaces := []string{path.namespace}
	if path.namespace == "" && !health.ClusterScoped(path.kind) && len(t.session.Scope.NamespaceWatch) != 0 {
		namespaces = t.session.Scope.NamespaceWatch
	}

	if path.namespace != "" && !t.session.Scope.WatchesNamespace(path.namespace) {
		t.partial = true
		return describeResponse(req, http.StatusForbidden, "Describe namespace outside scope"), nil
	}

	items := []any{}
	for _, namespace := range namespaces {
		err := t.listNamespace(req, path, namespace, &items)
		if err != nil {
			t.partial = true
			if path.kind == "Event" {
				t.eventUnavailable = true
			} else {
				t.relatedIncomplete = true
			}

			return describeResponse(req, http.StatusServiceUnavailable, "Describe related list unavailable or incomplete"), nil
		}
	}

	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": path.gvr.GroupVersion().String(), "kind": path.kind + "List",
		"metadata": map[string]any{}, "items": items,
	}}

	return t.response(req, obj)
}

// listNamespace validates pages, supplies omitted item kinds from approved endpoints,
// and filters ignored or unsupplied resources before library access.
func (t *describeTransport) listNamespace(req *http.Request, path describePath, namespace string, items *[]any) error {
	query := req.URL.Query()
	query.Set("limit", "20")
	query.Del("continue")

	if path.kind == "Event" {
		selector, err := fields.ParseSelector(query.Get("fieldSelector"))
		if err != nil {
			return errors.New("invalid describe event selector")
		}

		if uid, specified := selector.RequiresExactMatch("involvedObject.uid"); specified && uid != t.root.UID {
			return errors.New("describe event lookup requested a different incarnation")
		}

		query.Set("fieldSelector", "type=Warning,involvedObject.uid="+t.root.UID)
	}

	seen := map[string]bool{}
	version := ""

	for {
		call := req.Clone(t.ctx)
		u := *req.URL
		u.Path = t.resourcePath(path, namespace)
		u.RawQuery = query.Encode()
		call.URL = &u

		page, status, err := t.read(call)
		if err != nil || status != http.StatusOK {
			return errors.New("describe list read failed")
		}

		if page.GetResourceVersion() == "" || version != "" && version != page.GetResourceVersion() {
			t.failure = errors.New("inconsistent supplemental describe pagination")
			return t.failure
		}

		version = page.GetResourceVersion()

		objects, found, err := unstructured.NestedSlice(page.Object, "items")
		if err != nil || !found {
			t.failure = errors.New("invalid supplemental describe list")
			return t.failure
		}

		for _, raw := range objects {
			object, ok := raw.(map[string]any)
			if !ok {
				t.failure = errors.New("invalid supplemental describe list item")
				return t.failure
			}

			obj := &unstructured.Unstructured{Object: object}
			if namespace != "" && obj.GetNamespace() != namespace ||
				!health.ClusterScoped(path.kind) && len(validation.IsDNS1123Label(obj.GetNamespace())) != 0 {
				t.failure = errors.New("supplemental describe response escaped namespace scope")
				return t.failure
			}

			if !health.ClusterScoped(path.kind) && !t.session.Scope.WatchesNamespace(obj.GetNamespace()) {
				t.partial = true
				t.relatedIncomplete = true

				continue
			}

			if path.kind == "Event" {
				target, _, _ := unstructured.NestedString(object, "involvedObject", "uid")

				eventType, _, _ := unstructured.NestedString(object, "type")
				if target != t.root.UID || eventType != "Warning" {
					t.failure = errors.New("supplemental describe event escaped resource scope")
					return t.failure
				}
			} else {
				r, ref, authorized := t.identity(path, obj.GetName(), obj.GetNamespace())
				if !authorized {
					t.partial = true
					t.relatedIncomplete = true

					continue
				}

				if _, present := object["kind"]; !present {
					obj.SetKind(path.kind)
				}

				if err := t.checkObject(obj, r, ref); err != nil {
					t.failure = err
					return err
				}
			}

			if len(*items) >= 50 || path.kind == "Event" && len(*items) >= 20 {
				return errors.New("supplemental describe item limit reached")
			}

			*items = append(*items, object)
		}

		token := page.GetContinue()
		if token == "" {
			return nil
		}

		if seen[token] {
			t.failure = errors.New("repeated supplemental describe pagination token")
			return t.failure
		}

		seen[token] = true
		query.Set("continue", token)
	}
}

// resourcePath builds only the already-recognized resource endpoint in a selected namespace.
func (t *describeTransport) resourcePath(path describePath, namespace string) string {
	prefix := "/api/" + path.gvr.Version
	if path.gvr.Group != "" {
		prefix = "/apis/" + path.gvr.Group + "/" + path.gvr.Version
	}

	if namespace != "" {
		prefix += "/namespaces/" + url.PathEscape(namespace)
	}

	return t.basePath + prefix + "/" + path.gvr.Resource
}

// response prepares scoped objects for native rendering without filtering operational sections.
func (t *describeTransport) response(req *http.Request, obj *unstructured.Unstructured) (*http.Response, error) {
	protectDescribeValues(obj.Object, t.privateIDs)

	data, err := json.Marshal(obj.Object)
	if err != nil {
		t.failure = errors.New("supplemental describe response serialization failed")
		return nil, t.failure
	}

	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(bytes.NewReader(data)), Request: req}, nil
}

// describeResponse returns fixed safe API errors without response bodies, credentials or retry hints.
func describeResponse(req *http.Request, code int, message string) *http.Response {
	reason := metav1.StatusReasonServiceUnavailable

	switch code {
	case http.StatusForbidden:
		reason = metav1.StatusReasonForbidden
	case http.StatusNotFound:
		reason = metav1.StatusReasonNotFound
	case http.StatusConflict:
		reason = metav1.StatusReasonConflict
	}

	status := metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
		Status: "Failure", Code: int32(code), Reason: reason, Message: message}
	data, _ := json.Marshal(status)

	return &http.Response{StatusCode: code, Status: fmt.Sprint(code), Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(bytes.NewReader(data)), Request: req}
}
