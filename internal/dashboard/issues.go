/*
Purpose: Read dashboard issue views and persist revision-scoped human triage.
Flow: Bounded, complete ClusterIssue reads in one storage namespace become
validated summaries and details. Dismiss/restore uses displayed-version and
UID/resourceVersion checks to patch status.triage only; it neither calls AI
nor writes incident specs, and reports upstream failures as explicit errors.
*/
package dashboard

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/benc-uk/kubescout/internal/incident"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
)

type APIError struct {
	Status  int    `json:"-"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Resource struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}

type Summary struct {
	Name      string             `json:"name"`
	Namespace string             `json:"namespace"`
	Observer  string             `json:"observer"`
	Family    string             `json:"family"`
	Anchor    Resource           `json:"anchor"`
	Resources []Resource         `json:"resources"`
	Title     string             `json:"title"`
	Severity  string             `json:"severity"`
	Impact    string             `json:"impact"`
	Lifecycle incident.Lifecycle `json:"lifecycle"`
	Dismissed bool               `json:"dismissed"`
	Version   string             `json:"version"`
}

type Detail struct {
	Summary
	Analysis incident.Analysis `json:"analysis"`
}

type Snapshot struct {
	Namespace string    `json:"namespace"`
	FetchedAt time.Time `json:"fetched_at"`
	Items     []Summary `json:"items"`
}

type DetailResponse struct {
	FetchedAt time.Time `json:"fetched_at"`
	Item      Detail    `json:"item"`
}

type Source interface {
	List(context.Context) (Snapshot, error)
	Get(context.Context, string) (DetailResponse, error)
	SetDismissed(context.Context, string, string, bool) (DetailResponse, error)
}

type Reader struct {
	client    dynamic.NamespaceableResourceInterface
	namespace string
}

const (
	MaxIssues        = 1000
	MaxResponseBytes = 8 << 20
	RequestTimeout   = 10 * time.Second
)

// Error exposes the caller-safe message without adding upstream error details.
func (e *APIError) Error() string { return e.Message }

var errResponseSize = &APIError{502, "response_too_large", "Issue data exceeds the 8 MiB response limit; narrow the stored issue set."}

// NewReader validates the storage namespace and binds the client to ClusterIssue operations.
func NewReader(client dynamic.Interface, namespace string) (*Reader, error) {
	if len(validation.IsDNS1123Label(namespace)) != 0 {
		return nil, errors.New("namespace must be a non-empty Kubernetes namespace name")
	}

	return &Reader{client: client.Resource(incident.GVR), namespace: namespace}, nil
}

// List reads all issue summaries in the configured namespace with time, count, and size bounds.
// Pagination inconsistencies or invalid records fail the whole snapshot rather than returning partial data.
func (r *Reader) List(ctx context.Context) (Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	result := Snapshot{Namespace: r.namespace, Items: []Summary{}}
	options := metav1.ListOptions{Limit: 100}
	tokens, names := map[string]bool{}, map[string]bool{}
	version := ""
	encodedSize := 0

	for {
		page, err := r.client.Namespace(r.namespace).List(ctx, options)
		if err != nil {
			return Snapshot{}, upstreamError(err, false)
		}

		if page.GetResourceVersion() == "" || (version != "" && page.GetResourceVersion() != version) {
			return Snapshot{}, invalidSnapshot("Inconsistent issue pagination; refresh to retry.")
		}

		version = page.GetResourceVersion()
		for i := range page.Items {
			if len(result.Items) == MaxIssues {
				return Snapshot{}, &APIError{502, "issue_limit", "More than 1,000 stored issues; the dashboard cannot display a complete snapshot. Archive issues explicitly outside this dashboard."}
			}

			detail, err := r.decode(&page.Items[i])
			if err != nil {
				return Snapshot{}, err
			}

			if names[detail.Name] {
				return Snapshot{}, invalidSnapshot("Duplicate issue in pagination; refresh to retry.")
			}

			encoded, err := json.Marshal(detail.Summary)
			if err != nil {
				return Snapshot{}, invalidSnapshot("Stored issue cannot be encoded.")
			}

			encodedSize += len(encoded) + 1
			if encodedSize > MaxResponseBytes {
				return Snapshot{}, errResponseSize
			}

			names[detail.Name] = true
			result.Items = append(result.Items, detail.Summary)
		}

		options.Continue = page.GetContinue()
		if options.Continue == "" {
			result.FetchedAt = time.Now().UTC()
			return result, nil
		}

		if tokens[options.Continue] {
			return Snapshot{}, invalidSnapshot("Repeated issue pagination token; refresh to retry.")
		}

		tokens[options.Continue] = true
	}
}

// Get validates an issue name, performs a bounded namespaced read, and projects its detail view.
func (r *Reader) Get(ctx context.Context, name string) (DetailResponse, error) {
	if len(validation.IsDNS1123Subdomain(name)) != 0 {
		return DetailResponse{}, &APIError{400, "invalid_name", "Invalid ClusterIssue name."}
	}

	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	obj, err := r.client.Namespace(r.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return DetailResponse{}, upstreamError(err, true)
	}

	if obj.GetName() != name {
		return DetailResponse{}, invalidSnapshot("The API returned a different issue than requested.")
	}

	item, err := r.decode(obj)
	if err != nil {
		return DetailResponse{}, err
	}

	return DetailResponse{FetchedAt: time.Now().UTC(), Item: item}, nil
}

// decode validates stored issue fields and projects a view without raw metadata or resource UIDs.
// Dismissal applies only when the stored triage revision matches the incident lifecycle revision.
func (r *Reader) decode(obj *unstructured.Unstructured) (Detail, error) {
	var value incident.Incident

	spec, ok := obj.Object["spec"].(map[string]any)
	if !ok || runtime.DefaultUnstructuredConverter.FromUnstructured(spec, &value) != nil {
		return Detail{}, invalidSnapshot("Stored issue has an invalid schema.")
	}

	if obj.GetNamespace() != r.namespace || len(validation.IsDNS1123Subdomain(obj.GetName())) != 0 ||
		value.Identity.Observer == "" || !slices.Contains(incident.Families, value.Identity.Family) ||
		value.Identity.Anchor.Kind == "" || value.Identity.Anchor.Name == "" ||
		!slices.Contains([]string{"low", "medium", "high", "critical"}, value.Analysis.Severity) ||
		!slices.Contains([]string{"limited", "degraded", "unavailable"}, value.Analysis.Impact) ||
		!slices.Contains([]string{"open", "resolved"}, value.Lifecycle.State) ||
		value.Lifecycle.Revision < 1 || value.Lifecycle.Recurrences < 0 ||
		value.Lifecycle.RaisedAt.IsZero() || value.Lifecycle.UpdatedAt.IsZero() ||
		len(value.Resources) == 0 || len(value.Resources) > 50 {
		return Detail{}, invalidSnapshot("Stored issue has invalid identity, analysis or lifecycle fields.")
	}

	for _, text := range []string{value.Analysis.Title, value.Analysis.Diagnosis, value.Analysis.Explanation,
		value.Analysis.Uncertainty, value.Analysis.Model, value.Analysis.PromptVersion, value.Analysis.AnalysisID} {
		if strings.TrimSpace(text) == "" || len(text) > 16000 {
			return Detail{}, invalidSnapshot("Stored issue has invalid analysis text.")
		}
	}

	for _, list := range []struct {
		values []string
		limit  int
	}{{value.Analysis.EvidenceIDs, 50}, {value.Analysis.Recommendations, 10}} {
		if list.values == nil || len(list.values) > list.limit {
			return Detail{}, invalidSnapshot("Stored issue has invalid analysis references.")
		}

		for _, text := range list.values {
			if strings.TrimSpace(text) == "" || len(text) > 16000 {
				return Detail{}, invalidSnapshot("Stored issue has invalid analysis references.")
			}
		}
	}

	refs := make([]Resource, 0, len(value.Resources))
	for _, ref := range value.Resources {
		if ref.Kind == "" || ref.Name == "" {
			return Detail{}, invalidSnapshot("Stored issue has an invalid resource reference.")
		}

		refs = append(refs, Resource{ref.Kind, ref.Namespace, ref.Name})
	}

	anchor := value.Identity.Anchor

	dismissedRevision, _, err := unstructured.NestedInt64(obj.Object, "status", "triage", "dismissedRevision")
	if err != nil || dismissedRevision < 0 {
		return Detail{}, invalidSnapshot("Stored issue has invalid triage status.")
	}

	return Detail{
		Summary: Summary{
			Name: obj.GetName(), Namespace: r.namespace, Observer: value.Identity.Observer,
			Family: value.Identity.Family, Anchor: Resource{anchor.Kind, anchor.Namespace, anchor.Name},
			Resources: refs, Title: value.Analysis.Title, Severity: value.Analysis.Severity,
			Impact: value.Analysis.Impact, Lifecycle: value.Lifecycle,
			Dismissed: dismissedRevision == value.Lifecycle.Revision, Version: issueVersion(obj),
		},
		Analysis: value.Analysis,
	}, nil
}

// issueVersion hashes the object UID and resource version into an opaque browser concurrency token.
func issueVersion(obj *unstructured.Unstructured) string {
	sum := sha256.Sum256([]byte(string(obj.GetUID()) + "\x00" + obj.GetResourceVersion()))
	return hex.EncodeToString(sum[:])
}

// SetDismissed verifies the displayed version and patches only triage status with UID/version preconditions.
// It skips unchanged actions, checks the returned dismissal, and never writes incident specs.
func (r *Reader) SetDismissed(ctx context.Context, name, version string, dismissed bool) (DetailResponse, error) {
	if len(validation.IsDNS1123Subdomain(name)) != 0 {
		return DetailResponse{}, &APIError{400, "invalid_name", "Invalid ClusterIssue name."}
	}

	if decoded, err := hex.DecodeString(version); err != nil || len(decoded) != sha256.Size {
		return DetailResponse{}, &APIError{400, "invalid_version", "A valid issue version is required."}
	}

	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	obj, err := r.client.Namespace(r.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return DetailResponse{}, upstreamError(err, true)
	}

	if obj.GetName() != name || obj.GetUID() == "" || obj.GetResourceVersion() == "" {
		return DetailResponse{}, invalidSnapshot("Cannot verify the issue identity for triage.")
	}

	item, err := r.decode(obj)
	if err != nil {
		return DetailResponse{}, err
	}

	if item.Version != version {
		return DetailResponse{}, triageConflict()
	}

	if item.Dismissed == dismissed {
		return DetailResponse{FetchedAt: time.Now().UTC(), Item: item}, nil
	}

	revision := int64(0)
	if dismissed {
		revision = item.Lifecycle.Revision
	}

	patch := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": string(obj.GetUID())},
		{"op": "test", "path": "/metadata/resourceVersion", "value": obj.GetResourceVersion()},
	}
	if obj.Object["status"] == nil {
		patch = append(patch, map[string]any{"op": "add", "path": "/status", "value": map[string]any{}})
	}

	patch = append(patch, map[string]any{
		"op": "add", "path": "/status/triage", "value": map[string]any{"dismissedRevision": revision},
	})

	body, err := json.Marshal(patch)
	if err != nil {
		return DetailResponse{}, errors.New("cannot encode triage change")
	}

	updated, err := r.client.Namespace(r.namespace).Patch(ctx, name, types.JSONPatchType, body, metav1.PatchOptions{}, "status")
	if err != nil {
		switch {
		case apierrors.IsConflict(err), apierrors.IsInvalid(err):
			return DetailResponse{}, triageConflict()
		case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
			return DetailResponse{}, &APIError{403, "triage_denied", "Cannot change triage status; check patch permission on ClusterIssues/status in the dashboard namespace."}
		case apierrors.IsNotFound(err):
			return DetailResponse{}, upstreamError(err, true)
		default:
			return DetailResponse{}, &APIError{503, "triage_uncertain", "The triage change could not be confirmed and may have applied. Refresh before retrying."}
		}
	}

	item, err = r.decode(updated)
	if err != nil {
		return DetailResponse{}, err
	}

	if updated.GetName() != name || updated.GetUID() != obj.GetUID() || item.Dismissed != dismissed {
		return DetailResponse{}, invalidSnapshot("The triage change was not retained as expected; check the CRD and refresh.")
	}

	return DetailResponse{FetchedAt: time.Now().UTC(), Item: item}, nil
}

// triageConflict tells the browser to refresh before acting on a changed issue.
func triageConflict() *APIError {
	return &APIError{409, "triage_conflict", "This issue changed. Refresh and review it before changing its triage status."}
}

// invalidSnapshot classifies invalid or inconsistent upstream issue data as a failed read.
func invalidSnapshot(message string) *APIError {
	return &APIError{502, "invalid_snapshot", message}
}

// upstreamError maps Kubernetes read failures to safe API errors without exposing upstream bodies.
// Missing individual issues and missing list resources receive distinct responses.
func upstreamError(err error, detail bool) error {
	switch {
	case errors.Is(err, errResponseSize):
		return errResponseSize
	case errors.Is(err, context.DeadlineExceeded), apierrors.IsTimeout(err), apierrors.IsServerTimeout(err):
		return &APIError{504, "timeout", "Kubernetes request timed out; refresh to retry."}
	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		return &APIError{503, "access_denied", "Cannot read ClusterIssues; check the dashboard ServiceAccount and namespace RoleBinding."}
	case apierrors.IsNotFound(err):
		if detail {
			return &APIError{404, "not_found", "Issue is no longer available; refresh the list."}
		}

		return &APIError{503, "unavailable", "ClusterIssues are unavailable; check the CRD and configured namespace."}
	default:
		return &APIError{503, "unavailable", "Cannot read ClusterIssues from Kubernetes; check connectivity and configuration."}
	}
}
