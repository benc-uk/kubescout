/*
Purpose: Persist and reconcile AI-directed incident lifecycle changes.
Flow: Reviewed actions and stored revisions produce material changes through
scoped ClusterIssue writes or kubeconfig memory simulation; retries reconcile
ambiguous writes, preserve identity and leave human triage status untouched.
*/
package incident

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type Record struct {
	Name, Version, UID string
	Value              Incident
}

type Repository interface {
	ListOpen(context.Context) ([]Record, error)
	Get(context.Context, string) (*Record, error)
	Save(context.Context, Record) error
}

type Kubernetes struct {
	Client    dynamic.Interface
	Namespace string
	Observer  string
	Limit     int
	Timeout   time.Duration
}

type Memory struct {
	Records map[string]Record
	Log     io.Writer
}

type Reconciler struct {
	Repository Repository
	Observer   string
	Model      string
	Simulated  bool
}

var GVR = schema.GroupVersionResource{Group: "health.k8s-ops-agent.io", Version: "v1alpha1", Resource: "clusterissues"}

// decode checks stored schema, observer ownership, stable name and lifecycle before returning a record.
func (s Kubernetes) decode(obj *unstructured.Unstructured) (*Record, error) {
	var value Incident

	raw, ok := obj.Object["spec"].(map[string]any)
	if !ok || runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &value) != nil {
		return nil, errors.New("invalid stored incident schema")
	}

	if value.Identity.Observer != s.Observer || obj.GetLabels()["app.kubernetes.io/managed-by"] != "k8s-health" ||
		!matchesName(obj.GetName(), value.Identity) || value.Identity.Anchor.UID == "" ||
		!slices.Contains(Families, value.Identity.Family) || value.Lifecycle.Revision < 1 {
		return nil, errors.New("incident ownership or identity mismatch")
	}

	if value.Lifecycle.State != "open" && value.Lifecycle.State != "resolved" {
		return nil, errors.New("invalid incident lifecycle")
	}

	return &Record{Name: obj.GetName(), Version: obj.GetResourceVersion(), UID: string(obj.GetUID()), Value: value}, nil
}

// ListOpen reads the observer's namespaced open-issue index with bounded, consistent pagination.
// Invalid records, duplicate entries and incomplete pagination fail the entire read.
func (s Kubernetes) ListOpen(ctx context.Context) ([]Record, error) {
	out := []Record{}
	opts := metav1.ListOptions{Limit: 100, LabelSelector: "app.kubernetes.io/managed-by=k8s-health,health.k8s-ops-agent.io/observer=" + s.Observer + ",health.k8s-ops-agent.io/state=open"}
	seen := map[string]bool{}
	names := map[string]bool{}
	version := ""

	for {
		callCtx, cancel := context.WithTimeout(ctx, s.Timeout)
		page, err := s.Client.Resource(GVR).Namespace(s.Namespace).List(callCtx, opts)

		cancel()

		if err != nil {
			return nil, errors.New("cannot load incident index; check ClusterIssue CRD, namespace and RBAC")
		}

		if page.GetResourceVersion() == "" || (version != "" && version != page.GetResourceVersion()) {
			return nil, errors.New("inconsistent incident pagination")
		}

		version = page.GetResourceVersion()
		for i := range page.Items {
			r, err := s.decode(&page.Items[i])
			if err != nil {
				return nil, err
			}

			if names[r.Name] {
				return nil, errors.New("duplicate incident in paginated index")
			}

			names[r.Name] = true

			out = append(out, *r)
			if len(out) > s.Limit {
				return nil, errors.New("incident admission limit exceeded")
			}
		}

		opts.Continue = page.GetContinue()
		if opts.Continue == "" {
			return out, nil
		}

		if seen[opts.Continue] {
			return nil, errors.New("repeated incident pagination token")
		}

		seen[opts.Continue] = true
	}
}

// Get performs a timed, namespaced lookup and validates ownership; a missing issue returns nil.
func (s Kubernetes) Get(ctx context.Context, name string) (*Record, error) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	obj, err := s.Client.Resource(GVR).Namespace(s.Namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	if err != nil {
		return nil, errors.New("incident lookup failed")
	}

	return s.decode(obj)
}

// Save creates a ClusterIssue or patches agent-owned fields with UID and version preconditions.
// Existing status and foreign metadata are preserved; ambiguous failures require reconciliation.
func (s Kubernetes) Save(ctx context.Context, r Record) error {
	payload, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&r.Value)
	if err != nil {
		return errors.New("cannot encode incident")
	}

	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": APIVersion, "kind": "ClusterIssue",
		"metadata": map[string]any{"name": r.Name, "namespace": s.Namespace,
			"labels": map[string]any{"app.kubernetes.io/managed-by": "k8s-health", "health.k8s-ops-agent.io/observer": s.Observer, "health.k8s-ops-agent.io/state": r.Value.Lifecycle.State}},
		"spec": payload,
	}}

	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	if r.Version == "" {
		_, err = s.Client.Resource(GVR).Namespace(s.Namespace).Create(ctx, obj, metav1.CreateOptions{})
	} else {
		// Patch only fields owned by this agent. Delivery status and foreign metadata survive.
		patch, marshalErr := json.Marshal([]map[string]any{
			{"op": "test", "path": "/metadata/resourceVersion", "value": r.Version},
			{"op": "test", "path": "/metadata/uid", "value": r.UID},
			{"op": "replace", "path": "/spec", "value": payload},
			{"op": "add", "path": "/metadata/labels/health.k8s-ops-agent.io~1state", "value": r.Value.Lifecycle.State},
		})
		if marshalErr != nil {
			return errors.New("cannot encode incident update")
		}

		_, err = s.Client.Resource(GVR).Namespace(s.Namespace).Patch(ctx, r.Name, "application/json-patch+json", patch, metav1.PatchOptions{})
	}

	if err != nil {
		return errors.New("incident persistence failed; state will be reconciled before retry")
	}

	return nil
}

// ListOpen returns only unresolved simulated records without accessing Kubernetes.
func (s *Memory) ListOpen(context.Context) ([]Record, error) {
	out := []Record{}

	for _, r := range s.Records {
		if r.Value.Lifecycle.State == "open" {
			out = append(out, r)
		}
	}

	return out, nil
}

// Get retrieves a simulated record by name, returning nil when it has not been stored.
func (s *Memory) Get(_ context.Context, name string) (*Record, error) {
	r, ok := s.Records[name]
	if !ok {
		return nil, nil
	}

	return &r, nil
}

// Save logs a simulated create or update before storing the record; log failures prevent mutation.
func (s *Memory) Save(_ context.Context, r Record) error {
	operation := "create"
	if _, ok := s.Records[r.Name]; ok {
		operation = "update"
	}

	if _, err := fmt.Fprintf(s.Log, "INFO: would %s ClusterIssue %s state=%s revision=%d (kubeconfig simulation)\n", operation, r.Name, r.Value.Lifecycle.State, r.Value.Lifecycle.Revision); err != nil {
		return errors.New("incident simulation log failed")
	}

	if s.Records == nil {
		s.Records = map[string]Record{}
	}

	s.Records[r.Name] = r

	return nil
}

// get checks the current stable name first and tries the historical name only when absent.
func (s Reconciler) get(ctx context.Context, identity Identity) (*Record, error) {
	r, err := s.Repository.Get(ctx, Name(identity))
	if err != nil || r != nil {
		return r, err
	}

	return s.Repository.Get(ctx, legacyName(identity))
}

// Existing loads model-visible summaries for every stored family on an anchor, including resolved issues.
func (s Reconciler) Existing(ctx context.Context, anchor health.Resource) ([]Existing, error) {
	out := []Existing{}

	for _, family := range Families {
		r, err := s.get(ctx, Identity{s.Observer, anchor, family})
		if err != nil {
			return nil, err
		}

		if r != nil {
			out = append(out, Existing{r.Name, r.Value.Identity.Family, r.Value.Analysis, r.Value.Lifecycle})
		}
	}

	return out, nil
}

// Apply reconciles validated AI actions, rejecting stale evidence or changed incident revisions.
// Bounded retries reread ambiguous writes under the same name; only material changes are returned.
func (s Reconciler) Apply(ctx context.Context, in InvestigationContext, d Decision, current func() bool) ([]Change, error) {
	changes := []Change{}

	for _, a := range d.Actions {
		identity := Identity{s.Observer, in.Identities[in.Anchor], a.Family}

		name := Name(identity)
		if a.IncidentID != "" && !matchesName(a.IncidentID, identity) {
			return changes, errors.New("incident action identity mismatch")
		}

		var (
			expected *Record
			desired  Record
		)

		for attempt := 0; attempt < 3; attempt++ {
			if !current() {
				return changes, errors.New("AI conclusion superseded before persistence")
			}

			var (
				prior *Record
				err   error
			)
			if attempt == 0 {
				prior, err = s.get(ctx, identity)
			} else {
				prior, err = s.Repository.Get(ctx, name)
			}

			if err != nil {
				return changes, err
			}

			if attempt > 0 {
				if prior != nil && reflect.DeepEqual(prior.Value, desired.Value) {
					break
				}

				if !sameValue(prior, expected) {
					return changes, errors.New("incident changed during persistence reconciliation")
				}
			} else {
				expected = prior
				if prior != nil && prior.Value.Identity != identity {
					return changes, errors.New("incident identity conflict")
				}

				if prior != nil {
					if a.IncidentID != "" && a.IncidentID != prior.Name {
						return changes, errors.New("incident action identity mismatch")
					}

					name = prior.Name
				}

				if !matchesInput(prior, a, in) {
					return changes, errors.New("incident revision changed since AI review")
				}

				desired, err = next(prior, identity, a, in, s.Model, time.Now().UTC())
				if err != nil {
					return changes, err
				}

				if prior != nil && reflect.DeepEqual(prior.Value, desired.Value) {
					break
				}
			}

			if prior != nil {
				desired.Version, desired.UID = prior.Version, prior.UID
			}

			if err := s.Repository.Save(ctx, desired); err != nil {
				if attempt == 2 {
					return changes, err
				}

				select {
				case <-ctx.Done():
					return changes, ctx.Err()
				case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
				}

				continue
			}

			break
		}

		if expected == nil || !reflect.DeepEqual(expected.Value, desired.Value) {
			op := a.Operation
			if expected != nil && expected.Value.Lifecycle.State == "resolved" && desired.Value.Lifecycle.State == "open" {
				op = "reopen"
			}

			changes = append(changes, Change{op, name, desired.Value, s.Simulated})
		}
	}

	return changes, nil
}

// sameValue compares record existence, UID and incident content while ignoring resource versions.
func sameValue(a, b *Record) bool {
	if a == nil || b == nil {
		return a == b
	}

	return a.UID == b.UID && reflect.DeepEqual(a.Value, b.Value)
}

// matchesInput requires an absent record for raises or the stored revision reviewed by AI for other actions.
func matchesInput(prior *Record, a Action, in InvestigationContext) bool {
	if prior == nil {
		return a.Operation == "raise"
	}

	for _, i := range in.Incidents {
		if i.ID == prior.Name {
			return a.Operation != "raise" && i.Lifecycle.Revision == prior.Value.Lifecycle.Revision
		}
	}

	return false
}

// next builds a material lifecycle revision without persisting it, preserving semantic no-ops.
// It gates resolution on recovery eligibility and tracks recurrence when reopening a record.
func next(prior *Record, identity Identity, a Action, in InvestigationContext, model string, now time.Time) (Record, error) {
	r := Record{Name: Name(identity), Value: Incident{Identity: identity}}
	if prior != nil {
		r = *prior
	}

	state := "open"

	if a.Operation == "resolve" {
		if prior == nil || !in.RecoveryEligible {
			return r, errors.New("resolution requires an existing incident and stable evidence")
		}

		state = "resolved"
	}

	resources := []health.Resource{}
	for _, ref := range a.ResourceRefs {
		resources = append(resources, in.Identities[ref])
	}

	slices.SortFunc(resources, func(a, b health.Resource) int { return stringsCompare(a, b) })

	if prior != nil && state == prior.Value.Lifecycle.State {
		// Wording, timestamps and evidence counters alone are never meaningful updates.
		if !a.MaterialChange || (a.Severity == r.Value.Analysis.Severity && a.Impact == r.Value.Analysis.Impact &&
			a.Diagnosis == r.Value.Analysis.Diagnosis && slices.Equal(resources, r.Value.Resources)) {
			return r, nil
		}
	}

	r.Value.Resources = resources
	r.Value.Analysis = Analysis{a.Title, a.Severity, a.Impact, a.Diagnosis, a.Explanation, a.EvidenceIDs, a.Recommendations, a.Uncertainty, model, PromptVersion, rand.Text()}

	l := &r.Value.Lifecycle
	if prior == nil {
		l.RaisedAt = now
	} else if l.State == "resolved" && state == "open" {
		l.Recurrences++
	}

	l.State, l.UpdatedAt, l.Revision = state, now, l.Revision+1

	l.ResolvedAt = nil
	if state == "resolved" {
		l.ResolvedAt = &now
	}

	return r, nil
}

// stringsCompare orders encoded resource identities for stable affected-resource comparisons.
func stringsCompare(a, b health.Resource) int {
	aa, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)

	return slices.Compare(aa, bb)
}
