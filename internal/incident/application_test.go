/*
Purpose: Verify resumable AI-approved writes and strict Incident HTTP validation.
Flow: Offline memory/HTTP fixtures retain exact revisions across failures, reconcile
lost acknowledgements, avoid replaying partial actions and reject replaced UIDs or stale
evidence. Unknown fields and RBAC failures remain explicit without raw API bodies.
*/
package incident

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

type applicationRepository struct {
	Memory
	failFamily, lostResponseFamily string
	lookupFailed                   bool
	writes                         []Record
	onGet                          func()
}

// Get simulates a failed reread after a committed-but-unacknowledged write.
func (r *applicationRepository) Get(ctx context.Context, name string) (*Record, error) {
	if r.lookupFailed {
		return nil, errors.New("fixture lookup unavailable")
	}

	if r.onGet != nil {
		r.onGet()
	}

	return r.Memory.Get(ctx, name)
}

// Save captures exact desired values and optionally fails before or after a fixture commit.
func (r *applicationRepository) Save(ctx context.Context, record Record) error {
	r.writes = append(r.writes, record)
	if record.Value.Identity.Family == r.lostResponseFamily {
		if err := r.Memory.Save(ctx, record); err != nil {
			return err
		}

		r.lookupFailed = true

		return errors.New("fixture response lost")
	}

	if record.Value.Identity.Family == r.failFamily {
		return errors.New("fixture write unavailable")
	}

	return r.Memory.Save(ctx, record)
}

// TestApplicationResumesExactRevision preserves timestamps, analysis IDs and names across delayed retries.
func TestApplicationResumesExactRevision(t *testing.T) {
	in, decision := fixture()
	repo := &applicationRepository{failFamily: decision.Actions[0].Family}
	reconciler := Reconciler{Repository: repo, Observer: "fixture", Model: "fixture"}

	application, err := reconciler.NewApplication(in, decision)
	if err != nil {
		t.Fatal(err)
	}

	changes, err := application.Apply(t.Context(), func() bool { return true })

	var persistence *PersistenceError

	if !errors.As(err, &persistence) || len(changes) != 0 || len(repo.writes) != 3 || len(repo.Records) != 0 {
		t.Fatalf("failed writes changed incident state: %v", err)
	}

	desired := repo.writes[0]
	repo.failFamily = ""

	changes, err = application.Apply(t.Context(), func() bool { return true })
	if err != nil || len(changes) != 1 || len(repo.Records) != 1 || len(repo.writes) != 4 {
		t.Fatalf("resumed write: %v", err)
	}

	for _, write := range repo.writes {
		if write.Name != desired.Name || !reflect.DeepEqual(write.Value, desired.Value) {
			t.Fatal("retry regenerated the identity or AI-approved revision")
		}
	}

	changes, err = application.Apply(t.Context(), func() bool { return true })
	if err != nil || len(changes) != 0 || len(repo.writes) != 4 {
		t.Fatal("completed application replayed its action")
	}
}

// TestApplicationResumesAmbiguousCommit confirms a delayed reread without creating or writing a second record.
func TestApplicationResumesAmbiguousCommit(t *testing.T) {
	in, decision := fixture()
	repo := &applicationRepository{lostResponseFamily: decision.Actions[0].Family}

	application, err := (Reconciler{Repository: repo, Observer: "fixture"}).NewApplication(in, decision)
	if err != nil {
		t.Fatal(err)
	}

	changes, err := application.Apply(t.Context(), func() bool { return true })

	var persistence *PersistenceError

	if !errors.As(err, &persistence) || len(changes) != 0 || len(repo.writes) != 1 || len(repo.Records) != 1 {
		t.Fatalf("ambiguous fixture: %v", err)
	}

	repo.lookupFailed = false

	changes, err = application.Apply(t.Context(), func() bool { return true })
	if err != nil || len(changes) != 1 || len(repo.writes) != 1 || len(repo.Records) != 1 {
		t.Fatal("delayed acknowledgement repeated the write or lost its confirmed change")
	}
}

// TestApplicationPartialProgress returns confirmed actions once and resumes only unfinished families.
func TestApplicationPartialProgress(t *testing.T) {
	in, decision := fixture()
	second := decision.Actions[0]
	second.Family = "scheduling"
	decision.Actions = append(decision.Actions, second)
	repo := &applicationRepository{failFamily: second.Family}

	application, err := (Reconciler{Repository: repo, Observer: "fixture"}).NewApplication(in, decision)
	if err != nil {
		t.Fatal(err)
	}

	changes, err := application.Apply(t.Context(), func() bool { return true })
	if err == nil || len(changes) != 1 || len(repo.Records) != 1 {
		t.Fatal("partial write was not reported")
	}

	firstName := changes[0].Name
	first := repo.Records[firstName]
	repo.failFamily = ""

	changes, err = application.Apply(t.Context(), func() bool { return true })
	if err != nil || len(changes) != 1 || changes[0].Name == firstName || len(repo.Records) != 2 ||
		!reflect.DeepEqual(repo.Records[firstName], first) {
		t.Fatal("resume replayed or changed a completed family")
	}
}

// TestApplicationRejectsStaleOrReplacedState prevents delayed writes after evidence changes or an Incident UID replacement.
func TestApplicationRejectsStaleOrReplacedState(t *testing.T) {
	in, decision := fixture()
	repo := &applicationRepository{}
	reconciler := Reconciler{Repository: repo, Observer: "fixture"}

	changes, err := reconciler.Apply(t.Context(), in, decision, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}

	name := changes[0].Name

	in.Incidents, err = reconciler.Existing(t.Context(), in.Identities[in.Anchor])
	if err != nil {
		t.Fatal(err)
	}

	decision.Actions[0].Operation, decision.Actions[0].IncidentID = "update", name
	decision.Actions[0].Severity = "high"
	repo.failFamily = decision.Actions[0].Family

	application, err := reconciler.NewApplication(in, decision)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := application.Apply(t.Context(), func() bool { return true }); err == nil {
		t.Fatal("expected write failure")
	}

	count := len(repo.writes)
	if _, err := application.Apply(t.Context(), func() bool { return false }); err == nil || len(repo.writes) != count {
		t.Fatal("stale evidence was persisted")
	}

	replaced := repo.Records[name]
	replaced.UID, replaced.Value = "replacement-incident", application.pending.desired.Value
	repo.Records[name] = replaced
	repo.failFamily = ""
	_, err = application.Apply(t.Context(), func() bool { return true })

	var persistence *PersistenceError

	if err == nil || errors.As(err, &persistence) || len(repo.writes) != count || repo.Records[name].UID != replaced.UID {
		t.Fatal("replacement with matching content was accepted as the original write")
	}
}

// TestStrictIncidentWriteErrors checks strict create/patch options and safe schema/RBAC diagnostics against local HTTP fixtures.
func TestStrictIncidentWriteErrors(t *testing.T) {
	in, decision := fixture()
	identity := Identity{Observer: "fixture", Anchor: in.Identities[in.Anchor], Family: decision.Actions[0].Family}

	record, err := next(nil, identity, decision.Actions[0], in, "fixture", time.Now())
	if err != nil {
		t.Fatal(err)
	}

	for _, patch := range []bool{false, true} {
		for _, status := range []int{400, 403, 422, 500} {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				method := http.MethodPost
				if patch {
					method = http.MethodPatch
				}

				if r.Method != method || r.URL.Query().Get("fieldValidation") != "Strict" {
					t.Error("Incident write did not require strict API field validation")
				}

				reason := map[int]string{400: "BadRequest", 403: "Forbidden", 422: "Invalid", 500: "InternalError"}[status]

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "Status",
					"reason": reason, "code": status, "message": "private-upstream-body"})
			}))

			client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}

			record.Version, record.UID = "", ""
			if patch {
				record.Version, record.UID = "1", "incident-uid"
			}

			err = (Kubernetes{Client: client, Observer: "fixture", Timeout: time.Second}).Save(t.Context(), record)

			server.Close()

			if err == nil || strings.Contains(err.Error(), "private-upstream-body") ||
				(status == 400 || status == 422) && !strings.Contains(err.Error(), "make incidents-setup") ||
				status == 403 && !strings.Contains(err.Error(), "RBAC") {
				t.Fatalf("unsafe or unhelpful error for status=%d: %v", status, err)
			}
		}
	}
}

// TestApplicationRechecksAfterLookup rejects evidence changed during a slow repository read, before any write.
func TestApplicationRechecksAfterLookup(t *testing.T) {
	in, decision := fixture()
	current := true
	repo := &applicationRepository{onGet: func() { current = false }}

	application, err := (Reconciler{Repository: repo, Observer: "fixture"}).NewApplication(in, decision)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := application.Apply(t.Context(), func() bool { return current }); err == nil || len(repo.writes) != 0 {
		t.Fatal("evidence changed during lookup but the old proposal was written")
	}
}

// TestPersistedAnchorPresenceFingerprint distinguishes resource changes from adding an approved record to the index.
func TestPersistedAnchorPresenceFingerprint(t *testing.T) {
	in, _ := fixture()
	o := health.Object{Resource: in.Identities[in.Anchor], Phase: "Failed"}
	snapshot := health.Snapshot{At: time.Now(), Objects: []health.Object{o}}

	before, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 100)
	if err != nil {
		t.Fatal(err)
	}

	record := Record{Value: Incident{Identity: Identity{Observer: "fixture", Anchor: o.Resource, Family: "container-failure"},
		Resources: []health.Resource{o.Resource}, Lifecycle: Lifecycle{State: "open", Revision: 1}}}
	after, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), []Record{record}, 100)

	key := Ref(o.Resource)
	if err != nil || before[key].Revision != after[key].Revision {
		t.Fatal("adding the approved Incident invalidated unchanged resource evidence")
	}

	snapshot.Objects[0].Phase = "Succeeded"

	changed, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), []Record{record}, 100)
	if err != nil || changed[key].Revision == before[key].Revision {
		t.Fatal("presence normalization concealed a real resource change")
	}
}
