/*
Purpose: Test attachment incident scope, failure retention and recovery gates offline.
Flow: Local collection denials and in-memory AI fixtures retain unresolved
storage incidents on no-action, model failure or unknown collection. Attached
state permits recovery review, not automatic resolution or proof of mounting;
changed inspected resources and renewed errors reject stale conclusions.
*/
package runner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/kube"
	"github.com/benc-uk/kubescout/internal/output"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// attachmentForRunner supplies stable cluster-scoped error evidence for offline lifecycle tests.
func attachmentForRunner() health.Object {
	return health.Object{Resource: health.Resource{Kind: "VolumeAttachment", Name: "disk-node", UID: "attachment"}, Version: "10",
		Attachment: health.AttachmentState{Attacher: "disk.csi.example", NodeName: "node", VolumeName: "disk",
			AttachError: health.AttachmentError{Present: true}}}
}

// TestAttachmentNoActionAndFailuresRetainIncidents preserves unresolved attachment state unless validated AI changes it.
func TestAttachmentNoActionAndFailuresRetainIncidents(t *testing.T) {
	for _, tt := range []struct {
		name              string
		collection, model bool
		code              int
	}{
		{name: "no-action"},
		{name: "unknown collection", collection: true, code: 1},
		{name: "model failure", model: true, code: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			attachment := attachmentForRunner()

			source := &fakeSource{objects: []health.Object{attachment}}
			if tt.collection {
				source.err = errors.New("attachment collection unknown")
			}

			repo := &incident.Memory{Records: map[string]incident.Record{"existing": {Value: incident.Incident{
				Identity:  incident.Identity{Anchor: attachment.Resource, Family: "storage-availability"},
				Resources: []health.Resource{attachment.Resource}, Lifecycle: incident.Lifecycle{State: "open"}}}}}
			ai := &fakeAI{fail: tt.model}

			var out, diag bytes.Buffer

			r := Runner{Config: options(), Source: source, Analyzer: ai, Incidents: incident.Reconciler{Repository: repo},
				Output: &output.Writer{Out: &out, Err: &diag}}
			if r.Once(t.Context()) != tt.code || len(repo.Records) != 1 || repo.Records["existing"].Value.Lifecycle.State != "open" ||
				tt.collection && ai.calls.Load() != 0 || !tt.collection && ai.calls.Load() != 1 {
				t.Fatal("attachment no-action/failure changed a stored incident or bypassed AI")
			}
		})
	}
}

// TestAttachmentStoredScope accepts globally observed attachments and rejects malformed namespaced records.
func TestAttachmentStoredScope(t *testing.T) {
	for _, namespace := range []string{"", "apps", "other"} {
		resource := attachmentForRunner().Resource
		resource.Namespace = namespace
		repo := &incident.Memory{Records: map[string]incident.Record{"existing": {Value: incident.Incident{
			Identity: incident.Identity{Anchor: resource, Family: "storage-availability"}, Lifecycle: incident.Lifecycle{State: "open"}}}}}
		r := Runner{Config: options(), Incidents: incident.Reconciler{Repository: repo}}

		if _, err := r.loadOpen(t.Context()); (err != nil) != (namespace != "") {
			t.Fatalf("unexpected stored attachment scope: %v", err)
		}
	}
}

// TestAttachmentDeniedCollectionSkipsAI retains existing storage incidents when the new required cluster-scoped read is denied.
func TestAttachmentDeniedCollectionSkipsAI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Error("collection outage caused a Kubernetes write")
		}

		w.Header().Set("Content-Type", "application/json")

		if strings.HasSuffix(r.URL.Path, "/volumeattachments") {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"Status","reason":"Forbidden","code":403,"message":"private-attachment-error"}`)

			return
		}

		items := []any{}
		if strings.HasSuffix(r.URL.Path, "/nodes") {
			items = append(items, map[string]any{"apiVersion": "v1", "kind": "Node", "metadata": map[string]any{"name": "node", "uid": "node", "resourceVersion": "1"}})
		}

		json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{"resourceVersion": "1"}, "items": items})
	}))
	defer server.Close()

	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}

	cfg := options()
	cfg.RequestTimeout = time.Second
	resource := attachmentForRunner().Resource
	repo := &incident.Memory{Records: map[string]incident.Record{"existing": {Value: incident.Incident{
		Identity: incident.Identity{Anchor: resource, Family: "storage-availability"}, Lifecycle: incident.Lifecycle{State: "open"}}}}}
	ai := &fakeAI{}

	var out, diag bytes.Buffer

	r := Runner{Config: cfg, Source: kube.NewSource(client, cfg), Analyzer: ai, Incidents: incident.Reconciler{Repository: repo},
		Output: &output.Writer{Out: &out, Err: &diag}}
	if r.Once(t.Context()) != 1 || ai.calls.Load() != 0 || len(repo.Records) != 1 ||
		repo.Records["existing"].Value.Lifecycle.State != "open" || strings.Contains(out.String()+diag.String(), "private-attachment-error") {
		t.Fatal("denied attachment collection invoked AI, leaked an API body or changed an incident")
	}
}

// TestAttachmentRecoveryFreshness starts continuous recovery review only on fresh state and rejects changed versions or renewed errors.
func TestAttachmentRecoveryFreshness(t *testing.T) {
	cfg := options()
	base := time.Now()
	attachment := attachmentForRunner()
	attachment.Attachment.Attached = true
	open := []incident.Record{{Value: incident.Incident{Identity: incident.Identity{Anchor: attachment.Resource, Family: "storage-availability"},
		Resources: []health.Resource{attachment.Resource}, Lifecycle: incident.Lifecycle{State: "open"}}}}

	contexts, err := incident.BuildInvestigationContexts(health.Snapshot{At: base, Objects: []health.Object{attachment}}, cfg.Policy, open, 100)
	if err != nil {
		t.Fatal(err)
	}

	key := incident.Ref(attachment.Resource)
	states := map[string]*candidate{}
	updateCandidates(states, contexts, open, base, cfg)

	if !states[key].recoverySince.Equal(base) {
		t.Fatal("attached state did not permit a fresh recovery review")
	}

	in := contexts[key]
	in.RecoveryEligible = true
	in.ToolVersions = map[string]string{key: "10"}
	source := &fakeSource{objects: []health.Object{attachment}}
	r := Runner{Config: cfg, Source: source}

	if !r.current(in, open) {
		t.Fatal("fresh unchanged attachment state rejected")
	}

	source.err = errors.New("attachment observation unknown")

	if r.current(in, open) {
		t.Fatal("unknown collection established attachment recovery")
	}

	source.err = nil
	source.objects[0].Version = "11"

	if r.current(in, open) {
		t.Fatal("changed inspected attachment version accepted")
	}

	source.objects[0].Version, source.objects[0].Attachment.Attached = "10", false

	if r.current(in, open) {
		t.Fatal("renewed attach error accepted as healthy recovery evidence")
	}

	contexts, err = incident.BuildInvestigationContexts(health.Snapshot{At: base.Add(time.Minute), Objects: source.objects}, cfg.Policy, open, 100)
	if err != nil {
		t.Fatal(err)
	}

	updateCandidates(states, contexts, open, base.Add(time.Minute), cfg)

	if !states[key].recoverySince.IsZero() {
		t.Fatal("renewed attachment failure did not reset continuous recovery")
	}
}
