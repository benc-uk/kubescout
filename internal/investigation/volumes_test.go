/*
Purpose: Test handle-scoped storage tools and backend exclusion.
Flow: Offline Kubernetes HTTP fixtures verify cluster PV and scoped PVC GETs,
approved storage state, binding handles, redaction, identity checks and bounded
PV Warning-event reads. Tools never fetch unapproved storage sources or workloads.
*/
package investigation

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// TestStorageInspection checks exact approved facts, authorized binding handles and private-field exclusion.
func TestStorageInspection(t *testing.T) {
	for _, kind := range []string{"PersistentVolumeClaim", "PersistentVolume"} {
		t.Run(kind, func(t *testing.T) {
			claim := health.Object{Resource: health.Resource{Kind: "PersistentVolumeClaim", Namespace: "apps", Name: "data", UID: "claim-uid"},
				Storage: health.Storage{VolumeName: "disk"}}
			volume := health.Object{Resource: health.Resource{Kind: "PersistentVolume", Name: "disk", UID: "volume-uid"}, Storage: health.Storage{Claim: claim.Resource}}
			target, peer, path, binding := claim, volume, "/api/v1/namespaces/apps/persistentvolumeclaims/data", "bound_volume_ref"

			if kind == "PersistentVolume" {
				target, peer, path, binding = volume, claim, "/api/v1/persistentvolumes/disk", "bound_claim_ref"
			}

			ref, peerRef := incident.Ref(target.Resource), incident.Ref(peer.Resource)

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != path {
					t.Error("storage inspection escaped authorized handle")
				}

				spec := map[string]any{"storageClassName": "standard", "accessModes": []any{"ReadWriteOnce"},
					"volumeMode": "Filesystem", "capacity": map[string]any{"storage": "1Gi"}, "persistentVolumeReclaimPolicy": "Retain",
					"volumeName": "disk", "claimRef": map[string]any{"kind": "PersistentVolumeClaim", "name": "data", "namespace": "apps", "uid": "claim-uid"},
					"resources": map[string]any{"requests": map[string]any{"storage": "2Gi"}},
					"csi": map[string]any{"driver": "private-driver", "volumeHandle": "private-handle",
						"nodePublishSecretRef": map[string]any{"name": "private-secret"}, "volumeAttributes": map[string]any{"path": "private-path"}},
					"nfs": map[string]any{"server": "private-server", "path": "private-path"}, "mountOptions": []any{"private-mount-option"}}

				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": kind,
					"metadata": map[string]any{"name": target.Name, "namespace": target.Namespace, "uid": target.UID, "resourceVersion": "7",
						"annotations": map[string]any{"private": "private-annotation"}}, "spec": spec,
					"status": map[string]any{"phase": "Bound", "capacity": map[string]any{"storage": "1Gi"},
						"conditions": []any{map[string]any{"type": "Resizing", "status": "True", "message": "password=fixture"}},
						"message":    "token=fixture"}})
			}))
			defer server.Close()

			client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}

			s := &Session{Client: client, Scope: config.Config{NamespaceWatch: []string{"apps"}}, MaxCalls: 4, Timeout: time.Second,
				Citations: map[string]bool{}, Investigation: incident.InvestigationContext{
					Identities: map[string]health.Resource{ref: target.Resource, peerRef: peer.Resource},
					Objects:    map[string]health.Object{ref: target, peerRef: peer}, ToolVersions: map[string]string{}}}

			value, err := (InspectResource{Session: s}).Call(t.Context(), fmt.Sprintf(`{"resource_ref":%q}`, ref))
			if err != nil {
				t.Fatal(err)
			}

			state := value.(map[string]any)["state"].(map[string]any)
			if state["storage_class"] != "standard" || state["capacity"] != "1Gi" || state["volume_mode"] != "Filesystem" ||
				state[binding] != peerRef || s.Investigation.ToolVersions[ref] != "7" {
				t.Fatalf("approved storage facts or binding handle missing: %+v", state)
			}

			if kind == "PersistentVolumeClaim" && state["requested_storage"] != "2Gi" ||
				kind == "PersistentVolume" && state["reclaim_policy"] != "Retain" {
				t.Fatal("kind-specific storage facts missing")
			}

			data, _ := json.Marshal(value)
			for _, forbidden := range []string{"private-", "claim-uid", "volume-uid", "password=fixture", "token=fixture", "claimRef", "volumeName", "csi", "nfs", "mountOptions"} {
				if strings.Contains(string(data), forbidden) {
					t.Fatalf("unapproved storage field escaped: %s", forbidden)
				}
			}
		})
	}
}

// TestStorageToolScopeRejectsBeforeRead verifies PVC scope and cluster PV namespace validation before any API request.
func TestStorageToolScopeRejectsBeforeRead(t *testing.T) {
	reads := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reads++

		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}

	for _, r := range []health.Resource{
		{Kind: "PersistentVolumeClaim", Namespace: "other", Name: "data", UID: "uid"},
		{Kind: "PersistentVolume", Namespace: "apps", Name: "disk", UID: "uid"},
	} {
		s := &Session{Client: client, Scope: config.Config{NamespaceWatch: []string{"apps"}}, MaxCalls: 4, Timeout: time.Second,
			Citations: map[string]bool{}, Investigation: incident.InvestigationContext{Identities: map[string]health.Resource{"ref": r}}}
		if value, err := (InspectResource{Session: s}).Call(t.Context(), `{"resource_ref":"ref"}`); err == nil || value != nil {
			t.Fatal("out-of-scope storage tool accepted")
		}
	}

	if reads != 0 {
		t.Fatal("rejected scope reached Kubernetes")
	}
}

// TestPVWarningEventsRespectNamespaceScope verifies cluster PV event reads filter ignored namespaces and stale UIDs.
func TestPVWarningEventsRespectNamespaceScope(t *testing.T) {
	for _, stale := range []bool{false, true} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/api/v1/events" || r.URL.Query().Get("limit") != "20" ||
				r.URL.Query().Get("fieldSelector") != "type=Warning,involvedObject.kind=PersistentVolume,involvedObject.name=disk" {
				t.Error("PV event read escaped scoped bounded lookup")
			}

			uid := "volume-uid"
			if stale {
				uid = "previous-incarnation"
			}

			event := map[string]any{"metadata": map[string]any{"namespace": "apps"}, "type": "Warning",
				"involvedObject": map[string]any{"kind": "PersistentVolume", "name": "disk", "uid": uid},
				"reason":         "VolumeFailed", "message": "password=fixture"}
			ignored := map[string]any{"metadata": map[string]any{"namespace": "system"}, "type": "Warning",
				"involvedObject": map[string]any{"kind": "Secret", "name": "private-ignored"}}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "EventList",
				"metadata": map[string]any{"resourceVersion": "1"}, "items": []any{ignored, event}})
		}))

		client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
		if err != nil {
			server.Close()
			t.Fatal(err)
		}

		s := &Session{Client: client, Scope: config.Config{NamespaceIgnore: []string{"system"}}, MaxCalls: 4, Timeout: time.Second,
			Citations: map[string]bool{}, Investigation: incident.InvestigationContext{Identities: map[string]health.Resource{
				"ref": {Kind: "PersistentVolume", Name: "disk", UID: "volume-uid"}}}}
		value, err := (RelatedWarningEvents{Session: s}).Call(t.Context(), `{"resource_ref":"ref"}`)

		server.Close()

		if stale {
			if err == nil || value != nil || len(s.Citations) != 0 {
				t.Fatal("stale PV event accepted as evidence")
			}

			continue
		}

		if err != nil || len(value.(map[string]any)["events"].([]map[string]any)) != 1 {
			t.Fatalf("unexpected scoped PV events: %v", err)
		}

		data, _ := json.Marshal(value)
		if strings.Contains(string(data), "password=fixture") || strings.Contains(string(data), "private-ignored") {
			t.Fatal("PV events leaked private or ignored evidence")
		}
	}
}
