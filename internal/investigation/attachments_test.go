/*
Purpose: Test approved attachment inspection and scoped cluster-resource events.
Flow: Offline HTTP fixtures verify storage API shapes, redacted/truncated driver
errors, authorized PV/Node handles and shared budgets. Raw attachment metadata
and inline configuration never become accepted evidence or API parameters.
*/
package investigation

import (
	"encoding/json"
	"errors"
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

// attachmentInspectionObject supplies optional numeric errors and synthetic private backend data.
func attachmentInspectionObject() map[string]any {
	return map[string]any{"apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
		"metadata": map[string]any{"name": "disk-node", "uid": "attachment-uid", "resourceVersion": "7",
			"annotations": map[string]any{"private": "private-annotation"}},
		"spec": map[string]any{"attacher": "disk.csi.example", "nodeName": "node", "source": map[string]any{"persistentVolumeName": "disk"}},
		"status": map[string]any{"attached": false,
			"attachmentMetadata": map[string]any{"devicePath": "private-device-path", "volumeHandle": "private-volume-handle"},
			"attachError":        map[string]any{"time": "2026-11-01T12:00:00Z", "errorCode": int64(0), "message": "password=fixture"},
			"detachError":        map[string]any{"message": strings.Repeat("x", 2048)}}}
}

// TestAttachmentInspectionAndEvents checks approved fields, scoped event requests and private-data exclusion.
func TestAttachmentInspectionAndEvents(t *testing.T) {
	reads := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads++

		if r.Method != http.MethodGet {
			t.Error("attachment tool attempted a storage write")
		}

		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/apis/storage.k8s.io/v1/volumeattachments/disk-node":
			json.NewEncoder(w).Encode(attachmentInspectionObject())
		case "/api/v1/namespaces/apps/events":
			if r.URL.Query().Get("fieldSelector") != "type=Warning,involvedObject.kind=VolumeAttachment,involvedObject.name=disk-node" ||
				r.URL.Query().Get("limit") != "20" {
				t.Error("attachment event lookup escaped selected namespaces or target")
			}

			fmt.Fprint(w, `{"apiVersion":"v1","kind":"EventList","metadata":{"resourceVersion":"1"},"items":[{"metadata":{"namespace":"apps"},"type":"Warning","involvedObject":{"uid":"attachment-uid","kind":"VolumeAttachment","name":"disk-node"},"reason":"AttachFailed","message":"token=fixture"}]}`)
		default:
			t.Error("attachment tool fetched inline config, credentials or arbitrary resources")
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	defer server.Close()

	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}

	pv := health.Object{Resource: health.Resource{Kind: "PersistentVolume", Name: "disk", UID: "pv-uid"}}
	node := health.Object{Resource: health.Resource{Kind: "Node", Name: "node", UID: "node-uid"}}
	pvRef, nodeRef := incident.Ref(pv.Resource), incident.Ref(node.Resource)
	s := &Session{Client: client, Scope: config.Config{NamespaceWatch: []string{"apps"}}, MaxCalls: 4, Timeout: time.Second,
		Citations: map[string]bool{}, Investigation: incident.InvestigationContext{
			Identities: map[string]health.Resource{"ref": {Kind: "VolumeAttachment", Name: "disk-node", UID: "attachment-uid"}, pvRef: pv.Resource, nodeRef: node.Resource},
			Objects:    map[string]health.Object{pvRef: pv, nodeRef: node}, ToolVersions: map[string]string{}}}

	value, err := (InspectResource{Session: s}).Call(t.Context(), `{"resource_ref":"ref"}`)
	if err != nil {
		t.Fatal(err)
	}

	state := value.(map[string]any)["state"].(map[string]any)

	attach, detach := state["attachError"].(map[string]any), state["detachError"].(map[string]any)
	if state["attacher"] != "disk.csi.example" || state["attached"] != false || state["inline_volume"] != false ||
		state["volume_ref"] != pvRef || state["node_ref"] != nodeRef || s.Investigation.ToolVersions["ref"] != "7" ||
		attach["message"] != "[redacted sensitive text]" || attach["error_code"] != int32(0) || attach["time"] == nil ||
		detach["error_code"] != nil || detach["time"] != nil || detach["message"] != strings.Repeat("x", 1024)+" [truncated]" {
		t.Fatalf("approved attachment evidence shape missing: %+v", state)
	}

	data, _ := json.Marshal(value)
	for _, forbidden := range []string{"private-", "attachment-uid", "pv-uid", "node-uid", "password=fixture", "attachmentMetadata", "inlineVolumeSpec"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("unapproved attachment field reached model evidence: %s", forbidden)
		}
	}

	events, err := (RelatedWarningEvents{Session: s}).Call(t.Context(), `{"resource_ref":"ref"}`)
	if err != nil || reads != 2 || len(s.Citations) != 2 {
		t.Fatalf("attachment Warning-event tool failed: %v", err)
	}

	data, _ = json.Marshal(events)
	if strings.Contains(string(data), "token=fixture") {
		t.Fatal("attachment event message was not redacted")
	}
}

// TestAttachmentToolRejectsInvalidEvidence checks cluster scope, incarnation, source schema and cumulative evidence-byte limits.
func TestAttachmentToolRejectsInvalidEvidence(t *testing.T) {
	for _, tt := range []struct {
		name, namespace string
		edit            func(map[string]any)
		reads, bytes    int
	}{
		{name: "namespace", namespace: "apps", edit: func(o map[string]any) {}},
		{name: "incarnation", edit: func(o map[string]any) { o["metadata"].(map[string]any)["uid"] = "changed" }, reads: 1},
		{name: "schema", edit: func(o map[string]any) { o["spec"].(map[string]any)["source"] = map[string]any{} }, reads: 1},
		{name: "cumulative bytes", edit: func(o map[string]any) {}, reads: 1, bytes: 32768},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reads := 0

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				reads++
				o := attachmentInspectionObject()
				tt.edit(o)
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(o)
			}))
			defer server.Close()

			client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}

			s := &Session{Client: client, Scope: config.Config{NamespaceWatch: []string{"apps"}}, MaxCalls: 4, Timeout: time.Second, Bytes: tt.bytes,
				Citations: map[string]bool{}, Investigation: incident.InvestigationContext{
					Identities: map[string]health.Resource{"ref": {Kind: "VolumeAttachment", Namespace: tt.namespace, Name: "disk-node", UID: "attachment-uid"}}}}

			value, err := (InspectResource{Session: s}).Call(t.Context(), `{"resource_ref":"ref"}`)
			if err == nil || value != nil || reads != tt.reads || len(s.Citations) != 0 {
				t.Fatal("invalid attachment tool evidence accepted")
			}

			if tt.bytes != 0 {
				var limit *LimitError

				if !errors.As(err, &limit) || limit.Kind != "local_cumulative_tool_bytes" || limit.Limit != 32768 || limit.Observed <= limit.Limit {
					t.Fatalf("attachment output exceeded the wrong limit: %v", err)
				}
			}
		})
	}
}
