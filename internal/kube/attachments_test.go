/*
Purpose: Test safe VolumeAttachment projection and cluster-scoped list/watch collection.
Flow: Offline unstructured and HTTP fixtures verify source bounds, error metadata,
shared capacity, namespace-independent reads and watch updates. Attachment metadata,
inline volume configuration and raw driver messages never enter cached evidence.
*/
package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// attachmentObject supplies a PV-backed attachment with private metadata and error messages for offline fixtures.
func attachmentObject() map[string]any {
	return map[string]any{"apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
		"metadata": map[string]any{"name": "disk-node", "uid": "attachment-id", "resourceVersion": "1",
			"annotations": map[string]any{"private": "private-annotation"}},
		"spec": map[string]any{"attacher": "disk.csi.example", "nodeName": "node", "source": map[string]any{"persistentVolumeName": "disk"}},
		"status": map[string]any{"attached": false, "attachmentMetadata": map[string]any{"devicePath": "private-device-path", "volumeHandle": "private-volume-handle"},
			"attachError": map[string]any{"time": "2026-11-01T12:00:00Z", "errorCode": int64(0), "message": "private-driver-error"},
			"detachError": map[string]any{"message": "private-detach-error"}}}
}

// TestAttachmentProjection retains approved operation metadata, preserves optional codes and excludes raw storage details.
func TestAttachmentProjection(t *testing.T) {
	raw := attachmentObject()

	o, err := project(&unstructured.Unstructured{Object: raw}, "VolumeAttachment", "")
	if err != nil {
		t.Fatal(err)
	}

	a := o.Value.Attachment
	if a.Attacher != "disk.csi.example" || a.NodeName != "node" || a.VolumeName != "disk" || a.Inline || a.Attached ||
		!a.AttachError.Present || !a.AttachError.HasCode || a.AttachError.Code != 0 || a.AttachError.At.IsZero() ||
		!a.DetachError.Present || a.DetachError.HasCode || !a.DetachError.At.IsZero() {
		t.Fatalf("approved attachment facts missing: %+v", a)
	}

	data, _ := json.Marshal(o)
	if strings.Contains(string(data), "private-") || strings.Contains(string(data), "attachmentMetadata") {
		t.Fatal("raw attachment details entered cache projection")
	}

	raw["metadata"].(map[string]any)["deletionTimestamp"] = "2026-11-01T12:01:00Z"
	raw["spec"].(map[string]any)["source"] = map[string]any{"inlineVolumeSpec": map[string]any{
		"csi": map[string]any{"driver": "private-inline-driver", "volumeHandle": "private-inline-handle",
			"nodePublishSecretRef": map[string]any{"name": "private-secret", "namespace": "private-namespace"}}}}

	o, err = project(&unstructured.Unstructured{Object: raw}, "VolumeAttachment", "")
	if err != nil || !o.Value.Attachment.Inline || o.Value.Attachment.VolumeName != "" || !o.Value.Deleting || o.Value.Attachment.DeletingSince.IsZero() {
		t.Fatalf("inline/deleting attachment projection invalid: %v", err)
	}

	data, _ = json.Marshal(o)
	if strings.Contains(string(data), "private-") || strings.Contains(string(data), "SecretRef") {
		t.Fatal("inline volume configuration entered cache projection")
	}
}

// TestAttachmentProjectionBounds verifies exact identifier bounds and rejects ambiguous or missing source identity.
func TestAttachmentProjectionBounds(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(map[string]any)
		invalid bool
	}{
		{"253 driver", func(o map[string]any) { o["spec"].(map[string]any)["attacher"] = strings.Repeat("x", 253) }, false},
		{"254 driver", func(o map[string]any) { o["spec"].(map[string]any)["attacher"] = strings.Repeat("x", 254) }, true},
		{"253 node", func(o map[string]any) { o["spec"].(map[string]any)["nodeName"] = strings.Repeat("x", 253) }, false},
		{"254 node", func(o map[string]any) { o["spec"].(map[string]any)["nodeName"] = strings.Repeat("x", 254) }, true},
		{"253 PV", func(o map[string]any) {
			o["spec"].(map[string]any)["source"].(map[string]any)["persistentVolumeName"] = strings.Repeat("x", 253)
		}, false},
		{"254 PV", func(o map[string]any) {
			o["spec"].(map[string]any)["source"].(map[string]any)["persistentVolumeName"] = strings.Repeat("x", 254)
		}, true},
		{"missing source", func(o map[string]any) { o["spec"].(map[string]any)["source"] = map[string]any{} }, true},
		{"both sources", func(o map[string]any) {
			o["spec"].(map[string]any)["source"].(map[string]any)["inlineVolumeSpec"] = map[string]any{}
		}, true},
		{"missing node", func(o map[string]any) { delete(o["spec"].(map[string]any), "nodeName") }, true},
		{"empty PV", func(o map[string]any) {
			o["spec"].(map[string]any)["source"].(map[string]any)["persistentVolumeName"] = ""
		}, true},
		{"invalid error", func(o map[string]any) { o["status"].(map[string]any)["attachError"] = "invalid" }, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw := attachmentObject()
			tt.edit(raw)

			if _, err := project(&unstructured.Unstructured{Object: raw}, "VolumeAttachment", ""); (err != nil) != tt.invalid {
				t.Fatalf("unexpected attachment projection boundary: %v", err)
			}
		})
	}
}

// TestAttachmentCollectionScopeAndFailures verifies global attachment reads under all namespace modes and shared admission/failure boundaries.
func TestAttachmentCollectionScopeAndFailures(t *testing.T) {
	for _, tt := range []struct {
		name      string
		scope     config.Config
		max, want int
		denied    bool
	}{
		{name: "watch", scope: config.Config{NamespaceWatch: []string{"apps"}}, max: 2, want: 2},
		{name: "ignore", scope: config.Config{NamespaceIgnore: []string{"system"}}, max: 2, want: 2},
		{name: "all", max: 2, want: 2},
		{name: "shared budget", max: 1},
		{name: "denied", max: 2, denied: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Query().Get("limit") != "100" {
					t.Error("attachment collection escaped bounded read-only requests")
				}

				w.Header().Set("Content-Type", "application/json")

				items := []any{}

				if strings.HasSuffix(r.URL.Path, "/nodes") {
					items = append(items, node())
				}

				if strings.HasSuffix(r.URL.Path, "/volumeattachments") {
					if r.URL.Path != "/apis/storage.k8s.io/v1/volumeattachments" {
						t.Error("attachment read was not cluster-scoped")
					}

					if tt.denied {
						w.WriteHeader(http.StatusForbidden)
						fmt.Fprint(w, `{"apiVersion":"v1","kind":"Status","reason":"Forbidden","code":403,"message":"private-attachment-error"}`)

						return
					}

					items = append(items, attachmentObject())
				}

				json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{"resourceVersion": "1"}, "items": items})
			}))
			defer server.Close()

			c := testConfig()
			c.NamespaceWatch, c.NamespaceIgnore, c.MaxObjects = tt.scope.NamespaceWatch, tt.scope.NamespaceIgnore, tt.max

			snapshot, err := NewSource(clientFor(t, server), c).Collect(t.Context(), time.Now())
			if tt.want == 0 {
				if err == nil || strings.Contains(err.Error(), "private-attachment-error") {
					t.Fatal("incomplete attachment collection accepted or API body leaked")
				}
			} else if err != nil || len(snapshot.Objects) != tt.want {
				t.Fatalf("unexpected attachment collection: count=%d err=%v", len(snapshot.Objects), err)
			}
		})
	}

	raw := attachmentObject()

	raw["metadata"].(map[string]any)["namespace"] = "apps"
	if _, err := NewSource(nil, testConfig()).project(&unstructured.Unstructured{Object: raw}, resource{kind: "VolumeAttachment"}); err == nil {
		t.Fatal("namespaced attachment response accepted")
	}
}

// TestAttachmentWatchUpdate tracks successful attachment/error removal through the normal cluster-scoped projected cache.
func TestAttachmentWatchUpdate(t *testing.T) {
	trigger := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.URL.Query().Get("watch") == "true" {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()

			select {
			case <-trigger:
			case <-r.Context().Done():
				return
			}

			if strings.HasSuffix(r.URL.Path, "/volumeattachments") {
				raw := attachmentObject()
				raw["metadata"].(map[string]any)["resourceVersion"] = "2"
				raw["status"].(map[string]any)["attached"] = true
				delete(raw["status"].(map[string]any), "attachError")
				json.NewEncoder(w).Encode(map[string]any{"type": "MODIFIED", "object": raw})
				w.(http.Flusher).Flush()
			}

			<-r.Context().Done()

			return
		}

		items := []any{}
		if strings.HasSuffix(r.URL.Path, "/nodes") {
			items = append(items, node())
		} else if strings.HasSuffix(r.URL.Path, "/volumeattachments") {
			items = append(items, attachmentObject())
		}

		json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{"resourceVersion": "1"}, "items": items})
	}))
	defer server.Close()

	source := NewSource(clientFor(t, server), testConfig())
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() { defer close(done); source.Run(ctx) }()

	defer func() { cancel(); <-done }()

	until(t, func() bool { return source.Err() == nil })
	close(trigger)
	until(t, func() bool {
		snapshot, err := source.Snapshot(time.Now())
		if err != nil {
			return false
		}

		for _, o := range snapshot.Objects {
			if o.Kind == "VolumeAttachment" {
				return o.Version == "2" && o.Attachment.Attached && !o.Attachment.AttachError.Present
			}
		}

		return false
	})
}
