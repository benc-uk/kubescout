/*
Purpose: Test bounded storage collection and backend-free projection.
Flow: Offline unstructured and HTTP list/watch fixtures cover cluster-wide PVs,
scoped PVCs, shared bounds, watch changes and denied access. Raw storage sources,
credentials and mount paths never enter projected evidence.
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

// storageObject supplies explicit PVC/PV facts and private sentinel fields for offline collection.
func storageObject(kind, namespace, phase string) map[string]any {
	spec := map[string]any{
		"storageClassName": "standard", "accessModes": []any{"ReadWriteOnce"}, "volumeMode": "Filesystem",
		"csi": map[string]any{"driver": "private-driver", "volumeHandle": "private-volume-handle",
			"volumeAttributes": map[string]any{"path": "private-path"}, "nodePublishSecretRef": map[string]any{"name": "private-secret"}},
		"nfs": map[string]any{"server": "private-server", "path": "private-path"},
	}
	status := map[string]any{"phase": phase}

	if kind == "PersistentVolumeClaim" {
		spec["resources"] = map[string]any{"requests": map[string]any{"storage": "2Gi"}}
		spec["volumeName"] = "disk"
		status["capacity"] = map[string]any{"storage": "1Gi"}
		status["conditions"] = []any{map[string]any{"type": "Resizing", "status": "True", "message": "private-message"}}
	} else {
		spec["capacity"] = map[string]any{"storage": "1Gi"}
		spec["persistentVolumeReclaimPolicy"] = "Retain"
		spec["claimRef"] = map[string]any{"kind": "PersistentVolumeClaim", "namespace": "apps", "name": "data", "uid": "claim-id"}
		status["message"] = "private-message"
	}

	name, uid := "data", "claim-id"
	if kind == "PersistentVolume" {
		name, uid = "disk", "volume-id"
	}

	return map[string]any{"apiVersion": "v1", "kind": kind,
		"metadata": map[string]any{"name": name, "namespace": namespace, "uid": uid, "resourceVersion": "1",
			"annotations": map[string]any{"private": "private-annotation"}}, "spec": spec, "status": status}
}

// TestStorageProjection verifies approved quantities, private links, copy isolation and hard list bounds.
func TestStorageProjection(t *testing.T) {
	for _, kind := range []string{"PersistentVolumeClaim", "PersistentVolume"} {
		namespace := "apps"
		if kind == "PersistentVolume" {
			namespace = ""
		}

		raw := storageObject(kind, namespace, "Bound")

		o, err := project(&unstructured.Unstructured{Object: raw}, kind, namespace)
		if err != nil {
			t.Fatal(err)
		}

		if o.Value.Storage.Class != "standard" || o.Value.Storage.Capacity != "1Gi" || o.Value.Storage.Mode != "Filesystem" ||
			len(o.Value.Storage.AccessModes) != 1 || o.Value.Storage.AccessModes[0] != "ReadWriteOnce" {
			t.Fatalf("storage facts missing: %+v", o.Value.Storage)
		}

		if kind == "PersistentVolumeClaim" && (o.Value.Storage.Requested != "2Gi" || o.Value.Storage.VolumeName != "disk") ||
			kind == "PersistentVolume" && (o.Value.Storage.Claim.UID != "claim-id" || o.Value.Storage.ReclaimPolicy != "Retain") {
			t.Fatal("private storage linkage missing")
		}

		data, _ := json.Marshal(o)
		if strings.Contains(string(data), "private-") || strings.Contains(string(data), "claim-id") && kind == "PersistentVolume" {
			t.Fatal("backend configuration or raw claim UID retained in serialization")
		}

		copy := o.DeepCopyObject().(*cachedObject)

		copy.Value.Storage.AccessModes[0] = "ReadOnlyMany"
		if o.Value.Storage.AccessModes[0] != "ReadWriteOnce" {
			t.Fatal("storage access modes aliased in cache copy")
		}

		modes := make([]any, 9)
		for i := range modes {
			modes[i] = "ReadWriteOnce"
		}

		raw["spec"].(map[string]any)["accessModes"] = modes
		if _, err := project(&unstructured.Unstructured{Object: raw}, kind, namespace); err == nil {
			t.Fatal("access mode projection bound ignored")
		}
	}

	raw := pod("consumer", "")
	volumes := []any{map[string]any{"name": "data", "persistentVolumeClaim": map[string]any{"claimName": "data"}},
		map[string]any{"name": "config", "secret": map[string]any{"secretName": "private-secret"}}}
	raw["spec"].(map[string]any)["volumes"] = volumes

	o, err := project(&unstructured.Unstructured{Object: raw}, "Pod", "apps")
	if err != nil || len(o.Value.Claims) != 1 || o.Value.Claims[0] != "data" {
		t.Fatalf("explicit Pod claim use missing: %v", err)
	}

	copy := o.DeepCopyObject().(*cachedObject)

	copy.Value.Claims[0] = "other"
	if o.Value.Claims[0] != "data" {
		t.Fatal("Pod claims aliased in cache copy")
	}

	for len(volumes) <= 128 {
		volumes = append(volumes, volumes[0])
	}

	raw["spec"].(map[string]any)["volumes"] = volumes
	if _, err := project(&unstructured.Unstructured{Object: raw}, "Pod", "apps"); err == nil {
		t.Fatal("Pod volume projection bound ignored")
	}
}

// TestStorageCollectionScopeAndFailures checks PVC filters before admission, always-cluster PVs and incomplete denied reads.
func TestStorageCollectionScopeAndFailures(t *testing.T) {
	for _, tt := range []struct {
		name   string
		scope  config.Config
		denied string
		max    int
		want   int
	}{
		{name: "watch", scope: config.Config{NamespaceWatch: []string{"apps"}}, max: 3, want: 3},
		{name: "ignore before admission", scope: config.Config{NamespaceIgnore: []string{"system"}}, max: 3, want: 3},
		{name: "all", max: 4, want: 4},
		{name: "shared budget", max: 2},
		{name: "denied PV", denied: "persistentvolumes", max: 4},
		{name: "denied PVC", denied: "persistentvolumeclaims", max: 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")

				if r.Method != http.MethodGet || r.URL.Query().Get("limit") != "100" {
					t.Errorf("unexpected collection request: %s %s", r.Method, r.URL)
				}

				if tt.denied != "" && strings.HasSuffix(r.URL.Path, "/"+tt.denied) {
					w.WriteHeader(http.StatusForbidden)
					fmt.Fprint(w, `{"apiVersion":"v1","kind":"Status","reason":"Forbidden","code":403,"message":"private-error"}`)

					return
				}

				items := []any{}

				switch {
				case strings.HasSuffix(r.URL.Path, "/nodes"):
					items = append(items, node())
				case strings.HasSuffix(r.URL.Path, "/persistentvolumes"):
					if r.URL.Path != "/api/v1/persistentvolumes" {
						t.Error("PV request was not cluster-scoped")
					}

					items = append(items, storageObject("PersistentVolume", "", "Failed"))
				case strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims"):
					items = append(items, storageObject("PersistentVolumeClaim", "apps", "Lost"))
					if len(tt.scope.NamespaceWatch) == 0 {
						ignored := storageObject("PersistentVolumeClaim", "system", "Lost")
						if len(tt.scope.NamespaceIgnore) != 0 {
							ignored["spec"].(map[string]any)["accessModes"] = "invalid-must-not-project"
						}

						items = append(items, ignored)
					} else if r.URL.Path != "/api/v1/namespaces/apps/persistentvolumeclaims" {
						t.Error("PVC request escaped watch scope")
					}
				}

				json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List",
					"metadata": map[string]any{"resourceVersion": "1"}, "items": items})
			}))
			defer server.Close()

			c := testConfig()
			c.NamespaceWatch, c.NamespaceIgnore, c.MaxObjects = tt.scope.NamespaceWatch, tt.scope.NamespaceIgnore, tt.max

			snapshot, err := NewSource(clientFor(t, server), c).Collect(t.Context(), time.Now())
			if tt.want == 0 {
				if err == nil || strings.Contains(err.Error(), "private-error") {
					t.Fatal("failed storage collection accepted or leaked API body")
				}
			} else if err != nil || len(snapshot.Objects) != tt.want {
				t.Fatalf("unexpected storage collection: count=%d err=%v", len(snapshot.Objects), err)
			}
		})
	}

	c := testConfig()
	if _, err := NewSource(nil, c).project(&unstructured.Unstructured{Object: storageObject("PersistentVolume", "apps", "Failed")}, resource{kind: "PersistentVolume"}); err == nil {
		t.Fatal("namespaced PV response accepted")
	}
}

// TestStorageWatchUpdates tracks PVC/PV modifications and deletion under the normal cache and freshness boundary.
func TestStorageWatchUpdates(t *testing.T) {
	trigger := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		kind, namespace := "", ""

		if strings.HasSuffix(r.URL.Path, "/persistentvolumes") {
			kind = "PersistentVolume"
		} else if strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims") {
			kind, namespace = "PersistentVolumeClaim", "apps"
		}

		if r.URL.Query().Get("watch") == "true" {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()

			select {
			case <-trigger:
			case <-r.Context().Done():
				return
			}

			if kind != "" {
				o := storageObject(kind, namespace, "Bound")
				o["metadata"].(map[string]any)["resourceVersion"] = "2"

				event := "MODIFIED"
				if kind == "PersistentVolume" {
					event = "DELETED"
				}

				json.NewEncoder(w).Encode(map[string]any{"type": event, "object": o})
				w.(http.Flusher).Flush()
			}

			<-r.Context().Done()

			return
		}

		items := []any{}
		if kind != "" {
			items = append(items, storageObject(kind, namespace, "Pending"))
		} else if strings.HasSuffix(r.URL.Path, "/nodes") {
			items = append(items, node())
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
		if err != nil || len(snapshot.Objects) != 2 {
			return false
		}

		for _, o := range snapshot.Objects {
			if o.Kind == "PersistentVolumeClaim" {
				return o.Phase == "Bound" && o.Version == "2"
			}
		}

		return false
	})
}
