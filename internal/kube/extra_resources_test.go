/*
Purpose: Verify extra-resource list/watch collection, projection and continuity offline.
Flow: Local Kubernetes HTTP fixtures cover namespace filters, cluster scope, shared
admission, explicit API failures and status changes without live resources or model calls.
*/
package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/resourcepolicy"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// certificate supplies an approved identity/status fixture with unselected private sentinels.
func certificate(namespace, ready, version string) map[string]any {
	return map[string]any{"apiVersion": "cert-manager.io/v1", "kind": "Certificate",
		"metadata": map[string]any{"name": "tls", "namespace": namespace, "uid": "certificate-" + namespace,
			"resourceVersion": version, "creationTimestamp": "2026-01-01T00:00:00Z",
			"annotations": map[string]any{"private": "private-annotation"}},
		"spec": map[string]any{"secretName": "private-secret"},
		"status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": ready,
			"message": "private-message"}}}}
}

// TestExtraResourceCollection verifies actual endpoints, filtering before projection/admission and explicit collection failures.
func TestExtraResourceCollection(t *testing.T) {
	definitions, err := resourcepolicy.Load("../../examples/extra-resources.yaml")
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name            string
		cluster, ignore bool
		max, code       int
	}{
		{name: "namespaced", max: 2},
		{name: "cluster", cluster: true, max: 2},
		{name: "ignore before projection", ignore: true, max: 2},
		{name: "shared object budget", max: 1},
		{name: "forbidden", max: 2, code: 403},
		{name: "missing API", max: 2, code: 404},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := testConfig()
			c.MaxObjects = tt.max

			c.Policy.ExtraResources = slices.Clone(definitions)
			if tt.cluster {
				c.Policy.ExtraResources[0].Scope = "Cluster"
			}

			if tt.ignore {
				c.NamespaceWatch, c.NamespaceIgnore = nil, []string{"ignored"}
			}

			reads := 0

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")

				items := []any{}

				if strings.HasSuffix(r.URL.Path, "/nodes") {
					items = append(items, node())
				}

				if strings.HasSuffix(r.URL.Path, "/certificates") {
					reads++

					expected := "/apis/cert-manager.io/v1/certificates"
					if !tt.cluster && !tt.ignore {
						expected = "/apis/cert-manager.io/v1/namespaces/apps/certificates"
					}

					if r.Method != http.MethodGet || r.URL.Path != expected || r.URL.Query().Get("limit") != "100" {
						t.Errorf("unexpected extra-resource request: %s %s", r.Method, r.URL)
					}

					if tt.code != 0 {
						w.WriteHeader(tt.code)
						fmt.Fprint(w, `{"apiVersion":"v1","kind":"Status","message":"private-error"}`)

						return
					}

					namespace := "apps"
					if tt.cluster {
						namespace = ""
					}

					items = append(items, certificate(namespace, "False", "1"))
					if tt.ignore {
						ignored := certificate("ignored", "False", "1")
						ignored["status"] = "invalid status must be filtered before projection"
						items = append(items, ignored)
					}
				}

				_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List",
					"metadata": map[string]any{"resourceVersion": "1"}, "items": items})
			}))
			defer server.Close()

			snapshot, err := NewSource(clientFor(t, server), c).Collect(t.Context(), time.Now())
			if (err != nil) != (tt.max == 1 || tt.code != 0) {
				t.Fatalf("collection: %v", err)
			}

			if err != nil {
				if strings.Contains(err.Error(), "private-error") {
					t.Fatal("raw API error leaked")
				}

				return
			}

			if reads != 1 || len(snapshot.Objects) != 2 {
				t.Fatal("extra resource was not admitted exactly once")
			}

			o := snapshot.Objects[1]

			data, _ := json.Marshal(o)
			if o.APIGroup != "cert-manager.io" || o.Extra == nil || strings.Contains(string(data), "private-") {
				t.Fatal("incorrect or unsafe custom projection")
			}

			copy := (&cachedObject{Value: o}).DeepCopyObject().(*cachedObject)

			copy.Value.Extra["conditions"].([]any)[0].(map[string]any)["status"] = "True"
			if o.Extra["conditions"].([]any)[0].(map[string]any)["status"] != "False" {
				t.Fatal("cache copies shared projected facts")
			}
		})
	}
}

// TestExtraResourceWatchChanges tracks status transitions in the real informer and host recovery markers.
func TestExtraResourceWatchChanges(t *testing.T) {
	c := testConfig()

	var err error

	c.Policy.ExtraResources, err = resourcepolicy.Load("../../examples/extra-resources.yaml")
	if err != nil {
		t.Fatal(err)
	}

	var watchReads atomic.Int32

	modify := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.URL.Query().Get("watch") == "true" {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()

			if strings.HasSuffix(r.URL.Path, "/certificates") {
				watchReads.Add(1)

				select {
				case <-modify:
					_ = json.NewEncoder(w).Encode(map[string]any{"type": "MODIFIED", "object": certificate("apps", "True", "2")})
					w.(http.Flusher).Flush()
				case <-r.Context().Done():
					return
				}
			}

			<-r.Context().Done()

			return
		}

		items := []any{}
		if strings.HasSuffix(r.URL.Path, "/nodes") {
			items = append(items, node())
		}

		if strings.HasSuffix(r.URL.Path, "/certificates") {
			items = append(items, certificate("apps", "False", "1"))
		}

		_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List",
			"metadata": map[string]any{"resourceVersion": "1"}, "items": items})
	}))
	defer server.Close()

	source := NewSource(clientFor(t, server), c)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() { defer close(done); source.Run(ctx) }()

	defer func() { cancel(); <-done }()

	until(t, func() bool { return source.Err() == nil && watchReads.Load() > 0 })
	close(modify)
	until(t, func() bool {
		snapshot, err := source.Snapshot(time.Now())
		if err != nil {
			return false
		}

		for _, o := range snapshot.Objects {
			if o.Extra == nil || o.Version != "2" {
				continue
			}

			name := o.Resource
			name.UID = ""
			findings, err := health.ExtraResources(snapshot, c.Policy)

			return err == nil && len(findings) == 0 && snapshot.RecoveryChanges[name] > 0
		}

		return false
	})

	u := &unstructured.Unstructured{Object: certificate("apps", "True", "2")}
	u.SetAPIVersion("wrong.example/v1")

	if _, err := ProjectExtraResource(u, c.Policy.ExtraResources[0], "apps"); err == nil {
		t.Fatal("unexpected API version accepted")
	}
}
