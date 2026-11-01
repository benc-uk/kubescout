/*
Purpose: Test bounded Ingress projection and networking.k8s.io/v1 collection.
Flow: Offline unstructured and HTTP list/watch fixtures verify route/default
backends, private-data exclusion, exact projection bounds and scoped failures.
No Secret, IngressClass, custom backend or external HTTP endpoint is accessed.
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

// ingressObject supplies named/numeric/default/custom backends with private annotations, TLS and address sentinels.
func ingressObject(namespace string) map[string]any {
	return map[string]any{"apiVersion": "networking.k8s.io/v1", "kind": "Ingress",
		"metadata": map[string]any{"name": "web", "namespace": namespace, "uid": namespace + "-ingress", "resourceVersion": "1", "generation": int64(2),
			"annotations": map[string]any{"nginx.ingress.kubernetes.io/configuration-snippet": "private-annotation"}},
		"spec": map[string]any{"ingressClassName": "nginx",
			"defaultBackend": map[string]any{"service": map[string]any{"name": "fallback", "port": map[string]any{"number": int64(80)}}},
			"tls":            []any{map[string]any{"hosts": []any{"private-tls-host"}, "secretName": "private-secret"}},
			"rules": []any{map[string]any{"host": "web.example", "http": map[string]any{"paths": []any{
				map[string]any{"path": "/", "pathType": "Prefix", "backend": map[string]any{"service": map[string]any{"name": "web", "port": map[string]any{"name": "http"}}}},
				map[string]any{"path": "/api", "pathType": "Exact", "backend": map[string]any{"service": map[string]any{"name": "api", "port": map[string]any{"number": int64(8080)}}}},
				map[string]any{"path": "/custom", "pathType": "ImplementationSpecific", "backend": map[string]any{"resource": map[string]any{"apiGroup": "custom.example", "kind": "StorageBucket", "name": "private-custom-backend"}}},
			}}}}},
		"status": map[string]any{"loadBalancer": map[string]any{"ingress": []any{map[string]any{"ip": "192.0.2.1", "hostname": "private-lb-host"}}}},
	}
}

// TestIngressProjection preserves only approved route facts, counts and independent cache copies.
func TestIngressProjection(t *testing.T) {
	o, err := project(&unstructured.Unstructured{Object: ingressObject("apps")}, "Ingress", "apps")
	if err != nil {
		t.Fatal(err)
	}

	i := o.Value.Ingress
	if o.Value.Generation != 2 || i.Class != "nginx" || i.TLSCount != 1 || i.AddressCount != 1 || len(i.Routes) != 4 ||
		!i.Routes[0].Default || i.Routes[0].Service != "fallback" || i.Routes[0].PortNumber != 80 ||
		i.Routes[1].Host != "web.example" || i.Routes[1].Path != "/" || i.Routes[1].PortName != "http" ||
		i.Routes[2].PortNumber != 8080 || !i.Routes[3].ResourceBackend {
		t.Fatalf("approved Ingress projection missing: %+v", i)
	}

	data, _ := json.Marshal(o)
	if strings.Contains(string(data), "private-") || strings.Contains(string(data), "192.0.2.1") {
		t.Fatal("Ingress annotations, TLS Secret/host, custom backend or address entered projection")
	}

	copy := o.DeepCopyObject().(*cachedObject)

	copy.Value.Ingress.Routes[0].Service = "other"
	if o.Value.Ingress.Routes[0].Service != "fallback" {
		t.Fatal("Ingress routes aliased in cache copy")
	}
}

// TestIngressProjectionBounds checks exact total-route/string/list bounds and rejects ambiguous Service backends.
func TestIngressProjectionBounds(t *testing.T) {
	repeat := func(value any, n int) []any {
		values := make([]any, n)
		for i := range values {
			values[i] = value
		}

		return values
	}
	paths := func(o map[string]any) map[string]any {
		return o["spec"].(map[string]any)["rules"].([]any)[0].(map[string]any)["http"].(map[string]any)
	}
	port := func(o map[string]any) map[string]any {
		return o["spec"].(map[string]any)["defaultBackend"].(map[string]any)["service"].(map[string]any)["port"].(map[string]any)
	}

	for _, tt := range []struct {
		name    string
		edit    func(map[string]any)
		invalid bool
	}{
		{"128 routes including default", func(o map[string]any) { http := paths(o); http["paths"] = repeat(http["paths"].([]any)[0], 127) }, false},
		{"129 routes including default", func(o map[string]any) { http := paths(o); http["paths"] = repeat(http["paths"].([]any)[0], 128) }, true},
		{"128 rules", func(o map[string]any) {
			o["spec"].(map[string]any)["rules"] = repeat(map[string]any{"host": "web.example"}, 128)
		}, false},
		{"129 rules", func(o map[string]any) {
			o["spec"].(map[string]any)["rules"] = repeat(map[string]any{"host": "web.example"}, 129)
		}, true},
		{"128 TLS entries", func(o map[string]any) { o["spec"].(map[string]any)["tls"] = repeat(map[string]any{}, 128) }, false},
		{"129 TLS entries", func(o map[string]any) { o["spec"].(map[string]any)["tls"] = repeat(map[string]any{}, 129) }, true},
		{"128 addresses", func(o map[string]any) {
			o["status"].(map[string]any)["loadBalancer"].(map[string]any)["ingress"] = repeat(map[string]any{"ip": "192.0.2.1"}, 128)
		}, false},
		{"129 addresses", func(o map[string]any) {
			o["status"].(map[string]any)["loadBalancer"].(map[string]any)["ingress"] = repeat(map[string]any{"ip": "192.0.2.1"}, 129)
		}, true},
		{"2048 path", func(o map[string]any) {
			paths(o)["paths"].([]any)[0].(map[string]any)["path"] = strings.Repeat("x", 2048)
		}, false},
		{"2049 path", func(o map[string]any) {
			paths(o)["paths"].([]any)[0].(map[string]any)["path"] = strings.Repeat("x", 2049)
		}, true},
		{"253 host", func(o map[string]any) {
			o["spec"].(map[string]any)["rules"].([]any)[0].(map[string]any)["host"] = strings.Repeat("x", 253)
		}, false},
		{"254 host", func(o map[string]any) {
			o["spec"].(map[string]any)["rules"].([]any)[0].(map[string]any)["host"] = strings.Repeat("x", 254)
		}, true},
		{"253 class", func(o map[string]any) { o["spec"].(map[string]any)["ingressClassName"] = strings.Repeat("x", 253) }, false},
		{"254 class", func(o map[string]any) { o["spec"].(map[string]any)["ingressClassName"] = strings.Repeat("x", 254) }, true},
		{"253 Service name", func(o map[string]any) {
			o["spec"].(map[string]any)["defaultBackend"].(map[string]any)["service"].(map[string]any)["name"] = strings.Repeat("x", 253)
		}, false},
		{"254 Service name", func(o map[string]any) {
			o["spec"].(map[string]any)["defaultBackend"].(map[string]any)["service"].(map[string]any)["name"] = strings.Repeat("x", 254)
		}, true},
		{"63 port name", func(o map[string]any) { delete(port(o), "number"); port(o)["name"] = strings.Repeat("x", 63) }, false},
		{"64 port name", func(o map[string]any) { delete(port(o), "number"); port(o)["name"] = strings.Repeat("x", 64) }, true},
		{"both port forms", func(o map[string]any) { port(o)["name"] = "http" }, true},
		{"missing port", func(o map[string]any) { delete(port(o), "number") }, true},
		{"out of range port", func(o map[string]any) { port(o)["number"] = int64(65536) }, true},
		{"missing path type", func(o map[string]any) { delete(paths(o)["paths"].([]any)[0].(map[string]any), "pathType") }, true},
		{"both backend types", func(o map[string]any) {
			o["spec"].(map[string]any)["defaultBackend"].(map[string]any)["resource"] = map[string]any{"kind": "Bucket", "name": "bucket"}
		}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw := ingressObject("apps")
			tt.edit(raw)

			if _, err := project(&unstructured.Unstructured{Object: raw}, "Ingress", "apps"); (err != nil) != tt.invalid {
				t.Fatalf("unexpected Ingress projection boundary: %v", err)
			}
		})
	}
}

// TestIngressCollectionScopeAndFailures verifies exact scoped networking requests and filters ignored objects before projection/admission.
func TestIngressCollectionScopeAndFailures(t *testing.T) {
	for _, tt := range []struct {
		name      string
		scope     config.Config
		max, want int
		denied    bool
	}{
		{name: "watch", scope: config.Config{NamespaceWatch: []string{"apps"}}, max: 2, want: 2},
		{name: "ignore", scope: config.Config{NamespaceIgnore: []string{"system"}}, max: 2, want: 2},
		{name: "all", max: 3, want: 3},
		{name: "shared budget", max: 2},
		{name: "denied", max: 3, denied: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Query().Get("limit") != "100" {
					t.Error("Ingress collection escaped bounded read-only requests")
				}

				w.Header().Set("Content-Type", "application/json")

				items := []any{}

				if strings.HasSuffix(r.URL.Path, "/nodes") {
					items = append(items, node())
				}

				if strings.HasSuffix(r.URL.Path, "/ingresses") {
					if tt.denied {
						w.WriteHeader(http.StatusForbidden)
						fmt.Fprint(w, `{"apiVersion":"v1","kind":"Status","reason":"Forbidden","code":403,"message":"private-ingress-error"}`)

						return
					}

					items = append(items, ingressObject("apps"))

					if len(tt.scope.NamespaceWatch) == 0 {
						if r.URL.Path != "/apis/networking.k8s.io/v1/ingresses" {
							t.Error("all/ignore Ingress requests were not all-namespace")
						}

						ignored := ingressObject("system")
						if len(tt.scope.NamespaceIgnore) != 0 {
							ignored["spec"].(map[string]any)["rules"] = "invalid-must-not-project"
						}

						items = append(items, ignored)
					} else if r.URL.Path != "/apis/networking.k8s.io/v1/namespaces/apps/ingresses" {
						t.Error("Ingress read escaped explicit watch scope")
					}
				}

				json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{"resourceVersion": "1"}, "items": items})
			}))
			defer server.Close()

			c := testConfig()
			c.NamespaceWatch, c.NamespaceIgnore, c.MaxObjects = tt.scope.NamespaceWatch, tt.scope.NamespaceIgnore, tt.max

			snapshot, err := NewSource(clientFor(t, server), c).Collect(t.Context(), time.Now())
			if tt.want == 0 {
				if err == nil || strings.Contains(err.Error(), "private-ingress-error") {
					t.Fatal("incomplete Ingress collection accepted or API body leaked")
				}
			} else if err != nil || len(snapshot.Objects) != tt.want {
				t.Fatalf("unexpected Ingress collection: count=%d err=%v", len(snapshot.Objects), err)
			}
		})
	}
}

// TestIngressWatchUpdate tracks route changes through the normal scoped projected watch cache.
func TestIngressWatchUpdate(t *testing.T) {
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

			if strings.HasSuffix(r.URL.Path, "/ingresses") {
				raw := ingressObject("apps")
				raw["metadata"].(map[string]any)["resourceVersion"] = "2"
				raw["spec"].(map[string]any)["defaultBackend"].(map[string]any)["service"].(map[string]any)["port"].(map[string]any)["number"] = int64(81)
				json.NewEncoder(w).Encode(map[string]any{"type": "MODIFIED", "object": raw})
				w.(http.Flusher).Flush()
			}

			<-r.Context().Done()

			return
		}

		items := []any{}
		if strings.HasSuffix(r.URL.Path, "/nodes") {
			items = append(items, node())
		} else if strings.HasSuffix(r.URL.Path, "/ingresses") {
			items = append(items, ingressObject("apps"))
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
			if o.Kind == "Ingress" {
				return o.Version == "2" && o.Ingress.Routes[0].PortNumber == 81
			}
		}

		return false
	})
}
