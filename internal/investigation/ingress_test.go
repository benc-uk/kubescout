/*
Purpose: Test safe Ingress inspection and scoped controller Warning events.
Flow: Offline HTTP fixtures verify networking GETs, credential-only route protection, authorized
Service handles, private-data exclusion and shared tool budgets. Invalid
scope/schema/incarnation returns no accepted evidence; no external probes run.
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

// ingressInspectionObject supplies normal/default/custom routes and synthetic private values.
func ingressInspectionObject() map[string]any {
	return map[string]any{"apiVersion": "networking.k8s.io/v1", "kind": "Ingress",
		"metadata": map[string]any{"name": "web", "namespace": "apps", "uid": "ingress-uid", "resourceVersion": "7", "generation": int64(2),
			"annotations": map[string]any{"private": "private-annotation"}},
		"spec": map[string]any{"ingressClassName": "nginx",
			"defaultBackend": map[string]any{"service": map[string]any{"name": "web", "port": map[string]any{"number": int64(80)}}},
			"rules": []any{map[string]any{"host": "web.example", "http": map[string]any{"paths": []any{
				map[string]any{"path": "/password=fixture", "pathType": "Prefix", "backend": map[string]any{"service": map[string]any{"name": "web", "port": map[string]any{"name": "http"}}}},
				map[string]any{"path": "/custom", "pathType": "Exact", "backend": map[string]any{"resource": map[string]any{"kind": "Bucket", "name": "private-custom-backend"}}},
			}}}},
			"tls": []any{map[string]any{"hosts": []any{"private-tls-host"}, "secretName": "private-secret"}}},
		"status": map[string]any{"loadBalancer": map[string]any{"ingress": []any{map[string]any{"ip": "192.0.2.1", "hostname": "private-address"}}}},
	}
}

// TestIngressInspectionAndEvents checks credential-protected routes, authorized backend handles and scoped Warning reads.
func TestIngressInspectionAndEvents(t *testing.T) {
	reads := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads++

		if r.Method != http.MethodGet {
			t.Error("Ingress tool attempted a workload write")
		}

		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/apis/networking.k8s.io/v1/namespaces/apps/ingresses/web":
			json.NewEncoder(w).Encode(ingressInspectionObject())
		case "/api/v1/namespaces/apps/events":
			if r.URL.Query().Get("fieldSelector") != "type=Warning,involvedObject.uid=ingress-uid" || r.URL.Query().Get("limit") != "20" {
				t.Error("Ingress event read escaped bounded authorized scope")
			}

			fmt.Fprint(w, `{"apiVersion":"v1","kind":"EventList","metadata":{"resourceVersion":"1"},"items":[{"metadata":{"namespace":"apps"},"type":"Warning","involvedObject":{"uid":"ingress-uid","kind":"Ingress","name":"web"},"reason":"Rejected","message":"token=fixture"}]}`)
		default:
			t.Error("Ingress tool fetched a Secret, IngressClass, custom backend or arbitrary URL")
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	defer server.Close()

	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}

	service := health.Object{Resource: health.Resource{Kind: "Service", Namespace: "apps", Name: "web", UID: "service-uid"}}
	ref := incident.Ref(service.Resource)
	s := &Session{Client: client, Scope: config.Config{NamespaceWatch: []string{"apps"}}, MaxCalls: 4, Timeout: time.Second,
		Citations: map[string]bool{}, Investigation: incident.InvestigationContext{
			Identities: map[string]health.Resource{"ref": {Kind: "Ingress", Namespace: "apps", Name: "web", UID: "ingress-uid"}, ref: service.Resource},
			Objects:    map[string]health.Object{ref: service}, ToolVersions: map[string]string{}}}

	value, err := (InspectResource{Session: s}).Call(t.Context(), `{"resource_ref":"ref"}`)
	if err != nil {
		t.Fatal(err)
	}

	state := value.(map[string]any)["state"].(map[string]any)

	routes := state["routes"].([]map[string]any)
	if state["generation"] != int64(2) || state["class_name"] != "nginx" || state["tls_entry_count"] != 1 ||
		state["load_balancer_address_count"] != 1 || s.Investigation.ToolVersions["ref"] != "7" || len(routes) != 3 ||
		routes[0]["default_backend"] != true || routes[0]["port_number"] != int32(80) || routes[0]["service_ref"] != ref ||
		routes[1]["port_name"] != "http" || !strings.Contains(fmt.Sprint(routes[1]["path"]), "[redacted credential]") || routes[1]["service_ref"] != ref ||
		routes[2]["backend_type"] != "Resource" || routes[2]["service_ref"] != nil {
		t.Fatalf("approved Ingress inspection shape missing: %+v", state)
	}

	data, _ := json.Marshal(value)
	for _, forbidden := range []string{"private-", "ingress-uid", "service-uid", "192.0.2.1", "password=fixture", "secretName", "annotations"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("unapproved Ingress data reached the model: %s", forbidden)
		}
	}

	events, err := (RelatedWarningEvents{Session: s}).Call(t.Context(), `{"resource_ref":"ref"}`)
	if err != nil || reads != 2 || len(s.Citations) != 2 {
		t.Fatalf("Ingress controller Warning tool failed: %v", err)
	}

	data, _ = json.Marshal(events)
	if strings.Contains(string(data), "token=fixture") {
		t.Fatal("Ingress event message was not redacted")
	}
}

// TestIngressToolRejectsInvalidEvidence enforces scope, resource incarnation, route schema and exact tool-result byte limits.
func TestIngressToolRejectsInvalidEvidence(t *testing.T) {
	for _, tt := range []struct {
		name, namespace string
		edit            func(map[string]any)
		reads           int
	}{
		{name: "scope", namespace: "other", edit: func(o map[string]any) {}},
		{name: "incarnation", namespace: "apps", edit: func(o map[string]any) { o["metadata"].(map[string]any)["uid"] = "changed" }, reads: 1},
		{name: "schema", namespace: "apps", edit: func(o map[string]any) { o["spec"].(map[string]any)["defaultBackend"] = map[string]any{} }, reads: 1},
		{name: "result bytes", namespace: "apps", edit: func(o map[string]any) {
			paths := make([]any, 16)
			for i := range paths {
				paths[i] = map[string]any{"path": "/" + strings.Repeat("x", 1023), "pathType": "Prefix", "backend": map[string]any{"service": map[string]any{"name": "web", "port": map[string]any{"number": int64(80)}}}}
			}

			o["spec"].(map[string]any)["rules"].([]any)[0].(map[string]any)["http"].(map[string]any)["paths"] = paths
		}, reads: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reads := 0

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				reads++
				o := ingressInspectionObject()
				tt.edit(o)
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(o)
			}))
			defer server.Close()

			client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}

			s := &Session{Client: client, Scope: config.Config{NamespaceWatch: []string{"apps"}}, MaxCalls: 4, Timeout: time.Second,
				Citations: map[string]bool{}, Investigation: incident.InvestigationContext{
					Identities: map[string]health.Resource{"ref": {Kind: "Ingress", Namespace: tt.namespace, Name: "web", UID: "ingress-uid"}}}}

			value, err := (InspectResource{Session: s}).Call(t.Context(), `{"resource_ref":"ref"}`)
			if err == nil || value != nil || reads != tt.reads || len(s.Citations) != 0 {
				t.Fatal("invalid Ingress tool evidence accepted")
			}

			if tt.name == "result bytes" {
				var limit *LimitError

				if !errors.As(err, &limit) || limit.Kind != "local_tool_result_bytes" || limit.Limit != 8192 || limit.Observed <= limit.Limit {
					t.Fatalf("Ingress inspection exceeded the wrong limit: %v", err)
				}
			}
		})
	}
}
