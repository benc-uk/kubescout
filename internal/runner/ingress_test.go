/*
Purpose: Test Ingress configuration incident retention and recovery gates offline.
Flow: Local collection denials and in-memory AI fixtures retain unresolved
incidents during unknown observations, model failure and no-action. Backend
repair starts continuous recovery review; stale routes/versions reject results.
No live model, cluster, Secret or external traffic endpoint is accessed.
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

// ingressForRunner supplies a stable Ingress referencing a same-namespace Service port for offline lifecycle tests.
func ingressForRunner() health.Object {
	return health.Object{Resource: health.Resource{Kind: "Ingress", Namespace: "apps", Name: "web", UID: "ingress"},
		Version: "10", Ingress: health.IngressState{Routes: []health.IngressRoute{{Default: true, Service: "web", PortNumber: 80}}}}
}

// TestIngressNoActionAndFailuresRetainIncidents preserves unresolved routing problems unless validated AI changes them.
func TestIngressNoActionAndFailuresRetainIncidents(t *testing.T) {
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
			ingress := ingressForRunner()

			source := &fakeSource{objects: []health.Object{ingress}}
			if tt.collection {
				source.err = errors.New("Ingress collection unknown")
			}

			repo := &incident.Memory{Records: map[string]incident.Record{"existing": {Value: incident.Incident{
				Identity:  incident.Identity{Anchor: ingress.Resource, Family: "service-availability"},
				Resources: []health.Resource{ingress.Resource}, Lifecycle: incident.Lifecycle{State: "open"}}}}}
			ai := &fakeAI{fail: tt.model}

			var out, diag bytes.Buffer

			r := Runner{Config: options(), Source: source, Analyzer: ai, Incidents: incident.Reconciler{Repository: repo},
				Output: &output.Writer{Out: &out, Err: &diag}}
			if r.Once(t.Context()) != tt.code || len(repo.Records) != 1 || repo.Records["existing"].Value.Lifecycle.State != "open" ||
				tt.collection && ai.calls.Load() != 0 || !tt.collection && ai.calls.Load() != 1 {
				t.Fatal("Ingress no-action/failure changed an incident or bypassed AI")
			}
		})
	}
}

// TestIngressDeniedCollectionSkipsAI rejects denied Ingress or Service lists before any absence-based finding can reach AI.
func TestIngressDeniedCollectionSkipsAI(t *testing.T) {
	for _, denied := range []string{"ingresses", "services"} {
		t.Run(denied, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Error("collection outage caused a workload write")
				}

				w.Header().Set("Content-Type", "application/json")

				if strings.HasSuffix(r.URL.Path, "/"+denied) {
					w.WriteHeader(http.StatusForbidden)
					fmt.Fprint(w, `{"apiVersion":"v1","kind":"Status","reason":"Forbidden","code":403,"message":"private-ingress-error"}`)

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
			resource := ingressForRunner().Resource
			repo := &incident.Memory{Records: map[string]incident.Record{"existing": {Value: incident.Incident{
				Identity:  incident.Identity{Anchor: resource, Family: "service-availability"},
				Resources: []health.Resource{resource}, Lifecycle: incident.Lifecycle{State: "open"}}}}}
			ai := &fakeAI{}

			var out, diag bytes.Buffer

			r := Runner{Config: cfg, Source: kube.NewSource(client, cfg), Analyzer: ai, Incidents: incident.Reconciler{Repository: repo},
				Output: &output.Writer{Out: &out, Err: &diag}}
			if r.Once(t.Context()) != 1 || ai.calls.Load() != 0 || len(repo.Records) != 1 ||
				repo.Records["existing"].Value.Lifecycle.State != "open" || strings.Contains(out.String()+diag.String(), "private-ingress-error") {
				t.Fatal("incomplete Ingress/Service collection invoked AI, leaked an API body or changed an incident")
			}
		})
	}
}

// TestIngressRecoveryFreshness requires fresh repaired backend configuration and rejects changed routes, inspected versions or renewed absence.
func TestIngressRecoveryFreshness(t *testing.T) {
	cfg := options()
	base := time.Now()
	ingress := ingressForRunner()
	service := health.Object{Resource: health.Resource{Kind: "Service", Namespace: "apps", Name: "web", UID: "service"},
		Version: "20", Ports: []health.Port{{Number: 80, Target: "8080"}}}
	open := []incident.Record{{Value: incident.Incident{Identity: incident.Identity{Anchor: ingress.Resource, Family: "service-availability"},
		Resources: []health.Resource{ingress.Resource}, Lifecycle: incident.Lifecycle{State: "open"}}}}

	contexts, err := incident.BuildInvestigationContexts(health.Snapshot{At: base, Objects: []health.Object{ingress, service}}, cfg.Policy, open, 100)
	if err != nil {
		t.Fatal(err)
	}

	key := incident.Ref(ingress.Resource)
	states := map[string]*candidate{}
	updateCandidates(states, contexts, open, base, cfg)

	if !states[key].recoverySince.Equal(base) {
		t.Fatal("fresh repaired Ingress configuration did not start recovery review")
	}

	in := contexts[key]
	in.RecoveryEligible = true
	in.ToolVersions = map[string]string{key: "10", incident.Ref(service.Resource): "20"}
	source := &fakeSource{objects: []health.Object{ingress, service}}
	r := Runner{Config: cfg, Source: source}

	if !r.current(in, open) {
		t.Fatal("fresh unchanged Ingress/backend evidence rejected")
	}

	source.err = errors.New("Ingress collection unknown")

	if r.current(in, open) {
		t.Fatal("unknown collection established Ingress recovery")
	}

	source.err = nil
	source.objects[1].Version = "21"

	if r.current(in, open) {
		t.Fatal("changed inspected backend Service version accepted")
	}

	source.objects[1].Version = "20"
	source.objects[0].Ingress.Routes = []health.IngressRoute{{Default: true, Service: "web", PortNumber: 81}}

	if r.current(in, open) {
		t.Fatal("changed Ingress route accepted as unchanged recovery")
	}

	source.objects[0] = ingress
	source.objects = source.objects[:1]

	if r.current(in, open) {
		t.Fatal("disappeared backend accepted as healthy recovery")
	}

	contexts, err = incident.BuildInvestigationContexts(health.Snapshot{At: base.Add(time.Minute), Objects: source.objects}, cfg.Policy, open, 100)
	if err != nil {
		t.Fatal(err)
	}

	updateCandidates(states, contexts, open, base.Add(time.Minute), cfg)

	if !states[key].recoverySince.IsZero() {
		t.Fatal("renewed missing backend did not reset continuous recovery")
	}
}
