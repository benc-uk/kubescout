/*
Purpose: Test investigation tool scope, projection and invocation limits.
Flow: Offline Kubernetes HTTP fixtures check handles before API access,
excluded private fields and namespace-aware node event pagination; malformed,
out-of-scope or over-budget reads must not return accepted tool evidence.
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

// TestToolScopeRedactionAndInvocationBudget checks pre-read argument validation, private-field
// exclusion and the shared eight-call limit against a local Kubernetes fixture.
func TestToolScopeRedactionAndInvocationBudget(t *testing.T) {
	calls := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++

		if r.Method != "GET" || r.URL.Path != "/api/v1/namespaces/apps/pods/test" {
			t.Error("tool escaped scope")
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"test","namespace":"apps","uid":"private-uid","annotations":{"private":"do-not-send"}},"spec":{"containers":[{"env":[{"name":"PASSWORD","value":"do-not-send"}]}]},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"False","message":"Bearer private-token"}]}}`)
	}))
	defer server.Close()

	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}

	s := &Session{Client: client, Timeout: time.Second, Citations: map[string]bool{}, Investigation: incident.InvestigationContext{Identities: map[string]health.Resource{"allowed": {Kind: "Pod", Namespace: "apps", Name: "test", UID: "private-uid"}}}}

	tool := Tool{Session: s}
	for _, args := range []string{`{"resource_ref":"other"}`, `{"resource_ref":"allowed","namespace":"kube-system"}`, `{"resource_ref":"allowed"}{}`} {
		if _, err := tool.Call(t.Context(), args); err == nil {
			t.Fatal("invalid tool call accepted")
		}
	}

	if calls != 0 {
		t.Fatal("scope validation occurred after API access")
	}

	for i := 0; i < 8; i++ {
		result, err := tool.Call(t.Context(), `{"resource_ref":"allowed"}`)
		if err != nil {
			t.Fatal(err)
		}

		data, _ := json.Marshal(result)
		if strings.Contains(string(data), "private-uid") || strings.Contains(string(data), "private-token") || strings.Contains(string(data), "do-not-send") {
			t.Fatal("sensitive data escaped projection")
		}
	}

	if _, err := tool.Call(t.Context(), `{"resource_ref":"allowed"}`); err == nil || calls != 8 {
		t.Fatal("ninth tool invocation allowed")
	}
}

// TestNodeEventsRespectNamespaceSelection checks all/watch/ignore event scope and
// rejects invalid targets, inconsistent pagination and over-budget results without partial evidence.
func TestNodeEventsRespectNamespaceSelection(t *testing.T) {
	for _, tt := range []struct {
		name           string
		scope          config.Config
		mode           string
		want, requests int
		invalid        bool
	}{
		{name: "all", want: 3, requests: 2},
		{name: "ignore with paginated excluded events", scope: config.Config{NamespaceIgnore: []string{"system"}}, want: 2, requests: 2},
		{name: "watch", scope: config.Config{NamespaceWatch: []string{"apps", "payments"}}, want: 2, requests: 2},
		{name: "wrong response namespace", scope: config.Config{NamespaceWatch: []string{"apps"}}, mode: "wrong namespace", invalid: true},
		{name: "wrong target", mode: "wrong target", invalid: true},
		{name: "wrong event type", mode: "wrong type", invalid: true},
		{name: "too many events", mode: "event limit", invalid: true},
		{name: "bounded ignored pages", scope: config.Config{NamespaceIgnore: []string{"system"}}, mode: "page limit", invalid: true},
		{name: "repeated token", scope: config.Config{NamespaceIgnore: []string{"system"}}, mode: "repeated token", invalid: true},
		{name: "changing version", mode: "changing version", invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++

				if r.Method != http.MethodGet || r.URL.Query().Get("limit") != "20" ||
					r.URL.Query().Get("fieldSelector") != "type=Warning,involvedObject.kind=Node,involvedObject.name=node" {
					t.Errorf("unexpected event lookup: %s %s", r.Method, r.URL)
				}

				items := []any{}
				event := func(ns string) map[string]any {
					target, eventType := "node", "Warning"
					if tt.mode == "wrong target" {
						target = "other-node"
					}

					if tt.mode == "wrong type" {
						eventType = "Normal"
					}

					return map[string]any{"apiVersion": "v1", "kind": "Event",
						"metadata": map[string]any{"name": "event", "namespace": ns},
						"type":     eventType, "reason": ns,
						"involvedObject": map[string]any{"kind": "Node", "name": target, "uid": "node-id"}}
				}
				meta := map[string]any{"resourceVersion": "1"}

				if len(tt.scope.NamespaceWatch) != 0 {
					ns := "apps"
					if strings.Contains(r.URL.Path, "/namespaces/payments/") {
						ns = "payments"
					} else if !strings.Contains(r.URL.Path, "/namespaces/apps/") {
						t.Error("watch lookup escaped explicit namespaces")
					}

					if tt.mode == "wrong namespace" {
						ns = "system"
					}

					items = append(items, event(ns))
				} else {
					if r.URL.Path != "/api/v1/events" {
						t.Error("all/ignore lookup must use all-namespace endpoint")
					}

					if r.URL.Query().Get("continue") == "" || tt.mode == "page limit" || tt.mode == "repeated token" {
						items = append(items, event("system"))

						meta["continue"] = fmt.Sprintf("page-%d", calls)
						if tt.mode == "repeated token" {
							meta["continue"] = "same"
						}
					} else {
						items = append(items, event("apps"), event("payments"))

						if tt.mode == "changing version" {
							meta["resourceVersion"] = "2"
						}
					}

					if tt.mode == "event limit" {
						items = []any{}
						for i := 0; i < 21; i++ {
							items = append(items, event("apps"))
						}

						delete(meta, "continue")
					}
				}

				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "EventList", "metadata": meta, "items": items})
			}))
			defer server.Close()

			client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
			if err != nil {
				t.Fatal(err)
			}

			s := &Session{Client: client, Scope: tt.scope, Timeout: time.Second, Citations: map[string]bool{},
				Investigation: incident.InvestigationContext{Identities: map[string]health.Resource{"node": {Kind: "Node", Name: "node", UID: "node-id"}}}}

			result, err := (Tool{Session: s, RelatedEvents: true}).Call(t.Context(), `{"resource_ref":"node"}`)
			if (err != nil) != tt.invalid {
				t.Fatalf("unexpected result error: %v", err)
			}

			if tt.invalid {
				if calls > 20 || result != nil {
					t.Fatal("tool exceeded HTTP budget or returned a partial result")
				}

				return
			}

			events := result.(map[string]any)["events"].([]map[string]any)
			if len(events) != tt.want || calls != tt.requests {
				t.Fatalf("events=%d requests=%d", len(events), calls)
			}

			for _, entry := range events {
				if !tt.scope.WatchesNamespace(entry["reason"].(string)) {
					t.Fatal("ignored event reached the model")
				}
			}
		})
	}
}

// TestToolsRejectIgnoredOrUnwatchedHandlesBeforeAPIRead checks both tools reject
// out-of-scope handles without HTTP requests.
func TestToolsRejectIgnoredOrUnwatchedHandlesBeforeAPIRead(t *testing.T) {
	for _, scope := range []config.Config{{NamespaceWatch: []string{"other"}}, {NamespaceIgnore: []string{"apps"}}} {
		calls := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls++

			w.WriteHeader(http.StatusInternalServerError)
		}))

		client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
		if err != nil {
			server.Close()
			t.Fatal(err)
		}

		s := &Session{Client: client, Scope: scope, Timeout: time.Second,
			Investigation: incident.InvestigationContext{Identities: map[string]health.Resource{"pod": {Kind: "Pod", Namespace: "apps", Name: "pod", UID: "uid"}}}}
		for _, related := range []bool{false, true} {
			if _, err := (Tool{Session: s, RelatedEvents: related}).Call(t.Context(), `{"resource_ref":"pod"}`); err == nil {
				t.Fatal("out-of-scope handle accepted")
			}
		}

		server.Close()

		if calls != 0 {
			t.Fatal("out-of-scope validation happened after API access")
		}
	}
}
