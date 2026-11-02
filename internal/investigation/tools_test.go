/*
Purpose: Test separate investigation tool contracts, scope and shared budgets.
Flow: Offline Kubernetes HTTP fixtures check handles before API access,
operation-named citations, version tracking, private-field exclusion, cross-tool
call and byte limits and node event pagination; typed budget failures must not
return accepted tool evidence. Operational keywords remain visible while credential values do not.
*/
package investigation

import (
	"context"
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

type testTool interface {
	Name() string
	Description() string
	Schema() any
	ReturnSchema() any
	Call(context.Context, string) (any, error)
}

// TestToolContracts verifies both concrete tools preserve the model-facing names and schemas.
func TestToolContracts(t *testing.T) {
	for _, test := range []struct {
		tool              testTool
		name, description string
	}{
		{
			tool: InspectResource{}, name: "inspect_resource",
			description: "Read approved current state and credential-protected condition messages for one supplied resource handle; never raw objects.",
		},
		{
			tool: RelatedWarningEvents{}, name: "related_warning_events",
			description: "Read bounded, credential-protected Warning events for one authorised resource handle.",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.tool.Schema())
			if err != nil {
				t.Fatal(err)
			}

			const schema = `{"additionalProperties":false,"properties":{"resource_ref":{"type":"string"}},"required":["resource_ref"],"type":"object"}`

			if test.tool.Name() != test.name || test.tool.Description() != test.description ||
				string(data) != schema || test.tool.ReturnSchema() != nil {
				t.Fatal("model-facing tool contract changed")
			}
		})
	}
}

// TestOperationalTextPreserved verifies shared projections retain keywords, identifiers and credential context.
func TestOperationalTextPreserved(t *testing.T) {
	input := map[string]any{
		"name": "token_rate",
		"path": "/secrets/token-refresh",
		"messages": []any{
			"FailedToRetrieveImagePullSecret: secret app-registry unavailable",
			"authorization denied for worker-1 at 10.20.30.40; token refresh failed",
			"Attach failed: password=fixture-password; volume=data",
		},
	}
	want := map[string]any{
		"name": "token_rate",
		"path": "/secrets/token-refresh",
		"messages": []any{
			"FailedToRetrieveImagePullSecret: secret app-registry unavailable",
			"authorization denied for worker-1 at 10.20.30.40; token refresh failed",
			"Attach failed: password=[redacted credential]; volume=data",
		},
	}

	gotJSON, err := json.Marshal(redactValue(input))
	if err != nil {
		t.Fatal(err)
	}

	wantJSON, err := json.Marshal(want)
	if err != nil || string(gotJSON) != string(wantJSON) {
		t.Fatalf("operational content changed: %s", gotJSON)
	}
}

// TestToolScopeRedactionAndInvocationBudget checks pre-read argument validation, private-field
// exclusion, unique inspection citations and the shared 64-call limit against a local Kubernetes fixture.
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

	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}

	s := &Session{Client: client, MaxCalls: config.DefaultMaxToolCalls, Timeout: time.Second, Citations: map[string]bool{}, Investigation: incident.InvestigationContext{Identities: map[string]health.Resource{"allowed": {Kind: "Pod", Namespace: "apps", Name: "test", UID: "private-uid"}}}}

	tool := InspectResource{Session: s}
	for _, args := range []string{`{"resource_ref":"other"}`, `{"resource_ref":"allowed","namespace":"kube-system"}`, `{"resource_ref":"allowed"}{}`} {
		if _, err := tool.Call(t.Context(), args); err == nil {
			t.Fatal("invalid tool call accepted")
		}
	}

	if calls != 0 || len(s.Citations) != 0 {
		t.Fatal("invalid arguments reached the API or registered evidence")
	}

	for i := 0; i < s.MaxCalls; i++ {
		result, err := tool.Call(t.Context(), `{"resource_ref":"allowed"}`)
		if err != nil {
			t.Fatal(err)
		}

		expected := fmt.Sprintf("inspect-resource-%d", i+1)

		evidence, ok := result.(map[string]any)
		if !ok || evidence["evidence_id"] != expected || !s.Citations[expected] || len(s.Citations) != i+1 {
			t.Fatal("inspection citation is not descriptive, unique and registered")
		}

		data, _ := json.Marshal(result)
		if strings.Contains(string(data), "private-uid") || strings.Contains(string(data), "private-token") || strings.Contains(string(data), "do-not-send") {
			t.Fatal("sensitive data escaped projection")
		}
	}

	_, err = tool.Call(t.Context(), `{"resource_ref":"allowed"}`)

	var limit *LimitError

	if !errors.As(err, &limit) || limit.Kind != "local_tool_call_limit" ||
		limit.Observed != 65 || limit.Limit != 64 || calls != 64 || s.Calls != 64 {
		t.Fatalf("65th tool invocation allowed or incorrectly diagnosed: %v", err)
	}
}

// TestToolByteAndArgumentLimitsRemainIndependent checks unchanged budgets below the 64-call cap.
// Rejected offline reads register no citations and expose only numeric limit details.
func TestToolByteAndArgumentLimitsRemainIndependent(t *testing.T) {
	for _, test := range []struct {
		name, kind string
		large      bool
		bytes      int
		args       string
		limit      int
		reads      int
	}{
		{name: "arguments", kind: "local_tool_arguments_limit", args: strings.Repeat("x", 1025), limit: 1024},
		{name: "result", kind: "local_tool_result_bytes", large: true, args: `{"resource_ref":"allowed"}`, limit: 8192, reads: 1},
		{name: "cumulative output", kind: "local_cumulative_tool_bytes", bytes: 32768, args: `{"resource_ref":"allowed"}`, limit: 32768, reads: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			reads := 0

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reads++
				conditions := []any{}

				if test.large {
					for i := 0; i < 100; i++ {
						conditions = append(conditions, map[string]any{"type": "Ready", "status": "False", "message": strings.Repeat("x", 100)})
					}
				}

				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{
					"apiVersion": "v1", "kind": "Pod",
					"metadata": map[string]any{"name": "test", "namespace": "apps", "uid": "private-uid"},
					"status":   map[string]any{"phase": "Running", "conditions": conditions},
				})
			}))
			defer server.Close()

			client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}

			s := &Session{
				Client: client, MaxCalls: config.DefaultMaxToolCalls, Timeout: time.Second, Bytes: test.bytes, Citations: map[string]bool{},
				Investigation: incident.InvestigationContext{Identities: map[string]health.Resource{
					"allowed": {Kind: "Pod", Namespace: "apps", Name: "test", UID: "private-uid"},
				}},
			}
			result, err := (InspectResource{Session: s}).Call(t.Context(), test.args)

			var limit *LimitError

			if !errors.As(err, &limit) || limit.Kind != test.kind || limit.Limit != test.limit || limit.Observed <= limit.Limit ||
				result != nil || reads != test.reads || s.Bytes != test.bytes || len(s.Citations) != 0 {
				t.Fatalf("independent tool budget changed or returned evidence: %v", err)
			}
		})
	}
}

// TestToolsShareSessionBudgets verifies alternating tools share counters, bytes and citations.
// Offline reads rejected by either shared limit must return no accepted evidence.
func TestToolsShareSessionBudgets(t *testing.T) {
	for _, kind := range []string{"local_tool_call_limit", "local_cumulative_tool_bytes"} {
		t.Run(kind, func(t *testing.T) {
			reads := 0

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reads++

				w.Header().Set("Content-Type", "application/json")

				switch r.URL.Path {
				case "/api/v1/namespaces/apps/pods/test":
					fmt.Fprint(w, `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"test","namespace":"apps","uid":"private-uid","resourceVersion":"7"},"status":{"phase":"Running"}}`)
				case "/api/v1/namespaces/apps/events":
					if r.URL.Query().Get("fieldSelector") != "type=Warning,involvedObject.uid=private-uid" ||
						r.URL.Query().Get("limit") != "20" {
						t.Error("event lookup escaped the authorized handle or pagination limit")
					}

					fmt.Fprint(w, `{"apiVersion":"v1","kind":"EventList","metadata":{"resourceVersion":"1"},"items":[{"metadata":{"name":"warning","namespace":"apps"},"type":"Warning","involvedObject":{"uid":"private-uid"},"reason":"BackOff","message":"password=do-not-send"}]}`)
				default:
					t.Errorf("unexpected tool read: %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()

			client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}

			s := &Session{
				Client: client, MaxCalls: 2, Timeout: time.Second, Citations: map[string]bool{},
				Investigation: incident.InvestigationContext{
					Identities: map[string]health.Resource{
						"allowed": {Kind: "Pod", Namespace: "apps", Name: "test", UID: "private-uid"},
					},
					ToolVersions: map[string]string{},
				},
			}
			tools := []testTool{InspectResource{Session: s}, RelatedWarningEvents{Session: s}}
			totalBytes := 0

			for i, tool := range tools {
				result, err := tool.Call(t.Context(), `{"resource_ref":"allowed"}`)
				if err != nil {
					t.Fatal(err)
				}

				data, err := json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}

				totalBytes += len(data)
				expected := fmt.Sprintf("%s-%d", strings.ReplaceAll(tool.Name(), "_", "-"), i+1)
				evidence, ok := result.(map[string]any)

				if !ok || evidence["evidence_id"] != expected || evidence["resource_ref"] != "allowed" ||
					evidence["complete"] != true || evidence["observed_at"] == nil ||
					!s.Citations[expected] || len(s.Citations) != i+1 ||
					s.Calls != i+1 || reads != i+1 || s.Bytes != totalBytes {
					t.Fatal("tools did not share counters, bytes, citations or the result envelope")
				}

				if strings.Contains(string(data), "private-uid") || strings.Contains(string(data), "do-not-send") {
					t.Fatal("tool result leaked private identity or sensitive text")
				}

				if i == 0 {
					if evidence["exists"] != true || evidence["state"] == nil || evidence["events"] != nil ||
						s.Investigation.ToolVersions["allowed"] != "7" {
						t.Fatal("inspection result shape or version tracking changed")
					}
				} else {
					events, ok := evidence["events"].([]map[string]any)
					if !ok || len(events) != 1 || !strings.Contains(fmt.Sprint(events[0]["message"]), "[redacted credential]") ||
						evidence["exists"] != nil || evidence["state"] != nil {
						t.Fatal("Warning event result shape or redaction changed")
					}
				}
			}

			if kind == "local_cumulative_tool_bytes" {
				s.MaxCalls = config.DefaultMaxToolCalls
				s.Bytes = 32768
			}

			bytesBefore := s.Bytes

			for i, tool := range tools {
				result, err := tool.Call(t.Context(), `{"resource_ref":"allowed"}`)

				var limit *LimitError

				wantCalls, wantLimit := 2, 2
				if kind == "local_cumulative_tool_bytes" {
					wantCalls, wantLimit = 3+i, 32768
				}

				if !errors.As(err, &limit) || limit.Kind != kind || limit.Limit != wantLimit ||
					limit.Observed <= limit.Limit || result != nil || len(s.Citations) != 2 ||
					s.Bytes != bytesBefore || s.Calls != wantCalls || reads != wantCalls {
					t.Fatalf("shared budget changed or returned accepted evidence: %v", err)
				}
			}
		})
	}
}

// TestNodeEventsRespectNamespaceSelection checks all/watch/ignore event scope and
// verifies Warning-event citations and rejects invalid or over-budget reads without partial evidence.
func TestNodeEventsRespectNamespaceSelection(t *testing.T) {
	for _, tt := range []struct {
		name           string
		limitKind      string
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
		{name: "too many events", mode: "event limit", invalid: true, limitKind: "local_tool_event_result_limit"},
		{name: "bounded ignored pages", scope: config.Config{NamespaceIgnore: []string{"system"}}, mode: "page limit", invalid: true, limitKind: "local_tool_event_request_limit"},
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

			s := &Session{Client: client, MaxCalls: config.DefaultMaxToolCalls, Scope: tt.scope, Timeout: time.Second, Citations: map[string]bool{},
				Investigation: incident.InvestigationContext{Identities: map[string]health.Resource{"node": {Kind: "Node", Name: "node", UID: "node-id"}}}}

			result, err := (RelatedWarningEvents{Session: s}).Call(t.Context(), `{"resource_ref":"node"}`)
			if (err != nil) != tt.invalid {
				t.Fatalf("unexpected result error: %v", err)
			}

			if tt.invalid {
				if calls > 20 || result != nil || len(s.Citations) != 0 {
					t.Fatal("tool exceeded HTTP budget or returned accepted partial evidence")
				}

				var limit *LimitError

				if tt.limitKind != "" && (!errors.As(err, &limit) || limit.Kind != tt.limitKind || limit.Observed != 21 || limit.Limit != 20) {
					t.Fatalf("event limit incorrectly diagnosed: %v", err)
				}

				return
			}

			evidence := result.(map[string]any)
			if evidence["evidence_id"] != "related-warning-events-1" || !s.Citations["related-warning-events-1"] || len(s.Citations) != 1 {
				t.Fatal("Warning-event citation is not descriptive and registered")
			}

			events := evidence["events"].([]map[string]any)
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

		s := &Session{Client: client, MaxCalls: config.DefaultMaxToolCalls, Scope: scope, Timeout: time.Second,
			Investigation: incident.InvestigationContext{Identities: map[string]health.Resource{"pod": {Kind: "Pod", Namespace: "apps", Name: "pod", UID: "uid"}}}}
		for _, tool := range []testTool{InspectResource{Session: s}, RelatedWarningEvents{Session: s}} {
			if _, err := tool.Call(t.Context(), `{"resource_ref":"pod"}`); err == nil {
				t.Fatal("out-of-scope handle accepted")
			}
		}

		server.Close()

		if calls != 0 {
			t.Fatal("out-of-scope validation happened after API access")
		}
	}
}
