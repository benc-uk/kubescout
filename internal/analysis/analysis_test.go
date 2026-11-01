/*
Purpose: Test AI decision validation, tool round trips and transport limits.
Flow: Offline local HTTP fixtures exercise actual SDK requests, projected
evidence and no-action on critical signals; malformed output, model failures
and exhausted budgets must not yield incident actions.
*/
package analysis

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// inputFixture builds critical Pod evidence with a model-visible handle and host-only identity.
func inputFixture() incident.InvestigationContext {
	r := health.Resource{Kind: "Pod", Namespace: "apps", Name: "bad", UID: "private-uid"}
	ref := incident.Ref(r)

	return incident.InvestigationContext{Anchor: ref, Reason: "new evidence", Resources: []incident.Resource{{Ref: ref, Kind: r.Kind, Namespace: r.Namespace, Name: r.Name}},
		Signals:   []incident.Signal{{ID: "signal", Rule: "pod.failed", Hint: "critical", Resource: ref, Summary: "Failed", Evidence: []string{"phase=Failed"}}},
		Incidents: []incident.Existing{}, Identities: map[string]health.Resource{ref: r}}
}

// TestDecisionValidation permits explicit no-action on critical evidence while rejecting malformed output.
func TestDecisionValidation(t *testing.T) {
	in := inputFixture()

	good := `{"outcome":"no_action","reason":"Expected test workload; no user action justified","actions":[]}`
	for _, test := range []struct {
		name, text string
		valid      bool
	}{
		{"no action on critical evidence", good, true},
		{"missing actions", `{"outcome":"no_action","reason":"test"}`, false},
		{"null actions", `{"outcome":"no_action","reason":"test","actions":null}`, false},
		{"unknown field", strings.Replace(good, `"actions":[]`, `"actions":[],"extra":1`, 1), false},
		{"duplicate", strings.Replace(good, `"reason":`, `"reason":"other","reason":`, 1), false},
		{"trailing", good + "{}", false},
		{"fenced", "```json\n" + good + "\n```", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Validate(test.text, in, map[string]bool{"signal": true, "snapshot": true})
			if (err == nil) != test.valid {
				t.Fatalf("valid=%t err=%v", test.valid, err)
			}
		})
	}
}

// respond writes a completed Responses API envelope for offline SDK fixtures.
func respond(w http.ResponseWriter, output []any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"id": "response", "object": "response", "status": "completed", "output": output})
}

// textResponse wraps fixture text in the assistant output shape consumed by the SDK.
func textResponse(text string) []any {
	return []any{map[string]any{"type": "message", "id": "message", "role": "assistant", "status": "completed",
		"content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}}}}}
}

// toolResponse builds an inspect_resource function call for the supplied fixture handle.
func toolResponse(ref string) []any {
	return []any{map[string]any{"type": "function_call", "id": "fc1", "call_id": "call1", "name": "inspect_resource", "arguments": fmt.Sprintf(`{"resource_ref":%q}`, ref)}}
}

// TestResponsesToolRoundTripAndNoAction checks stateless SDK tool history, private-field
// exclusion and acceptance of no-action using local model and Kubernetes fixtures.
func TestResponsesToolRoundTripAndNoAction(t *testing.T) {
	in := inputFixture()

	var reads, calls atomic.Int32

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads.Add(1)

		if r.Method != "GET" || r.URL.Path != "/api/v1/namespaces/apps/pods/bad" {
			t.Error("unscoped tool request")
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"bad","namespace":"apps","uid":"private-uid"},"spec":{"containers":[{"name":"bad","env":[{"name":"PASSWORD","value":"do-not-send"}]}]},"status":{"phase":"Failed","conditions":[{"type":"Ready","status":"False","message":"password=do-not-send"}]}}`)
	}))
	defer kubeServer.Close()

	kube, err := dynamic.NewForConfig(&rest.Config{Host: kubeServer.URL})
	if err != nil {
		t.Fatal(err)
	}

	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)

		var body map[string]json.RawMessage
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			t.Fatal("invalid request")
		}

		if r.URL.Path != "/openai/v1/responses" || string(body["store"]) != "false" ||
			body["previous_response_id"] != nil || body["conversation"] != nil {
			t.Errorf("unsafe Responses request: %s", body)
		}

		if strings.Contains(string(body["input"]), "private-uid") || strings.Contains(string(body["input"]), "do-not-send") {
			t.Error("private raw data reached model")
		}

		if n == 1 {
			respond(w, toolResponse(in.Anchor))
			return
		}

		if !strings.Contains(string(body["input"]), "function_call_output") || !strings.Contains(string(body["input"]), "tool-1") {
			t.Error("stateless tool result missing from second request")
		}

		respond(w, textResponse(`{"outcome":"no_action","reason":"Expected failure fixture","actions":[]}`))
	}))
	defer model.Close()

	a := Analyzer{Config: config.Config{Endpoint: model.URL + "/openai/v1/", APIKey: "fixture", Model: "fixture", MaxInputChars: 20000,
		MaxOutputTokens: 2000, RequestTimeout: time.Second, InvestigationTimeout: 5 * time.Second, CallsPerHour: 60}, Kubernetes: kube}

	d, err := a.Investigate(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}

	if d.Outcome != "no_action" || calls.Load() != 2 || reads.Load() != 1 {
		t.Fatal("unexpected investigation flow")
	}
}

// TestModelFailuresHaveNoIncidentDecision rejects failed or unsupported model output
// without retrying HTTP requests, returning actions or leaking raw error text.
func TestModelFailuresHaveNoIncidentDecision(t *testing.T) {
	for _, mode := range []string{"rate limit", "unknown tool", "malformed", "incomplete"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)

				switch mode {
				case "rate limit":
					w.WriteHeader(429)
					fmt.Fprint(w, `{"error":{"message":"private-error"}}`)
				case "unknown tool":
					respond(w, []any{map[string]any{"type": "function_call", "call_id": "call", "name": "shell", "arguments": "{}"}})
				case "malformed":
					respond(w, textResponse("not JSON"))
				case "incomplete":
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprint(w, `{"id":"r","status":"incomplete","output":[]}`)
				}
			}))
			defer server.Close()

			a := Analyzer{Config: config.Config{Endpoint: server.URL + "/openai/v1/", APIKey: "fixture", Model: "fixture", MaxInputChars: 20000, MaxOutputTokens: 1000,
				RequestTimeout: time.Second, InvestigationTimeout: time.Second, CallsPerHour: 60}}

			d, err := a.Investigate(t.Context(), inputFixture())
			if err == nil || len(d.Actions) != 0 || calls.Load() != 1 {
				t.Fatalf("failure produced decision or retried: %v", err)
			}

			if strings.Contains(err.Error(), "private-error") {
				t.Fatal("raw model error leaked")
			}
		})
	}
}

// TestTransportHardLimits checks the HTTP request cap and rolling hourly quota expiration.
func TestTransportHardLimits(t *testing.T) {
	var requests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); fmt.Fprint(w, "{}") }))
	defer server.Close()

	b := &boundedClient{client: server.Client(), admit: func(time.Time) bool { return true }, limit: 100, inputLimit: 100}
	for i := 0; i < 7; i++ {
		req, _ := http.NewRequestWithContext(context.Background(), "POST", server.URL, strings.NewReader("{}"))

		res, err := b.Do(req)
		if i < 6 && err != nil {
			t.Fatal(err)
		}

		if i == 6 && err == nil {
			t.Fatal("seventh model request allowed")
		}

		if res != nil {
			res.Body.Close()
		}
	}

	if requests.Load() != 6 {
		t.Fatal("request budget not enforced at transport")
	}

	a := Analyzer{Config: config.Config{CallsPerHour: 60}}

	now := time.Now()
	for i := 0; i < 60; i++ {
		if !a.admit(now) {
			t.Fatal("quota prematurely exhausted")
		}
	}

	if a.admit(now) {
		t.Fatal("hourly quota exceeded")
	}

	if !a.admit(now.Add(time.Hour)) {
		t.Fatal("rolling boundary did not expire")
	}
}
