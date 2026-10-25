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

	"k8s-ops-agent/internal/config"
	"k8s-ops-agent/internal/health"
)

func fixture() health.Finding {
	return health.NewFinding("pod.container.waiting", "warning", health.Resource{Kind: "Pod", Namespace: "apps", Name: "pod", UID: "uid"}, "app", "Image pull failed", "image=registry/app:bad")
}

func goodJSON(f health.Finding) string {
	return fmt.Sprintf(`{"items":[{"finding_ids":[%q],"explanation":"Observed image pull failure; the tag may be unavailable.","suggested_checks":["Inspect the configured image tag."]}]}`, f.ID)
}

func TestStrictValidation(t *testing.T) {
	f := fixture()
	good := goodJSON(f)
	for _, test := range []struct {
		name, text string
		valid      bool
	}{
		{"valid", good, true},
		{"code fence", "```json\n" + good + "\n```", false},
		{"trailing object", good + "{}", false},
		{"empty", "", false},
		{"null", "null", false},
		{"empty items", `{"items":[]}`, false},
		{"unknown field", strings.Replace(good, `"items":`, `"extra":true,"items":`, 1), false},
		{"case mismatch", strings.Replace(good, `"items"`, `"Items"`, 1), false},
		{"duplicate key", strings.Replace(good, `"explanation":`, `"explanation":"other","explanation":`, 1), false},
		{"unknown finding", strings.ReplaceAll(good, f.ID, "unknown"), false},
		{"blank explanation", strings.Replace(good, `"Observed image pull failure; the tag may be unavailable."`, `" "`, 1), false},
		{"empty checks", strings.Replace(good, `["Inspect the configured image tag."]`, `[]`, 1), false},
		{"null check", strings.Replace(good, `["Inspect the configured image tag."]`, `[null]`, 1), false},
		{"duplicate IDs", strings.Replace(good, fmt.Sprintf("[%q]", f.ID), fmt.Sprintf("[%q,%q]", f.ID, f.ID), 1), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Validate(test.text, []health.Finding{f})
			if (err == nil) != test.valid {
				t.Fatalf("valid=%t error=%v", test.valid, err)
			}
		})
	}
	other := f
	other.ID = "second"
	if _, err := Validate(good, []health.Finding{f, other}); err == nil {
		t.Fatal("missing actionable coverage accepted")
	}
	if _, err := Evidence([]health.Finding{f}, 1); err == nil {
		t.Fatal("oversized evidence accepted")
	}
}

func TestFrameworkResponsesIntegration(t *testing.T) {
	f := fixture()
	var attempts atomic.Int32
	var mode atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		if r.URL.Path != "/openai/v1/responses" || r.Method != "POST" || r.Header.Get("Authorization") != "Bearer test-key" {
			t.Error("incorrect model request route or credentials")
		}
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if string(body["store"]) != "false" || string(body["max_output_tokens"]) != "1000" || body["previous_response_id"] != nil || body["conversation"] != nil {
			t.Errorf("unsafe request options: %s", body)
		}
		if !strings.Contains(string(body["input"]), "registry/app:bad") {
			t.Error("image evidence not supplied")
		}
		w.Header().Set("Content-Type", "application/json")
		switch mode.Load() {
		case 1:
			w.WriteHeader(429)
			fmt.Fprint(w, `{"error":{"message":"secret-server-message"}}`)
			return
		case 2:
			fmt.Fprint(w, `{"id":"response","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[]}`)
			return
		case 3:
			fmt.Fprint(w, `{"id":"response","status":"completed","output":[]}`)
			return
		case 4:
			<-r.Context().Done()
			return
		case 5:
			fmt.Fprint(w, strings.Repeat("x", 150000))
			return
		case 6:
			fmt.Fprint(w, `{"id":"response","status":"completed","output":[{"type":"function_call","call_id":"call","name":"not_allowed","arguments":"{}"}]}`)
			return
		case 7:
			fmt.Fprint(w, `{"id":"response","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"refusal","refusal":"no"}]}]}`)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"id": "response", "object": "response", "status": "completed",
			"output": []any{map[string]any{"type": "message", "id": "message", "role": "assistant", "status": "completed",
				"content": []any{map[string]any{"type": "output_text", "text": goodJSON(f), "annotations": []any{}}}}},
		})
	}))
	defer server.Close()
	a := Analyzer{Config: config.Config{Endpoint: server.URL + "/openai/v1/", APIKey: "test-key", Model: "test", MaxInputChars: 20000, MaxOutputTokens: 1000, RequestTimeout: time.Second}}
	for i, name := range []string{"valid", "rate limited no retry", "truncated", "empty", "timeout", "oversize body", "tool call refused", "refusal"} {
		t.Run(name, func(t *testing.T) {
			mode.Store(int32(i))
			before := attempts.Load()
			ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
			defer cancel()
			result := a.Analyze(ctx, []health.Finding{f})
			if i == 0 && result.Status != "completed" {
				t.Fatalf("framework response failed: %+v", result)
			}
			if i > 0 && result.Status != "failed" {
				t.Fatalf("invalid response accepted: %+v", result)
			}
			if strings.Contains(result.Reason, "secret-server-message") {
				t.Fatal("raw model error leaked")
			}
			if attempts.Load()-before != 1 {
				t.Fatal("model retried or continued")
			}
		})
	}
	before := attempts.Load()
	a.Config.MaxInputChars = 1
	if result := a.Analyze(t.Context(), []health.Finding{f}); result.Status != "failed" {
		t.Fatal(result)
	}
	if attempts.Load() != before {
		t.Fatal("oversized input made a request")
	}
}
