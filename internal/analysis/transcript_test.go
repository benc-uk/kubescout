/*
Purpose: Verify AI capture at actual SDK request and tool boundaries.
Flow: Offline HTTP fixtures compare enabled, disabled and failed file capture,
including multiple model rounds, invalid decisions, quotas and credential exclusion.
Transcripts must not change model attempts or create incident decisions on failure.
*/
package analysis

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/output"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// TestSDKTranscriptRoundTrips proves every real request and tool exchange is captured without extra HTTP calls.
func TestSDKTranscriptRoundTrips(t *testing.T) {
	for _, mode := range []string{"enabled", "disabled", "unwritable"} {
		t.Run(mode, func(t *testing.T) {
			in := inputFixture()

			var calls, reads atomic.Int32

			kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				reads.Add(1)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"bad","namespace":"apps","uid":"private-uid"},"status":{"phase":"Failed"}}`)
			}))
			defer kubeServer.Close()

			client, err := dynamic.NewForConfig(&rest.Config{Host: kubeServer.URL})
			if err != nil {
				t.Fatal(err)
			}

			model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) == 1 {
					respond(w, toolResponse(in.Anchor))
				} else {
					respond(w, textResponse(`{"outcome":"no_action","reason":"Expected test workload; token budget unchanged","actions":[]}`))
				}
			}))
			defer model.Close()

			var console, diagnostics bytes.Buffer

			log := &output.Writer{Out: &console, Err: &diagnostics, Run: "fixture"}
			dir := t.TempDir()
			a := Analyzer{Config: config.Config{Endpoint: model.URL + "/openai/v1/", APIKey: "fixture-secret", Model: "fixture-model",
				MaxInputChars: 20000, MaxOutputTokens: 2000, MaxToolCalls: 64, CallsPerHour: 60, RequestTimeout: time.Second, InvestigationTimeout: 5 * time.Second},
				Kubernetes: client, Log: log}

			if mode != "disabled" {
				if mode == "unwritable" {
					dir = filepath.Join(dir, "not-a-directory")
					if err := os.WriteFile(dir, []byte("keep"), 0600); err != nil {
						t.Fatal(err)
					}
				}

				a.Transcript = output.NewTranscript(dir, log)
			}

			ctx := output.WithInvestigation(t.Context(), "42", "Pod apps/bad")

			decision, err := a.Investigate(ctx, in)
			if err != nil || decision.Outcome != "no_action" || calls.Load() != 2 || reads.Load() != 1 {
				t.Fatalf("capture changed execution: %v calls=%d reads=%d", err, calls.Load(), reads.Load())
			}

			if mode == "unwritable" {
				if !strings.Contains(diagnostics.String(), "[GO][TRANSCRIPT]") {
					t.Fatal("capture failure hidden")
				}

				return
			}

			if mode == "disabled" {
				entries, err := os.ReadDir(dir)
				if err != nil || len(entries) != 0 {
					t.Fatal("disabled capture wrote files")
				}

				return
			}

			data, err := os.ReadFile(filepath.Join(dir, "ai-transcript.log"))
			if err != nil {
				t.Fatal(err)
			}

			text := string(data)
			last := -1

			for _, phase := range []string{"[GO][INVESTIGATION.START]", "[GO][CONTEXT]", "[GO][REQUEST.1]", "[AI][RESPONSE.1]",
				"[AI][TOOL.REQUEST]", "[GO][TOOL.RESULT]", "[GO][REQUEST.2]", "[AI][RESPONSE.2]", "[AI][FINAL.ANSWER]", "[GO][INVESTIGATION.END]"} {
				index := strings.Index(text, phase)
				if index <= last || strings.Count(text, phase) != 1 {
					t.Fatalf("missing, duplicate or reordered %s", phase)
				}

				last = index
			}

			for _, want := range []string{"inv=42", "Instructions:", `"instructions"`, `"function_call_output"`, "inspect-resource-1", "token budget unchanged", "Validated decision: no_action"} {
				if !strings.Contains(text, want) {
					t.Fatalf("transcript missing %s", want)
				}
			}

			for _, forbidden := range []string{"fixture-secret", "private-uid", "Authorization", `\"outcome\"`} {
				if strings.Contains(text, forbidden) {
					t.Fatalf("unsafe or unreadable transcript: %s", forbidden)
				}
			}

			if !strings.Contains(console.String(), "[AI][TOOL.REQUEST]") || !strings.Contains(console.String(), "[GO][TOOL.EXECUTE]") {
				t.Fatal("tool choice/execution provenance missing")
			}
		})
	}
}

// TestTranscriptArgumentProjection rejects unauthorized and duplicate-key tool arguments without logging their raw contents.
func TestTranscriptArgumentProjection(t *testing.T) {
	in := inputFixture()

	for _, args := range []string{
		`{"resource_ref":"private-unknown-handle"}`,
		`{"resource_ref":"` + in.Anchor + `","extra":"private-extra"}`,
		`{"resource_ref":"` + in.Anchor + `","resource_ref":"private-duplicate"}`,
	} {
		body := fmt.Sprintf(`{"type":"function_call","name":"inspect_resource","arguments":%q}`, args)
		text := transcriptPayload([]byte(body), "fixture-secret", in)

		if !strings.Contains(text, "unvalidated tool arguments") || strings.Contains(text, "private-") {
			t.Fatal("unvalidated argument content reached transcript")
		}
	}

	body := fmt.Sprintf(`{"type":"function_call","name":"inspect_resource","arguments":%q}`, `{"resource_ref":"`+in.Anchor+`"}`)
	if text := transcriptPayload([]byte(body), "", in); !strings.Contains(text, in.Anchor) || strings.Contains(text, `\"resource_ref\"`) {
		t.Fatal("authorized arguments not normalized into readable fields")
	}
}

// TestTranscriptRejectedResponses checks failed validation and admission remain visible without leaking provider bodies.
func TestTranscriptRejectedResponses(t *testing.T) {
	for _, mode := range []string{"invalid decision", "provider failure", "quota"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)

				if mode == "provider failure" {
					w.WriteHeader(429)
					fmt.Fprint(w, "private-provider-body")

					return
				}

				respond(w, textResponse("invalid-decision-marker password=private-value"))
			}))
			defer server.Close()

			dir := t.TempDir()

			a := Analyzer{Config: config.Config{Endpoint: server.URL + "/openai/v1/", APIKey: "fixture-secret", Model: "fixture",
				MaxInputChars: 20000, MaxOutputTokens: 2000, MaxToolCalls: 64, CallsPerHour: 60, RequestTimeout: time.Second, InvestigationTimeout: time.Second},
				Transcript: output.NewTranscript(dir, nil)}
			if mode == "quota" {
				a.Config.CallsPerHour = 0
			}

			_, err := a.Investigate(t.Context(), inputFixture())
			if err == nil {
				t.Fatal("invalid response yielded a decision")
			}

			data, err := os.ReadFile(filepath.Join(dir, "ai-transcript.log"))
			if err != nil {
				t.Fatal(err)
			}

			text := string(data)
			if !strings.Contains(text, "rejected/failed") || strings.Contains(text, "private-provider-body") || strings.Contains(text, "private-value") {
				t.Fatal("missing outcome or leaked private content")
			}

			if mode == "invalid decision" && !strings.Contains(text, "invalid-decision-marker") {
				t.Fatal("rejected model output disappeared")
			}

			if mode == "quota" && (calls.Load() != 0 || !strings.Contains(text, "sent=false") || strings.Contains(text, "[GO][REQUEST.1]")) {
				t.Fatal("admission rejection claimed a sent request")
			}
		})
	}
}
