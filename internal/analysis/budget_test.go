/*
Purpose: Test request and tool budgets against a nonterminating SDK tool loop.
Flow: Offline model and Kubernetes HTTP fixtures repeatedly request inspection;
batched calls verify configurable shared boundaries and safe failure details. Other
fixtures stop within independent model, tool and rolling hourly request limits.
Two native describe reads precede AI tool invocations without consuming their call allowance.
*/
package analysis

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// TestSDKBatchedToolCallBudget checks configurable shared boundaries through the real SDK.
// Successful offline batches include both tools and finish with a validated no-action decision.
func TestSDKBatchedToolCallBudget(t *testing.T) {
	for _, test := range []struct{ limit, count int }{
		{64, 9}, {64, 64}, {64, 65}, {8, 8}, {8, 9}, {1, 1}, {1, 2}, {96, 65},
	} {
		t.Run(fmt.Sprintf("%d/%d", test.count, test.limit), func(t *testing.T) {
			count := test.count
			in := inputFixture()

			var modelCalls, toolCalls atomic.Int32

			kubernetes := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				toolCalls.Add(1)
				w.Header().Set("Content-Type", "application/json")

				if r.URL.Path == "/api/v1/namespaces/apps/events" {
					fmt.Fprint(w, `{"apiVersion":"v1","kind":"EventList","metadata":{"resourceVersion":"1"},"items":[]}`)
					return
				}

				if r.URL.Path != "/api/v1/namespaces/apps/pods/bad" {
					t.Error("tool read escaped fixture scope")
				}

				fmt.Fprint(w, `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"bad","namespace":"apps","uid":"private-uid","resourceVersion":"7"},"status":{"phase":"Failed"}}`)
			}))
			defer kubernetes.Close()

			client, err := dynamic.NewForConfig(&rest.Config{Host: kubernetes.URL, QPS: 1000, Burst: 1000})
			if err != nil {
				t.Fatal(err)
			}

			model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if modelCalls.Add(1) > 1 {
					respond(w, textResponse(`{"outcome":"no_action","reason":"Expected fixture","actions":[]}`))
					return
				}

				output := make([]any, 0, count)
				for i := 0; i < count; i++ {
					call := toolResponse(in.Anchor)[0].(map[string]any)

					call["id"], call["call_id"] = fmt.Sprintf("fc%d", i), fmt.Sprintf("call%d", i)
					if i%2 != 0 {
						call["name"] = "related_warning_events"
					}

					output = append(output, call)
				}

				respond(w, output)
			}))
			defer model.Close()

			a := Analyzer{Config: config.Config{
				Endpoint: model.URL + "/openai/v1/", APIKey: "fixture", Model: "fixture",
				MaxInputChars: 20000, MaxOutputTokens: 2000, CallsPerHour: 60,
				MaxToolCalls:   test.limit,
				RequestTimeout: time.Second, InvestigationTimeout: 5 * time.Second,
			}, Kubernetes: client, DescribeConfig: &rest.Config{Host: kubernetes.URL}}
			decision, err := a.Investigate(t.Context(), in)

			if count <= test.limit {
				if err != nil || decision.Outcome != "no_action" || len(decision.Actions) != 0 ||
					int(toolCalls.Load()) != count+2 || modelCalls.Load() != 2 {
					t.Fatalf("allowed tool batch failed: %v (model=%d tools=%d)", err, modelCalls.Load(), toolCalls.Load())
				}

				return
			}

			var failure *Failure

			if !errors.As(err, &failure) || failure.Kind != "local_tool_call_limit" ||
				failure.Observed != int64(test.limit+1) || failure.Limit != int64(test.limit) ||
				failure.ToolCalls != test.limit || failure.ToolCallLimit != test.limit ||
				int(toolCalls.Load()) != test.limit+2 || modelCalls.Load() != 1 || len(decision.Actions) != 0 {
				t.Fatalf("over-budget call executed or incorrectly diagnosed: %v (model=%d tools=%d)", err, modelCalls.Load(), toolCalls.Load())
			}
		})
	}
}

// TestSDKCannotLoopPastTransportOrGlobalBudgets drives a nonterminating tool loop
// through local fixtures and checks it stops within request, tool and hourly limits.
func TestSDKCannotLoopPastTransportOrGlobalBudgets(t *testing.T) {
	for _, quota := range []int{1, 60} {
		t.Run(fmt.Sprint(quota), func(t *testing.T) {
			in := inputFixture()

			var modelCalls, toolCalls atomic.Int32

			kubernetes := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				toolCalls.Add(1)
				w.Header().Set("Content-Type", "application/json")

				if r.URL.Path == "/api/v1/namespaces/apps/events" {
					fmt.Fprint(w, `{"apiVersion":"v1","kind":"EventList","metadata":{"resourceVersion":"1"},"items":[]}`)
					return
				}

				fmt.Fprint(w, `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"bad","namespace":"apps","uid":"private-uid","resourceVersion":"7"},"status":{"phase":"Failed"}}`)
			}))
			defer kubernetes.Close()

			client, err := dynamic.NewForConfig(&rest.Config{Host: kubernetes.URL, QPS: 100, Burst: 100})
			if err != nil {
				t.Fatal(err)
			}

			model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := modelCalls.Add(1)
				output := toolResponse(in.Anchor)
				call := output[0].(map[string]any)
				call["id"], call["call_id"] = fmt.Sprintf("fc%d", n), fmt.Sprintf("call%d", n)

				respond(w, output)
			}))
			defer model.Close()

			a := Analyzer{Config: config.Config{Endpoint: model.URL + "/openai/v1/", APIKey: "fixture", Model: "fixture", MaxInputChars: 20000, MaxOutputTokens: 2000,
				MaxToolCalls: config.DefaultMaxToolCalls,
				CallsPerHour: quota, RequestTimeout: time.Second, InvestigationTimeout: 5 * time.Second},
				Kubernetes: client, DescribeConfig: &rest.Config{Host: kubernetes.URL}}

			decision, err := a.Investigate(t.Context(), in)
			if err == nil || len(decision.Actions) != 0 {
				t.Fatal("nonterminating tool loop produced incident decisions")
			}

			if modelCalls.Load() > 6 || int(toolCalls.Load())-2 > a.Config.MaxToolCalls || int(modelCalls.Load()) > quota {
				t.Fatal("real SDK bypassed configured budgets")
			}

			if quota == 1 && modelCalls.Load() != 1 {
				t.Fatal("tool follow-up bypassed hourly request accounting")
			}
		})
	}
}
