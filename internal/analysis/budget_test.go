package analysis

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"k8s-ops-agent/internal/config"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

func TestSDKCannotLoopPastTransportOrGlobalBudgets(t *testing.T) {
	for _, quota := range []int{1, 60} {
		t.Run(fmt.Sprint(quota), func(t *testing.T) {
			in := inputFixture()
			var modelCalls, toolCalls atomic.Int32
			kubernetes := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				toolCalls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"bad","namespace":"apps","uid":"private-uid"},"status":{"phase":"Failed"}}`)
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
				CallsPerHour: quota, RequestTimeout: time.Second, InvestigationTimeout: 5 * time.Second}, Kubernetes: client}
			decision, err := a.Investigate(t.Context(), in)
			if err == nil || len(decision.Actions) != 0 {
				t.Fatal("nonterminating tool loop produced incident decisions")
			}
			if modelCalls.Load() > 6 || toolCalls.Load() > 8 || int(modelCalls.Load()) > quota {
				t.Fatal("real SDK bypassed configured budgets")
			}
			if quota == 1 && modelCalls.Load() != 1 {
				t.Fatal("tool follow-up bypassed hourly request accounting")
			}
		})
	}
}
