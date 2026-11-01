/*
Purpose: Test HPA inspection, evidence limits and approved target handles.
Flow: Offline HTTP fixtures verify autoscaling/v2 GET and Warning-event shapes,
numeric scaling facts, redacted messages/identifiers and selector exclusion.
Invalid scope, schema or oversized results receive no accepted citations.
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

// hpaInspectionObject supplies CPU and external metrics with synthetic selectors and sensitive identifiers.
func hpaInspectionObject() map[string]any {
	return map[string]any{"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
		"metadata": map[string]any{"name": "web", "namespace": "apps", "uid": "hpa-uid", "resourceVersion": "7", "generation": int64(2),
			"annotations": map[string]any{"private": "private-annotation"}},
		"spec": map[string]any{"scaleTargetRef": map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"},
			"maxReplicas": int64(5), "metrics": []any{
				map[string]any{"type": "Resource", "resource": map[string]any{"name": "cpu", "target": map[string]any{"type": "Utilization", "averageUtilization": int64(70)}}},
				map[string]any{"type": "External", "external": map[string]any{"metric": map[string]any{"name": "token_rate", "selector": map[string]any{"matchLabels": map[string]any{"private": "private-selector"}}},
					"target": map[string]any{"type": "Value", "value": "100"}}},
			},
			"behavior": map[string]any{"scaleDown": map[string]any{"stabilizationWindowSeconds": int64(300), "selectPolicy": "Disabled",
				"policies": []any{map[string]any{"type": "Percent", "value": int64(10), "periodSeconds": int64(60)}}}}},
		"status": map[string]any{"currentReplicas": int64(2), "desiredReplicas": int64(3), "observedGeneration": int64(2),
			"lastScaleTime":  "2026-11-01T12:00:00Z",
			"currentMetrics": []any{map[string]any{"type": "Resource", "resource": map[string]any{"name": "cpu", "current": map[string]any{"averageUtilization": int64(90)}}}},
			"conditions":     []any{map[string]any{"type": "ScalingActive", "status": "False", "reason": "FailedGetResourceMetric", "message": "password=fixture"}}},
	}
}

// TestHPAInspectionAndEvents checks approved numeric state, authorized target context and shared scoped tool contracts.
func TestHPAInspectionAndEvents(t *testing.T) {
	reads := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads++

		if r.Method != http.MethodGet {
			t.Error("HPA tools attempted a scaling write")
		}

		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/apis/autoscaling/v2/namespaces/apps/horizontalpodautoscalers/web":
			json.NewEncoder(w).Encode(hpaInspectionObject())
		case "/api/v1/namespaces/apps/events":
			if r.URL.Query().Get("fieldSelector") != "type=Warning,involvedObject.uid=hpa-uid" || r.URL.Query().Get("limit") != "20" {
				t.Error("HPA event read escaped bounded authorized scope")
			}

			fmt.Fprint(w, `{"apiVersion":"v1","kind":"EventList","metadata":{"resourceVersion":"1"},"items":[{"metadata":{"namespace":"apps"},"type":"Warning","involvedObject":{"uid":"hpa-uid","kind":"HorizontalPodAutoscaler","name":"web"},"reason":"FailedComputeMetricsReplicas","message":"token=fixture"}]}`)
		default:
			t.Error("HPA tool fetched a target, metrics API or arbitrary resource")
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	defer server.Close()

	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}

	target := health.Object{Resource: health.Resource{Kind: "Deployment", Namespace: "apps", Name: "web", UID: "target-uid"}}
	targetRef := incident.Ref(target.Resource)
	s := &Session{Client: client, Scope: config.Config{NamespaceWatch: []string{"apps"}}, MaxCalls: 4, Timeout: time.Second,
		Citations: map[string]bool{}, Investigation: incident.InvestigationContext{
			Identities: map[string]health.Resource{"ref": {Kind: "HorizontalPodAutoscaler", Namespace: "apps", Name: "web", UID: "hpa-uid"}, targetRef: target.Resource},
			Objects:    map[string]health.Object{targetRef: target}, ToolVersions: map[string]string{}}}

	value, err := (InspectResource{Session: s}).Call(t.Context(), `{"resource_ref":"ref"}`)
	if err != nil {
		t.Fatal(err)
	}

	state := value.(map[string]any)["state"].(map[string]any)
	if state["min_replicas"] != int32(1) || state["max_replicas"] != int32(5) || state["current_replicas"] != int32(2) ||
		state["desired_replicas"] != int32(3) || state["scale_target_ref"] != targetRef || state["last_scale_time"] == nil ||
		state["generation"] != int64(2) || state["observed_generation"] != int64(2) || s.Investigation.ToolVersions["ref"] != "7" {
		t.Fatalf("approved HPA inspection facts missing: %+v", state)
	}

	metrics := state["metrics"].([]map[string]any)

	current := state["current_metrics"].([]map[string]any)
	if len(metrics) != 2 || metrics[0]["average_utilization"] != int32(70) ||
		len(current) != 1 || current[0]["average_utilization"] != int32(90) || metrics[1]["name"] != "[redacted sensitive text]" ||
		state["scale_down"].(map[string]any)["stabilization_window_seconds"] != int32(300) {
		t.Fatal("metric or scaling behavior projection missing")
	}

	data, _ := json.Marshal(value)
	for _, forbidden := range []string{"private-", "hpa-uid", "target-uid", "token_rate", "password=fixture", "matchLabels", "selector"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("unapproved HPA data reached model output: %s", forbidden)
		}
	}

	events, err := (RelatedWarningEvents{Session: s}).Call(t.Context(), `{"resource_ref":"ref"}`)
	if err != nil || reads != 2 || len(s.Citations) != 2 {
		t.Fatalf("HPA Warning-event tool failed: %v", err)
	}

	data, _ = json.Marshal(events)
	if strings.Contains(string(data), "token=fixture") {
		t.Fatal("HPA event message was not redacted")
	}
}

// TestHPAToolRejectsInvalidEvidence enforces scope, incarnation, projection and tool byte bounds without success-shaped results.
func TestHPAToolRejectsInvalidEvidence(t *testing.T) {
	for _, tt := range []struct {
		name, namespace string
		edit            func(map[string]any)
		reads           int
	}{
		{name: "scope", namespace: "other", edit: func(o map[string]any) {}},
		{name: "incarnation", namespace: "apps", edit: func(o map[string]any) { o["metadata"].(map[string]any)["uid"] = "changed" }, reads: 1},
		{name: "schema", namespace: "apps", edit: func(o map[string]any) {
			o["spec"].(map[string]any)["metrics"] = []any{map[string]any{"type": "Unknown"}}
		}, reads: 1},
		{name: "result bytes", namespace: "apps", edit: func(o map[string]any) {
			conditions := make([]any, 32)
			for i := range conditions {
				conditions[i] = map[string]any{"type": "ScalingActive", "status": "False", "message": strings.Repeat("x", 1024)}
			}

			o["status"].(map[string]any)["conditions"] = conditions
		}, reads: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reads := 0

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				reads++
				o := hpaInspectionObject()
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
					Identities: map[string]health.Resource{"ref": {Kind: "HorizontalPodAutoscaler", Namespace: tt.namespace, Name: "web", UID: "hpa-uid"}}}}

			value, err := (InspectResource{Session: s}).Call(t.Context(), `{"resource_ref":"ref"}`)
			if err == nil || value != nil || reads != tt.reads || len(s.Citations) != 0 {
				t.Fatal("invalid HPA tool evidence accepted")
			}

			if tt.name == "result bytes" {
				var limit *LimitError

				if !errors.As(err, &limit) || limit.Kind != "local_tool_result_bytes" || limit.Limit != 8192 || limit.Observed <= limit.Limit {
					t.Fatalf("HPA tool result exceeded the wrong limit: %v", err)
				}
			}
		})
	}
}
