/*
Purpose: Test bounded HPA projection and autoscaling/v2 collection offline.
Flow: Typed-projection and HTTP list/watch fixtures cover all five metric types,
selector exclusion, scaling behavior, namespace filtering, shared budgets and
permission failures. No metrics API or live Kubernetes resource is accessed.
*/
package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// hpaObject supplies all supported metric shapes, behavior and private selector/message sentinels.
func hpaObject(namespace string) map[string]any {
	identifier := map[string]any{"name": "request_rate", "selector": map[string]any{"matchLabels": map[string]any{"private": "private-selector"}}}
	object := map[string]any{"apiVersion": "networking.k8s.io/v1", "kind": "Ingress", "name": "gateway"}
	metrics := []any{
		map[string]any{"type": "Resource", "resource": map[string]any{"name": "cpu", "target": map[string]any{"type": "Utilization", "averageUtilization": int64(70)}}},
		map[string]any{"type": "ContainerResource", "containerResource": map[string]any{"name": "memory", "container": "app", "target": map[string]any{"type": "AverageValue", "averageValue": "64Mi"}}},
		map[string]any{"type": "Pods", "pods": map[string]any{"metric": identifier, "target": map[string]any{"type": "AverageValue", "averageValue": "10"}}},
		map[string]any{"type": "External", "external": map[string]any{"metric": identifier, "target": map[string]any{"type": "Value", "value": "100"}}},
		map[string]any{"type": "Object", "object": map[string]any{"metric": identifier, "describedObject": object, "target": map[string]any{"type": "Value", "value": "100"}}},
	}
	current := []any{
		map[string]any{"type": "Resource", "resource": map[string]any{"name": "cpu", "current": map[string]any{"averageUtilization": int64(90), "averageValue": "900m"}}},
		map[string]any{"type": "ContainerResource", "containerResource": map[string]any{"name": "memory", "container": "app", "current": map[string]any{"averageValue": "80Mi"}}},
		map[string]any{"type": "Pods", "pods": map[string]any{"metric": identifier, "current": map[string]any{"averageValue": "12"}}},
		map[string]any{"type": "External", "external": map[string]any{"metric": identifier, "current": map[string]any{"value": "110"}}},
		map[string]any{"type": "Object", "object": map[string]any{"metric": identifier, "describedObject": object, "current": map[string]any{"value": "110"}}},
	}

	return map[string]any{"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
		"metadata": map[string]any{"name": "web", "namespace": namespace, "uid": namespace + "-hpa", "resourceVersion": "1", "generation": int64(2),
			"annotations": map[string]any{"private": "private-annotation"}},
		"spec": map[string]any{"scaleTargetRef": map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"}, "maxReplicas": int64(10), "metrics": metrics,
			"behavior": map[string]any{"scaleDown": map[string]any{"stabilizationWindowSeconds": int64(300), "selectPolicy": "Max", "tolerance": "0.1",
				"policies": []any{map[string]any{"type": "Percent", "value": int64(10), "periodSeconds": int64(60)}}}}},
		"status": map[string]any{"currentReplicas": int64(2), "desiredReplicas": int64(3), "observedGeneration": int64(2), "lastScaleTime": "2026-11-01T12:00:00Z",
			"currentMetrics": current, "conditions": []any{map[string]any{"type": "ScalingActive", "status": "False", "reason": "FailedGetResourceMetric", "message": "private-message"}}},
	}
}

// TestHPAProjection checks all metric types, optional defaults and deep-copy ownership without retaining selectors.
func TestHPAProjection(t *testing.T) {
	o, err := project(&unstructured.Unstructured{Object: hpaObject("apps")}, "HorizontalPodAutoscaler", "apps")
	if err != nil {
		t.Fatal(err)
	}

	h := o.Value.HPA
	if h.Min != 1 || h.Max != 10 || h.Current != 2 || h.Desired != 3 || h.TargetVersion != "apps/v1" ||
		h.TargetKind != "Deployment" || h.TargetName != "web" || len(h.Metrics) != 5 || len(h.CurrentMetrics) != 5 ||
		h.ObservedGeneration == nil || *h.ObservedGeneration != 2 || h.LastScale == nil || h.ScaleDown == nil ||
		h.ScaleDown.Stabilization == nil || *h.ScaleDown.Stabilization != 300 || h.ScaleDown.Tolerance != "100m" {
		t.Fatalf("approved HPA facts missing: %+v", h)
	}

	for i, kind := range []string{"Resource", "ContainerResource", "Pods", "External", "Object"} {
		if h.Metrics[i].Type != kind || h.CurrentMetrics[i].Type != kind {
			t.Fatal("HPA metric source or observation missing")
		}
	}

	if h.Metrics[0].Utilization == nil || *h.Metrics[0].Utilization != 70 || h.CurrentMetrics[0].Utilization == nil ||
		*h.CurrentMetrics[0].Utilization != 90 || h.Metrics[1].Container != "app" || h.Metrics[1].AverageValue != "64Mi" ||
		h.Metrics[3].Value != "100" || h.Metrics[4].ObjectKind != "Ingress" {
		t.Fatal("numeric metric or described-object projection missing")
	}

	data, _ := json.Marshal(o)
	if strings.Contains(string(data), "private-") || strings.Contains(string(data), "matchLabels") {
		t.Fatal("HPA selectors or raw messages entered cache projection")
	}

	copy := o.DeepCopyObject().(*cachedObject)
	*copy.Value.HPA.Metrics[0].Utilization = 1
	*copy.Value.HPA.CurrentMetrics[0].Utilization = 1
	*copy.Value.HPA.ObservedGeneration = 1
	*copy.Value.HPA.LastScale = time.Time{}
	*copy.Value.HPA.ScaleDown.Stabilization = 1
	copy.Value.HPA.ScaleDown.Policies[0].Value = 1

	if *o.Value.HPA.Metrics[0].Utilization != 70 || *o.Value.HPA.CurrentMetrics[0].Utilization != 90 || *o.Value.HPA.ObservedGeneration != 2 ||
		o.Value.HPA.LastScale.IsZero() || *o.Value.HPA.ScaleDown.Stabilization != 300 || o.Value.HPA.ScaleDown.Policies[0].Value != 10 {
		t.Fatal("HPA cache copy aliased numeric facts or policy lists")
	}

	raw := hpaObject("apps")
	raw["spec"].(map[string]any)["minReplicas"] = int64(0)
	delete(raw["spec"].(map[string]any), "behavior")
	delete(raw["status"].(map[string]any), "lastScaleTime")
	delete(raw["status"].(map[string]any), "observedGeneration")

	o, err = project(&unstructured.Unstructured{Object: raw}, "HorizontalPodAutoscaler", "apps")
	if err != nil || o.Value.HPA.Min != 0 || o.Value.HPA.ScaleDown != nil || o.Value.HPA.LastScale != nil || o.Value.HPA.ObservedGeneration != nil {
		t.Fatal("explicit zero minimum or absent HPA fields were replaced by invented defaults")
	}
}

// TestHPAProjectionBounds verifies exact list/name bounds and rejects unknown or incomplete metric schemas.
func TestHPAProjectionBounds(t *testing.T) {
	repeat := func(value any, n int) []any {
		out := make([]any, n)
		for i := range out {
			out[i] = value
		}

		return out
	}

	for _, tt := range []struct {
		name    string
		edit    func(map[string]any)
		invalid bool
	}{
		{"32 targets", func(o map[string]any) {
			spec := o["spec"].(map[string]any)
			spec["metrics"] = repeat(spec["metrics"].([]any)[0], 32)
		}, false},
		{"33 targets", func(o map[string]any) {
			spec := o["spec"].(map[string]any)
			spec["metrics"] = repeat(spec["metrics"].([]any)[0], 33)
		}, true},
		{"32 observations", func(o map[string]any) {
			status := o["status"].(map[string]any)
			status["currentMetrics"] = repeat(status["currentMetrics"].([]any)[0], 32)
		}, false},
		{"33 observations", func(o map[string]any) {
			status := o["status"].(map[string]any)
			status["currentMetrics"] = repeat(status["currentMetrics"].([]any)[0], 33)
		}, true},
		{"32 conditions", func(o map[string]any) {
			status := o["status"].(map[string]any)
			status["conditions"] = repeat(status["conditions"].([]any)[0], 32)
		}, false},
		{"33 conditions", func(o map[string]any) {
			status := o["status"].(map[string]any)
			status["conditions"] = repeat(status["conditions"].([]any)[0], 33)
		}, true},
		{"16 policies", func(o map[string]any) {
			rules := o["spec"].(map[string]any)["behavior"].(map[string]any)["scaleDown"].(map[string]any)
			rules["policies"] = repeat(rules["policies"].([]any)[0], 16)
		}, false},
		{"17 policies", func(o map[string]any) {
			rules := o["spec"].(map[string]any)["behavior"].(map[string]any)["scaleDown"].(map[string]any)
			rules["policies"] = repeat(rules["policies"].([]any)[0], 17)
		}, true},
		{"256 target name", func(o map[string]any) {
			o["spec"].(map[string]any)["scaleTargetRef"].(map[string]any)["name"] = strings.Repeat("x", 256)
		}, false},
		{"257 target name", func(o map[string]any) {
			o["spec"].(map[string]any)["scaleTargetRef"].(map[string]any)["name"] = strings.Repeat("x", 257)
		}, true},
		{"256 metric name", func(o map[string]any) {
			o["spec"].(map[string]any)["metrics"].([]any)[3].(map[string]any)["external"].(map[string]any)["metric"].(map[string]any)["name"] = strings.Repeat("x", 256)
		}, false},
		{"257 metric name", func(o map[string]any) {
			o["spec"].(map[string]any)["metrics"].([]any)[3].(map[string]any)["external"].(map[string]any)["metric"].(map[string]any)["name"] = strings.Repeat("x", 257)
		}, true},
		{"unknown source", func(o map[string]any) {
			o["spec"].(map[string]any)["metrics"] = []any{map[string]any{"type": "Unknown"}}
		}, true},
		{"missing target value", func(o map[string]any) {
			o["spec"].(map[string]any)["metrics"].([]any)[0].(map[string]any)["resource"].(map[string]any)["target"] = map[string]any{"type": "Utilization"}
		}, true},
		{"unknown observation", func(o map[string]any) {
			o["status"].(map[string]any)["currentMetrics"] = []any{map[string]any{"type": "Unknown"}}
		}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw := hpaObject("apps")
			tt.edit(raw)

			if _, err := project(&unstructured.Unstructured{Object: raw}, "HorizontalPodAutoscaler", "apps"); (err != nil) != tt.invalid {
				t.Fatalf("unexpected HPA projection boundary: %v", err)
			}
		})
	}
}

// TestHPACollectionScopeAndFailures checks autoscaling/v2 requests, filtering before projection and incomplete denied reads.
func TestHPACollectionScopeAndFailures(t *testing.T) {
	for _, tt := range []struct {
		name      string
		scope     config.Config
		max, want int
		denied    bool
	}{
		{name: "watch", scope: config.Config{NamespaceWatch: []string{"apps"}}, max: 2, want: 2},
		{name: "ignore", scope: config.Config{NamespaceIgnore: []string{"system"}}, max: 2, want: 2},
		{name: "all", max: 3, want: 3},
		{name: "shared budget", max: 2},
		{name: "denied", max: 3, denied: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Query().Get("limit") != "100" {
					t.Error("HPA collection escaped bounded read-only requests")
				}

				w.Header().Set("Content-Type", "application/json")

				items := []any{}

				if strings.HasSuffix(r.URL.Path, "/nodes") {
					items = append(items, node())
				}

				if strings.HasSuffix(r.URL.Path, "/horizontalpodautoscalers") {
					if tt.denied {
						w.WriteHeader(http.StatusForbidden)
						fmt.Fprint(w, `{"apiVersion":"v1","kind":"Status","reason":"Forbidden","code":403,"message":"private-hpa-error"}`)

						return
					}

					items = append(items, hpaObject("apps"))

					if len(tt.scope.NamespaceWatch) == 0 {
						if r.URL.Path != "/apis/autoscaling/v2/horizontalpodautoscalers" {
							t.Error("all/ignore HPA requests were not all-namespace v2 reads")
						}

						ignored := hpaObject("system")
						if len(tt.scope.NamespaceIgnore) != 0 {
							ignored["spec"].(map[string]any)["metrics"] = "invalid-must-not-project"
						}

						items = append(items, ignored)
					} else if r.URL.Path != "/apis/autoscaling/v2/namespaces/apps/horizontalpodautoscalers" {
						t.Error("HPA read escaped explicit namespace scope")
					}
				}

				json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{"resourceVersion": "1"}, "items": items})
			}))
			defer server.Close()

			c := testConfig()
			c.NamespaceWatch, c.NamespaceIgnore, c.MaxObjects = tt.scope.NamespaceWatch, tt.scope.NamespaceIgnore, tt.max

			snapshot, err := NewSource(clientFor(t, server), c).Collect(t.Context(), time.Now())
			if tt.want == 0 {
				if err == nil || strings.Contains(err.Error(), "private-hpa-error") {
					t.Fatal("incomplete HPA collection accepted or API body leaked")
				}
			} else if err != nil || len(snapshot.Objects) != tt.want {
				t.Fatalf("unexpected HPA collection: count=%d err=%v", len(snapshot.Objects), err)
			}
		})
	}
}

// TestHPAWatchUpdate tracks condition recovery and metric observations through the normal projected watch cache.
func TestHPAWatchUpdate(t *testing.T) {
	trigger := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.URL.Query().Get("watch") == "true" {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()

			select {
			case <-trigger:
			case <-r.Context().Done():
				return
			}

			if strings.HasSuffix(r.URL.Path, "/horizontalpodautoscalers") {
				raw := hpaObject("apps")
				raw["metadata"].(map[string]any)["resourceVersion"] = "2"
				raw["status"].(map[string]any)["conditions"] = []any{map[string]any{"type": "ScalingActive", "status": "True"}}
				raw["status"].(map[string]any)["currentMetrics"].([]any)[0].(map[string]any)["resource"].(map[string]any)["current"].(map[string]any)["averageUtilization"] = int64(55)
				json.NewEncoder(w).Encode(map[string]any{"type": "MODIFIED", "object": raw})
				w.(http.Flusher).Flush()
			}

			<-r.Context().Done()

			return
		}

		items := []any{}
		if strings.HasSuffix(r.URL.Path, "/nodes") {
			items = append(items, node())
		} else if strings.HasSuffix(r.URL.Path, "/horizontalpodautoscalers") {
			items = append(items, hpaObject("apps"))
		}

		json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{"resourceVersion": "1"}, "items": items})
	}))
	defer server.Close()

	source := NewSource(clientFor(t, server), testConfig())
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() { defer close(done); source.Run(ctx) }()

	defer func() { cancel(); <-done }()

	until(t, func() bool { return source.Err() == nil })
	close(trigger)
	until(t, func() bool {
		snapshot, err := source.Snapshot(time.Now())
		if err != nil {
			return false
		}

		for _, o := range snapshot.Objects {
			if o.Kind == "HorizontalPodAutoscaler" {
				return o.Version == "2" && *o.HPA.CurrentMetrics[0].Utilization == 55 && len(health.Evaluate(snapshot, health.DefaultPolicy())) == 0
			}
		}

		return false
	})
}
