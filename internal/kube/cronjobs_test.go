/*
Purpose: Test CronJob projection and scoped collection offline.
Flow: Unstructured and local HTTP fixtures verify scheduling defaults, optional
timestamps, projection bounds, template exclusion and list/watch freshness.
Owned Job identities remain private; no cluster or model calls are made.
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

// cronJobObject supplies approved scheduling fields and private template/active-reference sentinels.
func cronJobObject(namespace string) map[string]any {
	return map[string]any{
		"apiVersion": "batch/v1", "kind": "CronJob",
		"metadata": map[string]any{"name": "nightly", "namespace": namespace, "uid": namespace + "-cron", "resourceVersion": "1", "generation": int64(2)},
		"spec": map[string]any{"schedule": "0 2 * * *", "timeZone": "Europe/London", "suspend": true,
			"concurrencyPolicy": "Forbid", "startingDeadlineSeconds": int64(0),
			"successfulJobsHistoryLimit": int64(0), "failedJobsHistoryLimit": int64(2),
			"jobTemplate": map[string]any{"metadata": map[string]any{"annotations": map[string]any{"private": "private-annotation"}},
				"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{
					map[string]any{"name": "worker", "image": "worker:v1", "env": []any{map[string]any{"name": "PASSWORD", "value": "private-env"}}},
				}}}}}},
		"status": map[string]any{"active": []any{map[string]any{"kind": "Job", "name": "private-job", "uid": "private-job-uid", "namespace": namespace}},
			"lastScheduleTime": "2026-11-01T02:00:00Z", "lastSuccessfulTime": "2026-10-31T02:01:00Z"},
	}
}

// TestCronJobProjection checks scheduling facts, absent-versus-zero defaults and independent cache copies.
func TestCronJobProjection(t *testing.T) {
	raw := cronJobObject("apps")

	o, err := project(&unstructured.Unstructured{Object: raw}, "CronJob", "apps")
	if err != nil {
		t.Fatal(err)
	}

	c := o.Value.CronJob
	if c.Schedule != "0 2 * * *" || c.TimeZone != "Europe/London" || !c.Suspended || c.ConcurrencyPolicy != "Forbid" ||
		c.StartingDeadline == nil || *c.StartingDeadline != 0 || c.SuccessfulHistory != 0 || c.FailedHistory != 2 || c.Active != 1 ||
		c.LastSchedule == nil || c.LastSuccess == nil || o.Value.Generation != 2 {
		t.Fatalf("approved CronJob facts missing: %+v", c)
	}

	data, _ := json.Marshal(o)
	if strings.Contains(string(data), "private-") || strings.Contains(string(data), "worker:v1") {
		t.Fatal("raw active references or Job template entered cache projection")
	}

	copy := o.DeepCopyObject().(*cachedObject)
	*copy.Value.CronJob.StartingDeadline = 30
	*copy.Value.CronJob.LastSchedule = time.Time{}

	*copy.Value.CronJob.LastSuccess = time.Time{}
	if *o.Value.CronJob.StartingDeadline != 0 || o.Value.CronJob.LastSchedule.IsZero() || o.Value.CronJob.LastSuccess.IsZero() {
		t.Fatal("CronJob pointer facts aliased in cache copy")
	}

	raw["spec"] = map[string]any{"schedule": "@daily"}
	raw["status"] = map[string]any{}

	o, err = project(&unstructured.Unstructured{Object: raw}, "CronJob", "apps")
	if err != nil {
		t.Fatal(err)
	}

	c = o.Value.CronJob
	if c.Suspended || c.TimeZone != "" || c.ConcurrencyPolicy != "Allow" || c.SuccessfulHistory != 3 || c.FailedHistory != 1 ||
		c.StartingDeadline != nil || c.LastSchedule != nil || c.LastSuccess != nil || c.Active != 0 {
		t.Fatalf("CronJob omission defaults changed: %+v", c)
	}

	if findings := health.Evaluate(health.Snapshot{At: time.Now(), Objects: []health.Object{o.Value}}, health.DefaultPolicy()); len(findings) != 0 {
		t.Fatal("standalone CronJob fabricated missed-schedule evidence")
	}
}

// TestCronJobProjectionBounds checks the exact active-reference, schedule and time-zone size boundaries.
func TestCronJobProjectionBounds(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(map[string]any)
		invalid bool
	}{
		{"128 active", func(o map[string]any) { o["status"].(map[string]any)["active"] = make([]map[string]any, 128) }, false},
		{"129 active", func(o map[string]any) { o["status"].(map[string]any)["active"] = make([]map[string]any, 129) }, true},
		{"256 schedule bytes", func(o map[string]any) { o["spec"].(map[string]any)["schedule"] = strings.Repeat("x", 256) }, false},
		{"257 schedule bytes", func(o map[string]any) { o["spec"].(map[string]any)["schedule"] = strings.Repeat("x", 257) }, true},
		{"128 time-zone bytes", func(o map[string]any) { o["spec"].(map[string]any)["timeZone"] = strings.Repeat("x", 128) }, false},
		{"129 time-zone bytes", func(o map[string]any) { o["spec"].(map[string]any)["timeZone"] = strings.Repeat("x", 129) }, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw := cronJobObject("apps")
			tt.edit(raw)

			if _, err := project(&unstructured.Unstructured{Object: raw}, "CronJob", "apps"); (err != nil) != tt.invalid {
				t.Fatalf("unexpected projection bound: %v", err)
			}
		})
	}
}

// TestCronJobCollectionScopeAndFailures verifies namespace filtering before projection/admission and incomplete denied reads.
func TestCronJobCollectionScopeAndFailures(t *testing.T) {
	for _, tt := range []struct {
		name   string
		scope  config.Config
		max    int
		want   int
		denied bool
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
					t.Error("CronJob collection escaped bounded read-only requests")
				}

				w.Header().Set("Content-Type", "application/json")

				items := []any{}

				if strings.HasSuffix(r.URL.Path, "/nodes") {
					items = append(items, node())
				}

				if strings.HasSuffix(r.URL.Path, "/cronjobs") {
					if tt.denied {
						w.WriteHeader(http.StatusForbidden)
						fmt.Fprint(w, `{"apiVersion":"v1","kind":"Status","reason":"Forbidden","code":403,"message":"private-error"}`)

						return
					}

					items = append(items, cronJobObject("apps"))

					if len(tt.scope.NamespaceWatch) == 0 {
						if r.URL.Path != "/apis/batch/v1/cronjobs" {
							t.Error("all/ignore CronJob read was not all-namespace")
						}

						ignored := cronJobObject("system")
						if len(tt.scope.NamespaceIgnore) != 0 {
							ignored["spec"].(map[string]any)["schedule"] = strings.Repeat("x", 257)
						}

						items = append(items, ignored)
					} else if r.URL.Path != "/apis/batch/v1/namespaces/apps/cronjobs" {
						t.Error("CronJob read escaped explicit watch scope")
					}
				}

				json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{"resourceVersion": "1"}, "items": items})
			}))
			defer server.Close()

			c := testConfig()
			c.NamespaceWatch, c.NamespaceIgnore, c.MaxObjects = tt.scope.NamespaceWatch, tt.scope.NamespaceIgnore, tt.max

			snapshot, err := NewSource(clientFor(t, server), c).Collect(t.Context(), time.Now())
			if tt.want == 0 {
				if err == nil || strings.Contains(err.Error(), "private-error") {
					t.Fatal("incomplete CronJob collection accepted or API error leaked")
				}
			} else if err != nil || len(snapshot.Objects) != tt.want {
				t.Fatalf("unexpected CronJob collection: count=%d err=%v", len(snapshot.Objects), err)
			}
		})
	}
}

// TestCronJobWatchUpdate tracks scheduling/status updates through the standard scoped cache.
func TestCronJobWatchUpdate(t *testing.T) {
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

			if strings.HasSuffix(r.URL.Path, "/cronjobs") {
				raw := cronJobObject("apps")
				raw["metadata"].(map[string]any)["resourceVersion"] = "2"
				raw["spec"].(map[string]any)["suspend"] = false
				raw["status"].(map[string]any)["active"] = []any{}
				json.NewEncoder(w).Encode(map[string]any{"type": "MODIFIED", "object": raw})
				w.(http.Flusher).Flush()
			}

			<-r.Context().Done()

			return
		}

		items := []any{}
		if strings.HasSuffix(r.URL.Path, "/nodes") {
			items = append(items, node())
		} else if strings.HasSuffix(r.URL.Path, "/cronjobs") {
			items = append(items, cronJobObject("apps"))
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
			if o.Kind == "CronJob" {
				return o.Version == "2" && !o.CronJob.Suspended && o.CronJob.Active == 0
			}
		}

		return false
	})
}
