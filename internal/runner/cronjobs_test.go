/*
Purpose: Test CronJob collection outages and recovery freshness offline.
Flow: Local API denial and in-memory Job/CronJob fixtures verify that missing
read permissions skip AI and preserve incidents. Recovery review remains gated
by fresh evidence and current inspected versions, never by vanished history alone.
*/
package runner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/kube"
	"github.com/benc-uk/kubescout/internal/output"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// TestCronJobCollectionFailureRetainsIncidents rejects denied CronJob lists before AI or incident mutation.
func TestCronJobCollectionFailureRetainsIncidents(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Error("collection outage caused a Kubernetes write")
		}

		w.Header().Set("Content-Type", "application/json")

		if strings.HasSuffix(r.URL.Path, "/cronjobs") {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"Status","reason":"Forbidden","code":403,"message":"private-cron-error"}`)

			return
		}

		items := []any{}
		if strings.HasSuffix(r.URL.Path, "/nodes") {
			items = append(items, map[string]any{"apiVersion": "v1", "kind": "Node",
				"metadata": map[string]any{"name": "node", "uid": "node", "resourceVersion": "1"}})
		}

		json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{"resourceVersion": "1"}, "items": items})
	}))
	defer server.Close()

	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}

	cfg := options()
	cfg.RequestTimeout = time.Second
	resource := health.Resource{Kind: "CronJob", Namespace: "apps", Name: "nightly", UID: "cron"}
	repo := &incident.Memory{Records: map[string]incident.Record{"existing": {Value: incident.Incident{
		Identity:  incident.Identity{Anchor: resource, Family: "job-failure"},
		Resources: []health.Resource{resource}, Lifecycle: incident.Lifecycle{State: "open"}}}}}
	ai := &fakeAI{}

	var out, diag bytes.Buffer

	r := Runner{Config: cfg, Source: kube.NewSource(client, cfg), Analyzer: ai, Incidents: incident.Reconciler{Repository: repo},
		Output: &output.Writer{Out: &out, Err: &diag}}
	if r.Once(t.Context()) != 1 || ai.calls.Load() != 0 || len(repo.Records) != 1 ||
		repo.Records["existing"].Value.Lifecycle.State != "open" || strings.Contains(out.String()+diag.String(), "private-cron-error") {
		t.Fatal("denied CronJob collection invoked AI, leaked an API body or changed an incident")
	}
}

// TestCronJobRecoveryFreshness retains a CronJob recovery candidate and rejects stale or unknown observations.
func TestCronJobRecoveryFreshness(t *testing.T) {
	cron := health.Object{Resource: health.Resource{Kind: "CronJob", Namespace: "apps", Name: "nightly", UID: "cron"},
		Version: "10", CronJob: health.CronJobState{Schedule: "@daily", LastSuccess: new(time.Now())}}
	open := []incident.Record{{Value: incident.Incident{Identity: incident.Identity{Anchor: cron.Resource, Family: "job-failure"},
		Resources: []health.Resource{cron.Resource}, Lifecycle: incident.Lifecycle{State: "open"}}}}

	contexts, err := incident.BuildInvestigationContexts(health.Snapshot{At: time.Now(), Objects: []health.Object{cron}}, health.DefaultPolicy(), open, 100)
	if err != nil {
		t.Fatal(err)
	}

	key := incident.Ref(cron.Resource)
	in := contexts[key]
	in.RecoveryEligible = true
	in.ToolVersions = map[string]string{key: "10"}
	source := &fakeSource{objects: []health.Object{cron}}
	r := Runner{Config: options(), Source: source}

	if len(contexts) != 1 || incident.Active(in) || !r.current(in, open) {
		t.Fatal("fresh CronJob recovery context missing")
	}

	source.err = errors.New("CronJob observation unknown")

	if r.current(in, open) {
		t.Fatal("collection outage accepted as CronJob recovery")
	}

	source.err = nil
	source.objects[0].Version = "11"

	if r.current(in, open) {
		t.Fatal("changed inspected CronJob version accepted")
	}

	source.objects[0].Version = "10"
	job := health.Object{Resource: health.Resource{Kind: "Job", Namespace: "apps", Name: "new-run", UID: "job"},
		Owner: cron.Resource, Conditions: []health.Condition{{Type: "Failed", Status: "True"}}}
	source.objects = append(source.objects, job)

	if r.current(in, open) {
		t.Fatal("new scheduled failure accepted as unchanged recovery evidence")
	}
}
