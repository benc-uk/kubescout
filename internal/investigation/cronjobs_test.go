/*
Purpose: Test approved CronJob inspection and scoped Warning-event tools.
Flow: Offline Kubernetes HTTP fixtures verify schedule/status fields, nested
Job-template container projection, version tracking and private-data exclusion.
Invalid scope, incarnation or schema returns no accepted evidence or citations.
*/
package investigation

import (
	"encoding/json"
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

// cronJobInspectionObject supplies nested template fields with synthetic private-data sentinels.
func cronJobInspectionObject() map[string]any {
	container := map[string]any{"name": "worker", "image": "worker:v1",
		"resources": map[string]any{"requests": map[string]any{"cpu": "10m", "memory": "32Mi"}},
		"ports":     []any{map[string]any{"name": "http", "containerPort": int64(8080), "protocol": "TCP", "hostIP": "private-host"}},
		"command":   []any{"private-command"}, "args": []any{"private-argument"},
		"env":          []any{map[string]any{"name": "PASSWORD", "value": "private-env"}},
		"volumeMounts": []any{map[string]any{"name": "data", "mountPath": "private-mount"}}}

	return map[string]any{"apiVersion": "batch/v1", "kind": "CronJob",
		"metadata": map[string]any{"name": "nightly", "namespace": "apps", "uid": "cron-uid", "resourceVersion": "7",
			"annotations": map[string]any{"private": "private-annotation"}},
		"spec": map[string]any{"schedule": "0 2 * * *", "timeZone": "Europe/London", "suspend": true, "concurrencyPolicy": "Forbid",
			"startingDeadlineSeconds": int64(0), "successfulJobsHistoryLimit": int64(0), "failedJobsHistoryLimit": int64(2),
			"jobTemplate": map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
				"containers": []any{container}, "initContainers": []any{map[string]any{"name": "init", "image": "init:v1"}},
				"volumes": []any{map[string]any{"name": "data", "secret": map[string]any{"secretName": "private-secret"}}},
			}}}}},
		"status": map[string]any{"active": []any{map[string]any{"name": "private-job", "namespace": "apps", "uid": "private-job-uid"}},
			"lastScheduleTime": "2026-11-01T02:00:00Z", "lastSuccessfulTime": "2026-10-31T02:01:00Z"},
	}
}

// TestCronJobInspectionAndEvents verifies precise approved facts and both tools' scoped API shapes.
func TestCronJobInspectionAndEvents(t *testing.T) {
	reads := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads++

		if r.Method != http.MethodGet {
			t.Error("CronJob tool attempted a workload write")
		}

		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/apis/batch/v1/namespaces/apps/cronjobs/nightly":
			json.NewEncoder(w).Encode(cronJobInspectionObject())
		case "/api/v1/namespaces/apps/events":
			if r.URL.Query().Get("fieldSelector") != "type=Warning,involvedObject.uid=cron-uid" || r.URL.Query().Get("limit") != "20" {
				t.Error("CronJob events escaped authorized bounded lookup")
			}

			fmt.Fprint(w, `{"apiVersion":"v1","kind":"EventList","metadata":{"resourceVersion":"1"},"items":[{"metadata":{"namespace":"apps"},"type":"Warning","involvedObject":{"uid":"cron-uid","name":"nightly","kind":"CronJob"},"reason":"UnexpectedJob","message":"password=fixture"}]}`)
		default:
			t.Error("CronJob tool fetched an unapproved resource")
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	defer server.Close()

	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}

	s := &Session{Client: client, Scope: config.Config{NamespaceWatch: []string{"apps"}}, MaxCalls: 4, Timeout: time.Second,
		Citations: map[string]bool{}, Investigation: incident.InvestigationContext{
			Identities:   map[string]health.Resource{"ref": {Kind: "CronJob", Namespace: "apps", Name: "nightly", UID: "cron-uid"}},
			ToolVersions: map[string]string{}}}

	value, err := (InspectResource{Session: s}).Call(t.Context(), `{"resource_ref":"ref"}`)
	if err != nil {
		t.Fatal(err)
	}

	state := value.(map[string]any)["state"].(map[string]any)
	if state["spec.schedule"] != "0 2 * * *" || state["spec.timeZone"] != "Europe/London" ||
		state["spec.suspend"] != true || state["spec.concurrencyPolicy"] != "Forbid" ||
		state["spec.startingDeadlineSeconds"] != int64(0) || state["spec.successfulJobsHistoryLimit"] != int32(0) ||
		state["spec.failedJobsHistoryLimit"] != int32(2) || state["status.active_count"] != 1 ||
		state["status.lastScheduleTime"] == nil || state["status.lastSuccessfulTime"] == nil || s.Investigation.ToolVersions["ref"] != "7" {
		t.Fatalf("approved scheduling state missing: %+v", state)
	}

	containers := state["spec.containers"].([]map[string]any)

	init := state["spec.initContainers"].([]map[string]any)
	if len(containers) != 1 || containers[0]["image"] != "worker:v1" || containers[0]["resources"] == nil ||
		len(containers[0]["ports"].([]map[string]any)) != 1 || len(init) != 1 || init[0]["image"] != "init:v1" {
		t.Fatal("nested Job-template container projection missing")
	}

	data, _ := json.Marshal(value)
	if strings.Contains(string(data), "private-") || strings.Contains(string(data), "cron-uid") || strings.Contains(string(data), `"status.active"`) {
		t.Fatal("private CronJob template or active reference reached the model")
	}

	events, err := (RelatedWarningEvents{Session: s}).Call(t.Context(), `{"resource_ref":"ref"}`)
	if err != nil || reads != 2 || len(s.Citations) != 2 {
		t.Fatalf("CronJob Warning-event tool failed: %v", err)
	}

	data, _ = json.Marshal(events)
	if strings.Contains(string(data), "password=fixture") {
		t.Fatal("CronJob Warning message was not redacted")
	}
}

// TestCronJobToolRejectsInvalidEvidence checks scope before reads and rejects changed incarnations or invalid projections.
func TestCronJobToolRejectsInvalidEvidence(t *testing.T) {
	for _, tt := range []struct {
		name, namespace string
		edit            func(map[string]any)
		wantReads       int
	}{
		{name: "scope", namespace: "other", edit: func(o map[string]any) {}},
		{name: "incarnation", namespace: "apps", edit: func(o map[string]any) { o["metadata"].(map[string]any)["uid"] = "different" }, wantReads: 1},
		{name: "active limit", namespace: "apps", edit: func(o map[string]any) { o["status"].(map[string]any)["active"] = make([]map[string]any, 129) }, wantReads: 1},
		{name: "schema", namespace: "apps", edit: func(o map[string]any) { o["spec"].(map[string]any)["suspend"] = "not-a-bool" }, wantReads: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reads := 0

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				reads++
				o := cronJobInspectionObject()
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
					Identities: map[string]health.Resource{"ref": {Kind: "CronJob", Namespace: tt.namespace, Name: "nightly", UID: "cron-uid"}}}}

			value, err := (InspectResource{Session: s}).Call(t.Context(), `{"resource_ref":"ref"}`)
			if err == nil || value != nil || reads != tt.wantReads || len(s.Citations) != 0 {
				t.Fatal("invalid CronJob state accepted as evidence")
			}
		})
	}
}
