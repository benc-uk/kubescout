/*
Purpose: Test native-only automatic descriptions and their independent bounds.
Flow: Offline HTTP fixtures cover native output, credentials and metadata exclusions,
unavailable reads, incarnation/scope rejection, selection and serialized UTF-8
limits and description-only JSON fields without Kubernetes credentials, model calls or incident publication.
*/
package investigation

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// descriptionSession serves one known Pod and Warning list with optional failure modes.
func descriptionSession(t *testing.T, mode string) (*Session, *atomic.Int32) {
	t.Helper()

	calls := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)

		if r.Method != "GET" {
			t.Error("description attempted a write")
		}

		w.Header().Set("Content-Type", "application/json")

		if r.URL.Path == "/api/v1/namespaces/apps/events" {
			if r.URL.Query().Get("fieldSelector") != "type=Warning,involvedObject.uid=private-uid" || r.URL.Query().Get("limit") != "20" {
				t.Error("description event read escaped approved scope")
			}

			if mode == "denied" {
				w.WriteHeader(http.StatusForbidden)
				fmt.Fprint(w, `{"message":"private-provider-error"}`)

				return
			}

			if mode == "bad-events" {
				fmt.Fprint(w, `{"apiVersion":"v1","kind":"EventList","metadata":{"resourceVersion":"1"},"items":[{"metadata":{"namespace":"other"},"involvedObject":{"uid":"private-uid"},"type":"Warning"}]}`)
				return
			}

			fmt.Fprint(w, `{"apiVersion":"v1","kind":"EventList","metadata":{"resourceVersion":"1"},"items":[]}`)

			return
		}

		if r.URL.Path != "/api/v1/namespaces/apps/pods/pod" {
			t.Error("unexpected resource scope")
		}

		if mode == "missing" {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","reason":"NotFound","code":404}`)

			return
		}

		uid := "private-uid"
		if mode == "recreated" {
			uid = "new-uid"
		}

		message := "Container not ready"
		if mode == "large" {
			message = strings.Repeat("界", 5000)
		}

		json.NewEncoder(w).Encode(map[string]any{
			"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": "pod", "namespace": "apps", "uid": uid, "resourceVersion": "7",
				"labels": map[string]any{"private": "excluded-label"}, "annotations": map[string]any{"private": "excluded-annotation"}},
			"spec": map[string]any{"nodeName": "worker", "containers": []any{map[string]any{
				"name": "api", "image": "api:v1", "env": []any{map[string]any{"name": "PASSWORD", "value": "excluded-env"}},
			}}},
			"status": map[string]any{"phase": "Running", "message": message, "conditions": []any{
				map[string]any{"type": "Ready", "status": "False", "reason": "ContainersNotReady", "message": message, "lastTransitionTime": "2026-11-02T09:00:00Z"},
				map[string]any{"type": "Diagnostic", "status": "False", "message": "token=excluded-credential"},
			}},
		})
	}))
	t.Cleanup(server.Close)

	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}

	return &Session{Client: client, DescribeConfig: &rest.Config{Host: server.URL}, Scope: config.Config{NamespaceWatch: []string{"apps"}},
		Timeout: time.Second, MaxCalls: 64, Citations: map[string]bool{},
		Investigation: incident.InvestigationContext{Anchor: "pod-ref", Identities: map[string]health.Resource{
			"pod-ref": {Kind: "Pod", Namespace: "apps", Name: "pod", UID: "private-uid"},
		}, ToolVersions: map[string]string{}}}, calls
}

// TestAutomaticDescriptionBoundaries verifies native-only shape, coverage, credentials and hard failures.
func TestAutomaticDescriptionBoundaries(t *testing.T) {
	for _, mode := range []string{"ok", "denied", "large", "missing", "recreated", "bad-events", "ignored", "metadata-budget"} {
		t.Run(mode, func(t *testing.T) {
			s, calls := descriptionSession(t, mode)
			budget := incident.MaxDescriptionBytes

			if mode == "ignored" {
				s.Scope.NamespaceWatch = []string{"other"}
			}

			if mode == "metadata-budget" {
				budget = 1
			}

			evidence, err := s.CaptureDescriptions(t.Context(), budget)
			if mode == "recreated" || mode == "bad-events" || mode == "ignored" || mode == "metadata-budget" {
				if err == nil || evidence != nil || len(s.Citations) != 0 {
					t.Fatal("invalid capture admitted evidence")
				}

				if (mode == "ignored" || mode == "metadata-budget") && calls.Load() != 0 {
					t.Fatal("invalid scope/budget performed API reads")
				}

				return
			}

			if err != nil {
				t.Fatal(err)
			}

			data, err := json.Marshal(evidence)
			if err != nil || len(data) > budget || s.Bytes != len(data) || s.Calls != 0 || calls.Load() != 2 || !s.Citations["kubectl-describe-1"] {
				t.Fatalf("incorrect capture bounds/accounting: %v bytes=%d/%d calls=%d want=2 citations=%v",
					err, s.Bytes, len(data), calls.Load(), s.Citations)
			}

			var encoded map[string]any
			if err := json.Unmarshal(data, &encoded); err != nil {
				t.Fatal(err)
			}

			if encoded["snapshots"] != nil {
				t.Fatal("old resource evidence field was emitted")
			}

			item := encoded["descriptions"].([]any)[0].(map[string]any)
			if len(item) != 5 || item["source"] != incident.DescribeSource || item["output"] == nil ||
				item["state"] != nil || item["warning_events"] != nil || item["describe"] != nil {
				t.Fatalf("duplicate resource details were stored: %s", data)
			}

			for _, private := range []string{"private-uid", "excluded-annotation", "excluded-env", "excluded-credential", "private-provider-error"} {
				if strings.Contains(string(data), private) {
					t.Fatalf("description leaked %s", private)
				}
			}

			description := evidence.Descriptions[0]
			if mode == "missing" {
				if description.Output.Status != "absent" || s.Investigation.ToolVersions["pod-ref"] != "" {
					t.Fatal("missing resource reported healthy")
				}

				return
			}

			if !strings.Contains(description.Output.Text, "Name:") || s.Investigation.ToolVersions["pod-ref"] != "7" {
				t.Fatal("native output or private freshness tracking missing")
			}

			if mode == "denied" && (description.Output.Status != "partial" || !strings.Contains(description.Output.Text, "Events: [unavailable:")) {
				t.Fatal("unavailable events reported as empty native output")
			}

			if mode == "large" && (description.Output.Status != "partial" || !utf8.ValidString(description.Output.Text)) {
				t.Fatal("large Unicode text was not explicitly partial")
			}
		})
	}
}

// TestDescriptionSelectionAndBytes checks anchor/symptom priority and JSON-byte limits with escaped text.
func TestDescriptionSelectionAndBytes(t *testing.T) {
	s, _ := descriptionSession(t, "ok")
	s.Investigation.Identities["a"] = health.Resource{}
	s.Investigation.Identities["b"] = health.Resource{}
	s.Investigation.Identities["c"] = health.Resource{}
	s.Investigation.Identities["d"] = health.Resource{}

	s.Investigation.Signals = []incident.Signal{{Resource: "d", Hint: "critical"}, {Resource: "c", Hint: "warning"}}
	if got := s.descriptionRefs(); !reflect.DeepEqual(got, []string{"pod-ref", "d", "c", "a", "b"}) {
		t.Fatalf("selection order: %v", got)
	}

	evidence := &incident.Evidence{Format: incident.DescriptionFormat, Descriptions: []incident.ResourceDescription{{
		EvidenceID: "kubectl-describe-1", Resource: incident.Resource{Ref: "ref", Kind: "Pod", Name: "pod", Namespace: "apps"},
		Source: incident.DescribeSource, ObservedAt: time.Now().UTC(),
		Output: incident.DescriptionOutput{Status: "captured", Text: strings.Repeat("界<\"\n", 2000)},
	}}}
	if err := fitDescriptions(evidence, 1000); err != nil {
		t.Fatal(err)
	}

	data, err := json.Marshal(evidence)
	if err != nil || len(data) > 1000 || !utf8.Valid(data) || evidence.Descriptions[0].Output.Status != "partial" {
		t.Fatal("serialized byte limit or UTF-8 safety violated")
	}
}

// TestCaptureUnsupportedDescribe records explicit unavailability without typed description fallback.
func TestCaptureUnsupportedDescribe(t *testing.T) {
	s, calls := descriptionSession(t, "ok")
	s.Investigation.Anchor = "attachment"
	s.Investigation.Identities = map[string]health.Resource{
		"attachment": {Kind: "VolumeAttachment", Name: "disk", UID: "attachment-uid"},
	}

	evidence, err := s.CaptureDescriptions(t.Context(), incident.MaxDescriptionBytes)
	if err != nil || calls.Load() != 0 || len(evidence.Descriptions) != 1 ||
		evidence.Descriptions[0].Output.Status != "unavailable" || !s.Citations["kubectl-describe-1"] {
		t.Fatalf("unsupported native kind gained a fallback: %+v %v", evidence, err)
	}
}

// TestDescriptionAndToolReadsCannotDisagree ensures a later inspection cannot overwrite captured freshness.
func TestDescriptionAndToolReadsCannotDisagree(t *testing.T) {
	s, _ := descriptionSession(t, "ok")

	s.Investigation.ToolVersions["pod-ref"] = "6"
	if _, err := s.CaptureDescriptions(t.Context(), incident.MaxDescriptionBytes); err == nil {
		t.Fatal("conflicting observations were accepted")
	}
}
