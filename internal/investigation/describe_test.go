/*
Purpose: Verify native kubectl describe retains operational details within its scope and limits.
Flow: Offline HTTP fixtures exercise the real library's resource/event reads,
scope guards, deadlines, byte/request ceilings, credential protection and freshness checks.
List fixtures omit item TypeMeta while retaining strict identity and version checks.
Captured and partial text start directly with native output, without prefixed notices.
No shell, live cluster, model calls or additional workload permissions are used.
*/
package investigation

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
	"unicode/utf8"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/describe"
)

// TestNativeDescribeGuardContract exercises the library's exact request and response protocol.
func TestNativeDescribeGuardContract(t *testing.T) {
	s, _ := descriptionSession(t, "ok")

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	g := &describeTransport{ctx: ctx, session: s, root: s.Investigation.Identities["pod-ref"],
		budget: &describeBudget{}, host: strings.TrimPrefix(s.DescribeConfig.Host, "http://"), privateIDs: map[string]bool{}}
	cfg := rest.CopyConfig(s.DescribeConfig)
	cfg.Timeout = time.Second
	cfg.QPS = -1
	cfg.RateLimiter = nil
	cfg.ContentType, cfg.AcceptContentTypes = "application/json", "application/json"
	cfg.Wrap(func(next http.RoundTripper) http.RoundTripper {
		g.next = next
		return g
	})

	d, ok := describe.DescriberFor(schema.GroupKind{Kind: "Pod"}, cfg)
	if !ok {
		t.Fatal("Pod describer missing")
	}

	text, err := d.Describe("apps", "pod", describe.DescriberSettings{ShowEvents: true, ChunkSize: 20})
	if err != nil || !strings.Contains(text, "Name:") {
		t.Fatalf("native library/transport contract failed: %v guard=%v", err, g.failure)
	}
}

// TestNativeDescribeCapture checks native output starts without preambles and is captured without duplicate reads.
func TestNativeDescribeCapture(t *testing.T) {
	for _, mode := range []string{"ok", "denied"} {
		t.Run(mode, func(t *testing.T) {
			s, calls := descriptionSession(t, mode)

			evidence, err := s.CaptureDescriptions(t.Context(), incident.MaxDescriptionBytes)
			if err != nil {
				t.Fatal(err)
			}

			d := evidence.Descriptions[0]
			if d.Source != incident.DescribeSource || !s.Citations[d.EvidenceID] ||
				!strings.HasPrefix(d.Output.Text, "Name:") || !strings.Contains(d.Output.Text, "QoS Class:") ||
				!strings.Contains(d.Output.Text, "Container ID:") && !strings.Contains(d.Output.Text, "Containers:") ||
				calls.Load() != 2 || s.Calls != 0 {
				t.Fatalf("native capture duplicated reads: calls=%d output=%+v", calls.Load(), d)
			}

			for _, private := range []string{"private-uid", "excluded-annotation", "excluded-env", "excluded-credential", "private-provider-error"} {
				if strings.Contains(d.Output.Text, private) {
					t.Fatalf("native describe leaked %s", private)
				}
			}

			if mode == "denied" && (d.Output.Status != "partial" || !strings.Contains(d.Output.Text, "Events: [unavailable:")) {
				t.Fatalf("kubectl's swallowed event failure became empty evidence: %s", d.Output.Text)
			}
		})
	}
}

// TestNativeDescribePodWithNode accepts ordinary Pod list items during related Node capture.
func TestNativeDescribePodWithNode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/namespaces/apps/pods/pod":
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"pod","namespace":"apps","uid":"pod-uid","resourceVersion":"7"},"spec":{"nodeName":"node","containers":[{"name":"api","image":"api:v1"}]}}`)
		case "/api/v1/nodes/node":
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"Node","metadata":{"name":"node","uid":"node-uid","resourceVersion":"8"}}`)
		case "/api/v1/pods":
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"PodList","metadata":{"resourceVersion":"9"},"items":[{"metadata":{"name":"pod","namespace":"apps","uid":"pod-uid","resourceVersion":"7"},"spec":{"nodeName":"node","containers":[{"name":"api","image":"api:v1"}]}}]}`)
		case "/api/v1/events", "/api/v1/namespaces/apps/events":
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"EventList","metadata":{"resourceVersion":"10"},"items":[]}`)
		default:
			t.Errorf("unexpected describe request: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	s := &Session{DescribeConfig: &rest.Config{Host: server.URL}, Timeout: time.Second, Citations: map[string]bool{},
		Investigation: incident.InvestigationContext{Anchor: "pod", Identities: map[string]health.Resource{
			"pod":  {Kind: "Pod", Namespace: "apps", Name: "pod", UID: "pod-uid"},
			"node": {Kind: "Node", Name: "node", UID: "node-uid"},
		}, ToolVersions: map[string]string{}}}

	evidence, err := s.CaptureDescriptions(t.Context(), incident.MaxDescriptionBytes)
	if err != nil {
		t.Fatal(err)
	}

	if len(evidence.Descriptions) != 2 || evidence.Descriptions[0].Output.Status != "captured" ||
		evidence.Descriptions[1].Output.Status != "partial" ||
		!strings.Contains(evidence.Descriptions[1].Output.Text, "(1 in total)") ||
		!strings.Contains(evidence.Descriptions[1].Output.Text, "apps") ||
		s.Investigation.ToolVersions["pod"] != "7" || s.Investigation.ToolVersions["node"] != "8" ||
		len(s.Citations) != 2 {
		t.Fatalf("ordinary Pod list prevented native evidence capture: %+v", evidence)
	}
}

// TestDescribeListItemTypeMeta accepts omitted list kinds without relaxing identity, scope or GET checks.
func TestDescribeListItemTypeMeta(t *testing.T) {
	for _, kind := range []string{"Pod", "ReplicaSet"} {
		for _, mode := range []string{"omitted", "explicit", "wrong-kind", "empty-kind", "invalid-kind", "null-kind", "recreated", "missing-uid", "missing-version", "changed-version", "wrong-namespace", "get-without-kind"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				resource := health.Resource{Kind: kind, Name: "target", Namespace: "apps", UID: "uid"}
				object := &unstructured.Unstructured{Object: map[string]any{
					"metadata": map[string]any{"name": resource.Name, "namespace": resource.Namespace, "uid": resource.UID, "resourceVersion": "7"},
				}}

				switch mode {
				case "explicit":
					object.SetKind(kind)
				case "wrong-kind":
					object.SetKind("Node")
				case "empty-kind":
					object.SetKind("")
				case "invalid-kind":
					object.Object["kind"] = true
				case "null-kind":
					object.Object["kind"] = nil
				case "recreated":
					object.SetUID("replacement")
				case "missing-uid":
					object.SetUID("")
				case "missing-version":
					object.SetResourceVersion("")
				case "changed-version":
					object.SetResourceVersion("8")
				case "wrong-namespace":
					object.SetNamespace("other")
				}

				gvr := resources[kind]

				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")

					if mode == "get-without-kind" {
						json.NewEncoder(w).Encode(object)
						return
					}

					json.NewEncoder(w).Encode(map[string]any{
						"apiVersion": gvr.GroupVersion().String(), "kind": kind + "List",
						"metadata": map[string]any{"resourceVersion": "9"}, "items": []any{object.Object},
					})
				}))
				defer server.Close()

				s := &Session{Investigation: incident.InvestigationContext{
					Identities: map[string]health.Resource{"ref": resource}, ToolVersions: map[string]string{"ref": "7"},
				}}
				guard := &describeTransport{ctx: t.Context(), session: s, root: resource, next: http.DefaultTransport,
					host: strings.TrimPrefix(server.URL, "http://"), budget: &describeBudget{}, privateIDs: map[string]bool{}}

				path := guard.resourcePath(describePath{gvr: gvr, kind: kind}, "apps")
				if mode == "get-without-kind" {
					path += "/target"
				}

				req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+path, nil)
				if err != nil {
					t.Fatal(err)
				}

				response, err := guard.RoundTrip(req)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()

				if mode != "omitted" && mode != "explicit" {
					if guard.failure == nil || response.StatusCode == http.StatusOK {
						t.Fatalf("invalid identity, scope or version accepted: status=%d failure=%v", response.StatusCode, guard.failure)
					}

					return
				}

				if guard.failure != nil || guard.partial || response.StatusCode != http.StatusOK {
					t.Fatalf("valid list item rejected: status=%d failure=%v", response.StatusCode, guard.failure)
				}

				var list unstructured.UnstructuredList
				if err := json.NewDecoder(response.Body).Decode(&list); err != nil {
					t.Fatal(err)
				}

				if len(list.Items) != 1 || list.Items[0].GetKind() != kind || s.Investigation.ToolVersions["ref"] != "7" {
					t.Fatalf("list item kind or freshness tracking missing: %+v", list)
				}
			})
		}
	}
}

// TestDescribeReadLimits checks raw-byte, attempt and cancellation bounds through the actual library.
func TestDescribeReadLimits(t *testing.T) {
	for _, mode := range []string{"pages", "aggregate", "large", "timeout", "recreated", "repeated-page"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := calls.Add(1)

				w.Header().Set("Content-Type", "application/json")

				if mode == "timeout" {
					time.Sleep(250 * time.Millisecond)
				}

				if mode == "large" {
					fmt.Fprint(w, strings.Repeat(" ", maxDescribeResponseBytes+1))
					return
				}

				if strings.HasSuffix(r.URL.Path, "/events") {
					token := fmt.Sprint(n)
					if mode == "repeated-page" {
						token = "same"
					}

					padding := ""
					if mode == "aggregate" {
						padding = strings.Repeat("x", 100000)
					}

					json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "EventList",
						"metadata": map[string]any{"resourceVersion": "1", "continue": token, "padding": padding}, "items": []any{}})

					return
				}

				uid := "uid"
				if mode == "recreated" {
					uid = "other-uid"
				}

				json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "Pod",
					"metadata": map[string]any{"name": "pod", "namespace": "apps", "uid": uid, "resourceVersion": "1"},
					"spec":     map[string]any{"containers": []any{map[string]any{"name": "api", "image": "api:v1"}}}})
			}))
			defer server.Close()

			s := &Session{DescribeConfig: &rest.Config{Host: server.URL}, Timeout: time.Second,
				Investigation: incident.InvestigationContext{Identities: map[string]health.Resource{
					"ref": {Kind: "Pod", Namespace: "apps", Name: "pod", UID: "uid"},
				}, ToolVersions: map[string]string{}}}
			if mode == "timeout" {
				s.Timeout = 50 * time.Millisecond
			}

			started := time.Now()
			budget := &describeBudget{}

			result, err := s.describeResource(t.Context(), "ref", budget)
			if mode == "recreated" || mode == "repeated-page" {
				if err == nil {
					t.Fatal("recreated resource accepted")
				}

				return
			}

			if err != nil || result.Status == "captured" {
				t.Fatalf("read failure hidden: %+v %v", result, err)
			}

			if mode == "pages" && calls.Load() != maxDescribeRequests {
				t.Fatalf("pagination escaped total HTTP cap: %d", calls.Load())
			}

			if mode == "aggregate" && (budget.bytes <= maxDescribeReadBytes || calls.Load() >= maxDescribeRequests) {
				t.Fatal("cumulative raw response bytes did not stop describe before the request limit")
			}

			if mode == "timeout" && time.Since(started) > time.Second {
				t.Fatal("kubectl context.TODO escaped the parent deadline")
			}
		})
	}
}

// TestDescribeUnauthorizedRequests rejects library-side API expansion before any upstream read.
func TestDescribeUnauthorizedRequests(t *testing.T) {
	s := &Session{Scope: config.Config{NamespaceWatch: []string{"apps"}},
		Investigation: incident.InvestigationContext{Identities: map[string]health.Resource{
			"pod": {Kind: "Pod", Namespace: "apps", Name: "pod", UID: "uid"},
		}}}
	for _, request := range []struct{ method, path string }{
		{"POST", "/api/v1/namespaces/apps/pods"},
		{"GET", "/api/v1/namespaces/apps/secrets"},
		{"GET", "/api/v1/namespaces/apps/configmaps"},
		{"GET", "/api/v1/namespaces/ignored/pods"},
		{"GET", "/api/v1/namespaces/apps/pods/other"},
		{"GET", "/api/v1/namespaces/apps/pods/pod/log"},
		{"GET", "/api/v1/namespaces/apps/pods?watch=true"},
		{"GET", "/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases/node"},
	} {
		g := &describeTransport{ctx: t.Context(), session: s, host: "api.example", budget: &describeBudget{}}

		req, err := http.NewRequestWithContext(t.Context(), request.method, "https://api.example"+request.path, nil)
		if err != nil {
			t.Fatal(err)
		}

		response, err := g.RoundTrip(req)
		if err != nil || response.StatusCode != http.StatusForbidden || g.budget.requests != 0 {
			t.Fatalf("unauthorized library request escaped: %s", request.path)
		}

		response.Body.Close()
	}
}

// TestDescribeIgnoredResources filters ignored objects before library rendering and freshness tracking.
func TestDescribeIgnoredResources(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/nodes/node":
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"Node","metadata":{"name":"node","uid":"node-uid","resourceVersion":"7"}}`)
		case "/api/v1/pods":
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"PodList","metadata":{"resourceVersion":"1"},"items":[{"apiVersion":"v1","kind":"Pod","metadata":{"name":"ignored-canary","namespace":"system","uid":"ignored-uid","resourceVersion":"1"}}]}`)
		case "/api/v1/events":
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"EventList","metadata":{"resourceVersion":"1"},"items":[]}`)
		default:
			t.Error("unexpected library read outside allowed APIs")
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	s := &Session{DescribeConfig: &rest.Config{Host: server.URL}, Timeout: time.Second,
		Scope: config.Config{NamespaceIgnore: []string{"system"}},
		Investigation: incident.InvestigationContext{Identities: map[string]health.Resource{
			"node": {Kind: "Node", Name: "node", UID: "node-uid"},
		}, ToolVersions: map[string]string{}}}

	result, err := s.describeResource(t.Context(), "node", &describeBudget{})
	if err != nil || result.Status != "partial" || strings.Contains(result.Text, "ignored-") || len(s.Investigation.ToolVersions) != 1 {
		t.Fatalf("ignored object leaked or affected freshness: %+v %v", result, err)
	}
}

// TestDescribeUnsupportedKind reports missing native support rather than rendering arbitrary raw objects.
func TestDescribeUnsupportedKind(t *testing.T) {
	s := &Session{DescribeConfig: &rest.Config{Host: "http://127.0.0.1:1"}, Timeout: time.Second,
		Investigation: incident.InvestigationContext{Identities: map[string]health.Resource{
			"attachment": {Kind: "VolumeAttachment", Name: "disk", UID: "uid"},
		}}}
	budget := &describeBudget{}

	result, err := s.describeResource(t.Context(), "attachment", budget)
	if err != nil || result.Status != "unavailable" || budget.requests != 0 || !strings.Contains(result.Text, "no resource describer") {
		t.Fatalf("unsupported kind gained a raw fallback: %+v %v", result, err)
	}
}

// TestDescribeNodeScope ensures native Pod/event lists respect explicit watch lists and cannot read Leases.
func TestDescribeNodeScope(t *testing.T) {
	var calls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")

		if r.URL.Path == "/api/v1/nodes/node" {
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"Node","metadata":{"name":"node","uid":"node-uid","resourceVersion":"7"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}`)
			return
		}

		if r.URL.Path != "/api/v1/namespaces/apps/pods" && r.URL.Path != "/api/v1/namespaces/jobs/pods" &&
			r.URL.Path != "/api/v1/namespaces/apps/events" && r.URL.Path != "/api/v1/namespaces/jobs/events" {
			t.Errorf("describe escaped watch scope or fetched Leases: %s", r.URL.Path)
		}

		fmt.Fprint(w, `{"apiVersion":"v1","kind":"List","metadata":{"resourceVersion":"1"},"items":[]}`)
	}))
	defer server.Close()

	s := &Session{DescribeConfig: &rest.Config{Host: server.URL}, Timeout: time.Second,
		Scope: config.Config{NamespaceWatch: []string{"apps", "jobs"}},
		Investigation: incident.InvestigationContext{Identities: map[string]health.Resource{
			"node": {Kind: "Node", Name: "node", UID: "node-uid"},
		}, ToolVersions: map[string]string{}}}

	result, err := s.describeResource(t.Context(), "node", &describeBudget{})
	if err != nil || result.Status != "partial" || calls.Load() != 5 || strings.Contains(result.Text, "node-uid") {
		t.Fatalf("unexpected native Node describe scope: %+v calls=%d err=%v", result, calls.Load(), err)
	}
}

// TestDescribeOperationalDetails preserves labels, commands, probes, volumes and addresses in native output.
func TestDescribeOperationalDetails(t *testing.T) {
	s, _ := descriptionSession(t, "ok")

	var calls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")

		if strings.HasSuffix(r.URL.Path, "/events") {
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"EventList","metadata":{"resourceVersion":"1"},"items":[]}`)
			return
		}

		fmt.Fprint(w, `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"pod","namespace":"apps","uid":"private-uid","resourceVersion":"7","labels":{"private":"label-canary"},"annotations":{"private":"annotation-canary\nName: annotation-spoof"}},"spec":{"nodeName":"node","containers":[{"name":"api","image":"api:v1","env":[{"name":"VALUE","value":"env-canary\nName: env-spoof"}],"command":["command-canary\nName: command-spoof"],"args":["arg-canary\nName: arg-spoof"],"livenessProbe":{"exec":{"command":["probe-canary\nName: probe-spoof"]}}},{"name":"sidecar","image":"sidecar:v1"}],"volumes":[{"name":"volume","hostPath":{"path":"/volume-canary"}}]},"status":{"phase":"Running","hostIP":"10.20.30.40","podIP":"10.20.30.50"}}`)
	}))
	defer server.Close()

	s.DescribeConfig = &rest.Config{Host: server.URL}

	result, err := s.describeResource(context.Background(), "pod-ref", &describeBudget{})
	if err != nil || result.Status != "captured" || !strings.Contains(result.Text, "sidecar:v1") || !strings.Contains(result.Text, "api:v1") {
		t.Fatalf("useful native output missing: %+v %v", result, err)
	}

	for _, wanted := range []string{"label-canary", "command-canary", "arg-canary", "probe-canary", "/volume-canary", "10.20.30.40", "10.20.30.50"} {
		if !strings.Contains(result.Text, wanted) {
			t.Fatalf("operational detail %q was removed: %s", wanted, result.Text)
		}
	}

	for _, forbidden := range []string{"annotation-canary", "annotation-spoof", "env-canary", "env-spoof", "private-uid", "omitted by policy"} {
		if strings.Contains(result.Text, forbidden) {
			t.Fatalf("unsafe native describe content: %s", result.Text)
		}
	}
}

// TestDescribeRetainsUnknownSections checks native text is unchanged without a field allowlist or added preamble.
func TestDescribeRetainsUnknownSections(t *testing.T) {
	text := "Labels:\tapp=token-reader\nSelector:\tcomponent=secret-reader\n" +
		"New native section:\n  Driver path:\t/data/private-volume\n" +
		"Message:\tauthorization denied; token refresh failed; password file missing\n" +
		"Events:\n  Warning FailedToRetrieveImagePullSecret registry secret missing\n"

	got := describeText(text, nil, nil, false, false)
	if got != text {
		t.Fatalf("native content was filtered: %s", got)
	}
}

// TestDescribeHPAMetrics verifies native output retains diagnostic metric names and credential context.
func TestDescribeHPAMetrics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/apis/autoscaling/v2/namespaces/apps/horizontalpodautoscalers/web":
			json.NewEncoder(w).Encode(hpaInspectionObject())
		case "/api/v1/namespaces/apps/events":
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"EventList","metadata":{"resourceVersion":"1"},"items":[]}`)
		default:
			t.Errorf("unexpected HPA describe read: %s", r.URL.Path)
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	defer server.Close()

	s := &Session{DescribeConfig: &rest.Config{Host: server.URL}, Timeout: time.Second,
		Investigation: incident.InvestigationContext{Identities: map[string]health.Resource{
			"ref": {Kind: "HorizontalPodAutoscaler", Namespace: "apps", Name: "web", UID: "hpa-uid"},
		}, ToolVersions: map[string]string{}}}

	result, err := s.describeResource(t.Context(), "ref", &describeBudget{})
	if err != nil || result.Status != "captured" {
		t.Fatalf("native HPA describe failed: %+v %v", result, err)
	}

	for _, wanted := range []string{"token_rate", "FailedGetResourceMetric", "[redacted credential]"} {
		if !strings.Contains(result.Text, wanted) {
			t.Fatalf("native HPA detail %q missing: %s", wanted, result.Text)
		}
	}
}

// TestDescribeTransportPreservesSelectors checks selectors survive transport even if kubectl does not display them.
func TestDescribeTransportPreservesSelectors(t *testing.T) {
	guard := &describeTransport{privateIDs: map[string]bool{}}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://api.example", nil)
	if err != nil {
		t.Fatal(err)
	}

	response, err := guard.response(req, &unstructured.Unstructured{Object: hpaInspectionObject()})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	var object map[string]any
	if err := json.NewDecoder(response.Body).Decode(&object); err != nil {
		t.Fatal(err)
	}

	metrics, _, err := unstructured.NestedSlice(object, "spec", "metrics")
	if err != nil || len(metrics) != 2 {
		t.Fatal("HPA metrics lost in describe transport")
	}

	selector, found, err := unstructured.NestedString(metrics[1].(map[string]any), "external", "metric", "selector", "matchLabels", "private")
	if err != nil || !found || selector != "private-selector" {
		t.Fatal("describe transport removed a metric selector")
	}
}

// TestDescribeCredentialProtection checks native commands and messages retain context around credentials.
func TestDescribeCredentialProtection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if strings.HasSuffix(r.URL.Path, "/events") {
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"EventList","metadata":{"resourceVersion":"1"},"items":[]}`)
			return
		}

		json.NewEncoder(w).Encode(map[string]any{
			"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": "pod", "namespace": "apps", "uid": "uid", "resourceVersion": "1"},
			"spec": map[string]any{"containers": []any{map[string]any{
				"name": "token-service", "image": "api:v1",
				"command": []any{"/app", "--password=fixture-password", "--feature=token-refresh"},
				"args":    []any{"literal-credential"},
			}}},
			"status": map[string]any{"phase": "Pending", "message": "Mount failed: token=fixture-token; endpoint=10.20.30.40"},
		})
	}))
	defer server.Close()

	s := &Session{DescribeConfig: &rest.Config{Host: server.URL}, Timeout: time.Second, SecretValues: []string{"literal-credential"},
		Investigation: incident.InvestigationContext{Identities: map[string]health.Resource{
			"ref": {Kind: "Pod", Namespace: "apps", Name: "pod", UID: "uid"},
		}, ToolVersions: map[string]string{}}}

	result, err := s.describeResource(t.Context(), "ref", &describeBudget{})
	if err != nil || result.Status != "captured" {
		t.Fatalf("native credential fixture failed: %+v %v", result, err)
	}

	for _, wanted := range []string{"token-service", "--password=[redacted credential]", "--feature=token-refresh", "Mount failed", "10.20.30.40"} {
		if !strings.Contains(result.Text, wanted) {
			t.Fatalf("credential protection removed context %q: %s", wanted, result.Text)
		}
	}

	for _, credential := range []string{"fixture-password", "fixture-token", "literal-credential"} {
		if strings.Contains(result.Text, credential) {
			t.Fatalf("credential value remained in native output: %s", credential)
		}
	}
}

// TestDescribeBudget bounds encoded native text without dropping resource identity or citation metadata.
func TestDescribeBudget(t *testing.T) {
	s, _ := descriptionSession(t, "ok")

	evidence, err := s.CaptureDescriptions(t.Context(), incident.MaxDescriptionBytes/2)
	if err != nil {
		t.Fatal(err)
	}

	before := evidence.Descriptions[0]
	original, _ := json.Marshal(evidence)
	evidence.Descriptions[0].Output = incident.DescriptionOutput{Status: "captured", Text: strings.Repeat("界<\"\n", 5000)}

	budget := len(original) + 1024
	if err := fitDescriptions(evidence, budget); err != nil {
		t.Fatal(err)
	}

	encoded, _ := json.Marshal(evidence)

	after := evidence.Descriptions[0]
	if len(encoded) > budget || !utf8.Valid(encoded) || after.Resource != before.Resource ||
		after.Source != before.Source || after.EvidenceID != before.EvidenceID || after.ObservedAt != before.ObservedAt ||
		after.Output.Status == "captured" || incident.ValidateEvidence(evidence) != nil {
		t.Fatal("describe output violated serialized limits or changed resource metadata")
	}
}
