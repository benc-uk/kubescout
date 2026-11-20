/*
Purpose: Test kubeconfig context selection and client initialization failures.
Flow: Offline generated kubeconfigs and a local HTTP fixture verify explicit
and current-context selection, request identity, sanitized errors and rejection
without auth fallback; no live cluster or model credentials are used.
Read-only dynamic and describe clients reject write attempts before reaching the HTTP fixture.
*/
package kube

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/benc-uk/kubescout/internal/config"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// clientKubeconfig writes a test-owned kubeconfig with fixture and unreachable contexts for selection tests.
func clientKubeconfig(t *testing.T, host, current string) string {
	t.Helper()

	c := clientcmdapi.NewConfig()
	c.Clusters["fixture"] = &clientcmdapi.Cluster{Server: host}
	c.Clusters["wrong"] = &clientcmdapi.Cluster{Server: "http://127.0.0.1:1"}
	c.Contexts["selected"] = &clientcmdapi.Context{Cluster: "fixture"}
	c.Contexts["wrong"] = &clientcmdapi.Context{Cluster: "wrong"}
	c.CurrentContext = current

	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(*c, path); err != nil {
		t.Fatal(err)
	}

	return path
}

// TestClientContextSelection verifies explicit and current-context selection from file or environment
// and checks the resulting client's request path and user agent against a local server.
func TestClientContextSelection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/api/v1/nodes" || r.Header.Get("User-Agent") != "kubescout/go" {
			t.Error("unexpected Kubernetes request")
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"v1","kind":"NodeList","items":[]}`)
	}))
	defer server.Close()

	for _, test := range []struct {
		source, context, current string
	}{
		{"explicit-file", "", "selected"},
		{"KUBECONFIG", "", "selected"},
		{"explicit-file", "selected", "wrong"},
		{"KUBECONFIG", "selected", "wrong"},
		{"explicit-file", "selected", ""},
	} {
		t.Run(test.source+"/context="+test.context+"/current="+test.current, func(t *testing.T) {
			path := clientKubeconfig(t, server.URL, test.current)
			c := config.Config{Auth: "kubeconfig", Context: test.context}

			t.Setenv("KUBERNETES_SERVICE_HOST", "127.0.0.1")
			t.Setenv("KUBERNETES_SERVICE_PORT", "1")

			if test.source == "explicit-file" {
				c.Kubeconfig = path
				t.Setenv("KUBECONFIG", path+"-missing")
			} else {
				t.Setenv("KUBECONFIG", path)
			}

			client, identifier, err := Client(c)
			if err != nil || identifier != "selected" {
				t.Fatalf("context selection failed: identifier=%q err=%v", identifier, err)
			}

			connection, ok := client.(*Connection)
			if !ok || connection.DescribeConfig == nil || connection.DescribeConfig.Host != server.URL {
				t.Fatal("supplemental describe must use the explicitly selected Kubernetes connection")
			}

			_, err = client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "nodes"}).List(t.Context(), metav1.ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestClientContextFailures rejects missing or malformed kubeconfigs and contexts without fallback or content leaks.
func TestClientContextFailures(t *testing.T) {
	path := clientKubeconfig(t, "http://127.0.0.1:1", "selected")
	noCurrent := clientKubeconfig(t, "http://127.0.0.1:1", "")
	missingCurrent := clientKubeconfig(t, "http://127.0.0.1:1", "missing")

	bad := filepath.Join(t.TempDir(), "malformed")
	if err := os.WriteFile(bad, []byte("private-fixture-content: ["), 0600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("KUBECONFIG", path)
	t.Setenv("KUBERNETES_SERVICE_HOST", "127.0.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "1")

	for _, c := range []config.Config{
		{Auth: "kubeconfig", Kubeconfig: noCurrent},
		{Auth: "kubeconfig", Kubeconfig: missingCurrent},
		{Auth: "kubeconfig", Context: "missing", Kubeconfig: path},
		{Auth: "kubeconfig", Kubeconfig: path + "-missing"},
		{Auth: "kubeconfig", Kubeconfig: bad},
	} {
		client, _, err := Client(c)
		if err == nil || client != nil {
			t.Fatalf("invalid kubeconfig used a fallback: %+v", c)
		}

		if strings.Contains(err.Error(), "private-fixture") {
			t.Fatal("authentication error exposed kubeconfig contents")
		}
	}
}

// TestReadOnlyClientRejectsWrites preserves actual GET access and blocks dynamic
// writes and supplemental describe transport writes before any request reaches the fixture.
func TestReadOnlyClientRejectsWrites(t *testing.T) {
	var requests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)

		if r.Method != http.MethodGet {
			t.Errorf("read-only client sent %s", r.Method)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"v1","kind":"NodeList","items":[]}`)
	}))
	defer server.Close()

	client, _, err := Client(config.Config{Auth: "kubeconfig", ReadOnly: true,
		Kubeconfig: clientKubeconfig(t, server.URL, "selected")})
	if err != nil {
		t.Fatal(err)
	}

	nodes := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "nodes"})
	if _, err := nodes.List(t.Context(), metav1.ListOptions{}); err != nil {
		t.Fatal(err)
	}

	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Node", "metadata": map[string]any{"name": "fixture"},
	}}
	if _, err := nodes.Create(t.Context(), object, metav1.CreateOptions{}); err == nil {
		t.Fatal("read-only client accepted create")
	}

	if _, err := nodes.Update(t.Context(), object, metav1.UpdateOptions{}); err == nil {
		t.Fatal("read-only client accepted update")
	}

	if _, err := nodes.Patch(t.Context(), "fixture", types.MergePatchType, []byte(`{}`), metav1.PatchOptions{}); err == nil {
		t.Fatal("read-only client accepted patch")
	}

	if err := nodes.Delete(t.Context(), "fixture", metav1.DeleteOptions{}); err == nil {
		t.Fatal("read-only client accepted delete")
	}

	connection, ok := client.(*Connection)
	if !ok {
		t.Fatal("missing shared Kubernetes connection")
	}

	describe, err := rest.HTTPClientFor(connection.DescribeConfig)
	if err != nil {
		t.Fatal(err)
	}

	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/api/v1/nodes", nil)
	if err != nil {
		t.Fatal(err)
	}

	response, err := describe.Do(request)
	if response != nil {
		response.Body.Close()
	}

	if err == nil || requests.Load() != 1 {
		t.Fatal("read-only boundary failed or a write reached Kubernetes")
	}
}
