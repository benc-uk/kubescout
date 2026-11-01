/*
Purpose: Test dashboard authentication selection and incident-read HTTP flow.
Flow: Offline TLS servers and generated kubeconfig fixtures exercise explicit
and current contexts, authentication failures without fallback, and sanitized
read errors through the client and handler; no live credentials are required.
*/
package dashboard

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// writeKubeconfig writes a TLS fixture configuration with selectable working and unreachable contexts.
func writeKubeconfig(t *testing.T, server *httptest.Server, current string) string {
	t.Helper()

	config := clientcmdapi.NewConfig()
	config.Clusters["selected-cluster"] = &clientcmdapi.Cluster{
		Server: server.URL,
		CertificateAuthorityData: pem.EncodeToMemory(&pem.Block{
			Type: "CERTIFICATE", Bytes: server.Certificate().Raw,
		}),
	}
	config.Clusters["wrong-cluster"] = &clientcmdapi.Cluster{Server: "https://127.0.0.1:1"}
	config.AuthInfos["fixture"] = &clientcmdapi.AuthInfo{Token: "fixture-only-token"}
	config.Contexts["selected"] = &clientcmdapi.Context{
		Cluster: "selected-cluster", AuthInfo: "fixture", Namespace: "ignored-kubeconfig-namespace",
	}
	config.Contexts["wrong"] = &clientcmdapi.Context{Cluster: "wrong-cluster", AuthInfo: "fixture"}
	config.CurrentContext = current

	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(*config, path); err != nil {
		t.Fatal(err)
	}

	return path
}

// TestKubeconfigReadOnlyHTTPFlow checks explicit and current context selection through local TLS fixtures.
// It verifies scoped authenticated reads and browser responses without raw metadata or fixture credentials.
func TestKubeconfigReadOnlyHTTPFlow(t *testing.T) {
	item := incidentObject(t, "incident-local")

	var calls atomic.Int32

	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)

		if r.Method != "GET" || r.Header.Get("Authorization") != "Bearer fixture-only-token" ||
			r.Header.Get("User-Agent") != "kubescout-dashboard/go" {
			t.Error("expected authenticated, read-only KubeScout dashboard request")
		}

		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/apis/kubescout.benc-uk.github.com/v1alpha1/namespaces/kubescout/incidents":
			writePage(w, []map[string]any{item}, "42", "")
		case "/apis/kubescout.benc-uk.github.com/v1alpha1/namespaces/kubescout/incidents/incident-local":
			_ = json.NewEncoder(w).Encode(item)
		default:
			t.Error("request escaped configured namespace or Incident scope")
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	for _, test := range []struct {
		source, context, current string
	}{
		{"explicit-file", "selected", "wrong"},
		{"KUBECONFIG", "selected", "wrong"},
		{"explicit-file", "", "selected"},
		{"KUBECONFIG", "", "selected"},
	} {
		t.Run(test.source+"/context="+test.context, func(t *testing.T) {
			path := writeKubeconfig(t, upstream, test.current)
			options := ClientOptions{AuthMode: "kubeconfig", Context: test.context}

			t.Setenv("KUBERNETES_SERVICE_HOST", "127.0.0.1")
			t.Setenv("KUBERNETES_SERVICE_PORT", "1")

			if test.source == "explicit-file" {
				options.Kubeconfig = path

				t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "must-not-read"))
			} else {
				t.Setenv("KUBECONFIG", path)
			}

			client, err := Client(options)
			if err != nil {
				t.Fatal(err)
			}

			reader, err := NewReader(client, "kubescout")
			if err != nil {
				t.Fatal(err)
			}

			server := httptest.NewServer(Handler(reader, log.New(io.Discard, "", 0)))
			defer server.Close()

			for _, endpoint := range []string{"/api/incidents", "/api/incidents/incident-local"} {
				response, err := http.Get(server.URL + endpoint)
				if err != nil {
					t.Fatal(err)
				}

				body, err := io.ReadAll(response.Body)
				response.Body.Close()

				if err != nil || response.StatusCode != 200 || !json.Valid(body) || !strings.Contains(string(body), "incident-local") {
					t.Fatalf("local API request failed: status=%d err=%v", response.StatusCode, err)
				}

				if strings.Contains(string(body), "fixture-only-token") || strings.Contains(string(body), "private-") {
					t.Fatal("browser response exposed credentials or raw metadata")
				}
			}
		})
	}

	if calls.Load() != 8 {
		t.Fatalf("unexpected upstream call count: %d", calls.Load())
	}
}

// TestAuthenticationNeverFallsBack checks that invalid authentication inputs neither contact Kubernetes nor leak details.
func TestAuthenticationNeverFallsBack(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("invalid configuration contacted Kubernetes")
		http.Error(w, "unexpected", 500)
	}))
	defer server.Close()

	path := writeKubeconfig(t, server, "wrong")
	noCurrent := writeKubeconfig(t, server, "")
	missingCurrent := writeKubeconfig(t, server, "missing")
	t.Setenv("KUBECONFIG", path)
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	bad := filepath.Join(t.TempDir(), "malformed")
	if err := os.WriteFile(bad, []byte("private-fixture-content: ["), 0600); err != nil {
		t.Fatal(err)
	}

	for _, options := range []ClientOptions{
		{AuthMode: "in-cluster"},
		{AuthMode: "kubeconfig", Context: "missing", Kubeconfig: path},
		{AuthMode: "kubeconfig", Context: "selected", Kubeconfig: path + "-missing"},
		{AuthMode: "kubeconfig", Context: "selected", Kubeconfig: bad},
		{AuthMode: "kubeconfig", Kubeconfig: noCurrent},
		{AuthMode: "kubeconfig", Kubeconfig: missingCurrent},
		{AuthMode: "in-cluster", Context: "selected"},
		{AuthMode: "in-cluster", Kubeconfig: path},
		{AuthMode: "auto"},
	} {
		client, err := Client(options)
		if err == nil || client != nil {
			t.Fatalf("unexpected authentication fallback for %+v", options)
		}

		if strings.Contains(err.Error(), "private-fixture") || strings.Contains(err.Error(), "fixture-only-token") {
			t.Fatal("authentication error leaked configuration contents")
		}
	}
}

// TestKubeconfigPreservesReadErrors verifies that forbidden TLS fixture reads become explicit access-denied errors.
func TestKubeconfigPreservesReadErrors(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,"message":"private fixture error"}`)
	}))
	defer server.Close()

	client, err := Client(ClientOptions{AuthMode: "kubeconfig", Context: "selected", Kubeconfig: writeKubeconfig(t, server, "wrong")})
	if err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(client, "kubescout")
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.List(context.Background())
	assertCode(t, err, "access_denied")
}
