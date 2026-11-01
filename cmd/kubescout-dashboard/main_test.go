/*
Purpose: Verify dashboard CLI validation, startup failures and HTTP shutdown.
Flow: Offline injected clients, listeners and a local Kubernetes HTTP fixture
exercise authentication options, loopback binding and namespaced Incident reads.
Tests use no live cluster or model and do not publish incident conclusions.
*/
package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/dashboard"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// TestCLIValidation checks help and invalid dashboard arguments before any credentials are loaded.
func TestCLIValidation(t *testing.T) {
	for _, test := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0},
		{[]string{"--namespace="}, 2},
		{[]string{"--namespace=*"}, 2},
		{[]string{"--namespace=UPPER"}, 2},
		{[]string{"--namespace=one,two"}, 2},
		{[]string{"--port=9000"}, 2},
		{[]string{"--auth-mode=kubeconfig", "--context= "}, 2},
		{[]string{"--auth-mode=auto"}, 2},
		{[]string{"--context=selected"}, 2},
		{[]string{"--kubeconfig=fixture"}, 2},
		{[]string{"--auth-mode=in-cluster", "--context=selected"}, 2},
		{[]string{"unexpected"}, 2},
	} {
		var output bytes.Buffer

		code := run(context.Background(), test.args, &output, func(dashboard.ClientOptions) (dynamic.Interface, error) {
			t.Fatal("credentials loaded before validation")
			return nil, nil
		}, nil)
		if code != test.code || output.Len() == 0 {
			t.Fatalf("args %v: %d / %s", test.args, code, &output)
		}

		if test.code == 0 && !strings.Contains(output.String(), "Usage of kubescout-dashboard:") {
			t.Fatal("CLI help must use the KubeScout dashboard executable name")
		}
	}
}

// TestBootstrapFailure injects authentication and listener failures to verify safe diagnostics
// and nonzero exit codes without contacting Kubernetes.
func TestBootstrapFailure(t *testing.T) {
	var output bytes.Buffer

	code := run(context.Background(), nil, &output, func(dashboard.ClientOptions) (dynamic.Interface, error) {
		return nil, errors.New("private credentials")
	}, nil)
	if code != 1 || strings.Contains(output.String(), "private") {
		t.Fatalf("bad startup failure %d: %s", code, &output)
	}

	client, err := dynamic.NewForConfig(&rest.Config{Host: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}

	output.Reset()

	code = run(context.Background(), nil, &output, func(dashboard.ClientOptions) (dynamic.Interface, error) { return client, nil },
		func(network, address string) (net.Listener, error) {
			if network != "tcp" || address != ":8000" {
				t.Error("must bind port 8000")
			}

			return nil, errors.New("address in use")
		})
	if code != 1 || !strings.Contains(output.String(), "port 8000") {
		t.Fatalf("bad bind failure %d: %s", code, &output)
	}
}

// TestGracefulExit cancels a local dashboard listener and checks bounded, successful shutdown.
func TestGracefulExit(t *testing.T) {
	client, err := dynamic.NewForConfig(&rest.Config{Host: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var output bytes.Buffer

	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"--namespace=kubescout"}, &output,
			func(dashboard.ClientOptions) (dynamic.Interface, error) { return client, nil },
			func(network, address string) (net.Listener, error) {
				listener, err := net.Listen(network, "127.0.0.1:0")

				cancel()

				return listener, err
			})
	}()

	select {
	case code := <-done:
		if code != 0 || !strings.Contains(output.String(), "namespace kubescout") {
			t.Fatalf("shutdown code %d: %s", code, &output)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("shutdown timed out")
	}
}

// TestLocalOptionsAndLoopback verifies kubeconfig options reach the injected client factory
// and require a loopback listener, stopping before serving or contacting Kubernetes.
func TestLocalOptionsAndLoopback(t *testing.T) {
	client, err := dynamic.NewForConfig(&rest.Config{Host: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer

	code := run(context.Background(), []string{
		"--auth-mode=kubeconfig", "--context=selected", "--kubeconfig=fixture", "--namespace=kubescout",
	}, &output, func(options dashboard.ClientOptions) (dynamic.Interface, error) {
		if options != (dashboard.ClientOptions{AuthMode: "kubeconfig", Context: "selected", Kubeconfig: "fixture"}) {
			t.Fatalf("incorrect options: %+v", options)
		}

		return client, nil
	}, func(network, address string) (net.Listener, error) {
		if network != "tcp" || address != "127.0.0.1:8000" {
			t.Fatal("local dashboard must bind only loopback port 8000")
		}

		return nil, errors.New("fixture: stop before serving")
	})
	if code != 1 || !strings.Contains(output.String(), "port 8000") {
		t.Fatalf("unexpected result %d: %s", code, &output)
	}
}

// TestKubeconfigServerLifecycle serves dashboard requests against a local Kubernetes fixture
// and checks namespaced Incident output and graceful cancellation without live cluster access.
func TestKubeconfigServerLifecycle(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/apis/kubescout.benc-uk.github.com/v1alpha1/namespaces/kubescout/incidents" {
			t.Error("unexpected Kubernetes request")
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"kubescout.benc-uk.github.com/v1alpha1","kind":"IncidentList","metadata":{"resourceVersion":"42"},"items":[]}`)
	}))
	defer upstream.Close()

	config := clientcmdapi.NewConfig()
	config.Clusters["fixture"] = &clientcmdapi.Cluster{Server: upstream.URL}
	config.Contexts["local-fixture"] = &clientcmdapi.Context{Cluster: "fixture"}
	config.CurrentContext = "local-fixture"

	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(*config, path); err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var output bytes.Buffer

	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{
			"--auth-mode=kubeconfig", "--kubeconfig=" + path, "--namespace=kubescout",
		}, &output, dashboard.Client, func(network, address string) (net.Listener, error) {
			if network != "tcp" || address != "127.0.0.1:8000" {
				t.Error("incorrect local listener")
			}

			return listener, nil
		})
	}()

	httpClient := &http.Client{Timeout: 3 * time.Second}
	for _, path := range []string{"/", "/api/incidents"} {
		response, err := httpClient.Get("http://" + listener.Addr().String() + path)
		if err != nil {
			t.Fatal(err)
		}

		body, err := io.ReadAll(response.Body)
		response.Body.Close()

		if err != nil || response.StatusCode != 200 {
			t.Fatalf("local request failed: status=%d err=%v", response.StatusCode, err)
		}

		if path == "/api/incidents" && (!strings.Contains(string(body), `"namespace":"kubescout"`) || !strings.Contains(string(body), `"incidents":[]`)) {
			t.Fatal("incorrect local API response")
		}
	}

	cancel()

	select {
	case code := <-done:
		if code != 0 || !strings.Contains(output.String(), "127.0.0.1:8000") {
			t.Fatalf("local server failed: %d / %s", code, &output)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("local server did not stop")
	}
}
