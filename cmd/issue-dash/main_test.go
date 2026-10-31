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

	"k8s-ops-agent/internal/dashboard"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

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
	}
}

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
		done <- run(ctx, []string{"--namespace=issues"}, &output,
			func(dashboard.ClientOptions) (dynamic.Interface, error) { return client, nil },
			func(network, address string) (net.Listener, error) {
				listener, err := net.Listen(network, "127.0.0.1:0")
				cancel()
				return listener, err
			})
	}()
	select {
	case code := <-done:
		if code != 0 || !strings.Contains(output.String(), "namespace issues") {
			t.Fatalf("shutdown code %d: %s", code, &output)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("shutdown timed out")
	}
}

func TestLocalOptionsAndLoopback(t *testing.T) {
	client, err := dynamic.NewForConfig(&rest.Config{Host: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	code := run(context.Background(), []string{
		"--auth-mode=kubeconfig", "--context=selected", "--kubeconfig=fixture", "--namespace=issues",
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

func TestKubeconfigServerLifecycle(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/apis/health.k8s-ops-agent.io/v1alpha1/namespaces/issues/clusterissues" {
			t.Error("unexpected Kubernetes request")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"health.k8s-ops-agent.io/v1alpha1","kind":"ClusterIssueList","metadata":{"resourceVersion":"42"},"items":[]}`)
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
			"--auth-mode=kubeconfig", "--kubeconfig=" + path, "--namespace=issues",
		}, &output, dashboard.Client, func(network, address string) (net.Listener, error) {
			if network != "tcp" || address != "127.0.0.1:8000" {
				t.Error("incorrect local listener")
			}
			return listener, nil
		})
	}()
	httpClient := &http.Client{Timeout: 3 * time.Second}
	for _, path := range []string{"/", "/api/issues"} {
		response, err := httpClient.Get("http://" + listener.Addr().String() + path)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != 200 {
			t.Fatalf("local request failed: status=%d err=%v", response.StatusCode, err)
		}
		if path == "/api/issues" && (!strings.Contains(string(body), `"namespace":"issues"`) || !strings.Contains(string(body), `"items":[]`)) {
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
