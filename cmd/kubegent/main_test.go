package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"k8s-ops-agent/internal/config"
	"k8s-ops-agent/internal/report"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func cliEnvironment(t *testing.T) {
	t.Helper()
	t.Chdir(t.TempDir())
	t.Setenv("AZURE_OPENAI_ENDPOINT", "https://fixture/openai/v1/")
	t.Setenv("AZURE_OPENAI_API_KEY", "fixture")
	t.Setenv("AZURE_AI_MODEL_DEPLOYMENT_NAME", "fixture")
	t.Setenv("NAMESPACE_WATCH", "")
	t.Setenv("NAMESPACE_IGNORE", "")
}
func TestCLIHealthyScopeMakesNoModelOrIncidentWrites(t *testing.T) {
	cliEnvironment(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error("healthy kubeconfig mode wrote to Kubernetes")
		}
		items := []any{}
		if strings.HasSuffix(r.URL.Path, "/nodes") {
			items = append(items, map[string]any{"apiVersion": "v1", "kind": "Node", "metadata": map[string]any{"name": "node", "uid": "id", "resourceVersion": "1"},
				"status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}})
		} else if !strings.Contains(r.URL.Path, "/namespaces/apps/") {
			t.Error("unexpected scope")
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{"resourceVersion": "1"}, "items": items})
	}))
	defer server.Close()
	kubeconfig := clientcmdapi.NewConfig()
	kubeconfig.Clusters["fixture"] = &clientcmdapi.Cluster{Server: server.URL}
	kubeconfig.Contexts["fixture"] = &clientcmdapi.Context{Cluster: "fixture"}
	kubeconfig.CurrentContext = "fixture"
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(*kubeconfig, path); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := run(t.Context(), []string{"--once", "--auth-mode=kubeconfig", "--kubeconfig=" + path, "--namespace=apps", "--observer-id=fixture"}, &out, &errOut)
	var result report.Result
	if code != 0 || bytes.Count(out.Bytes(), []byte("\n")) != 1 || json.Unmarshal(out.Bytes(), &result) != nil || len(result.Changes) != 0 || result.Status != "completed" {
		t.Fatalf("code=%d stdout=%s stderr=%s", code, &out, &errOut)
	}
	if strings.Contains(errOut.String(), "would create") {
		t.Fatal("healthy baseline published an incident")
	}
}
func TestCLIConfigurationAndBootstrapFailures(t *testing.T) {
	cliEnvironment(t)
	for _, test := range []struct {
		args []string
		code int
	}{
		{[]string{"--deterministic-only"}, 2},
		{[]string{"--report-namespace=old"}, 2},
		{[]string{"--auth-mode=in-cluster", "--namespace=apps"}, 2},
		{[]string{"--help"}, 0},
	} {
		var out, errOut bytes.Buffer
		if code := run(t.Context(), test.args, &out, &errOut); code != test.code {
			t.Fatalf("code=%d: %s", code, &errOut)
		}
		if out.Len() != 0 {
			t.Fatal("invalid configuration emitted an incident")
		}
	}

	var out, errOut bytes.Buffer
	code := runWithClient(t.Context(), []string{"--once", "--auth-mode=in-cluster", "--namespace=apps", "--observer-id=fixture"}, &out, &errOut,
		func(config.Config) (dynamic.Interface, string, error) {
			return nil, "fixture", errors.New("credentials unavailable")
		})
	var result report.Result
	if code != 1 || json.Unmarshal(out.Bytes(), &result) != nil || result.Status != "incomplete" || len(result.Changes) != 0 {
		t.Fatal("bootstrap failure violated once envelope")
	}
	t.Setenv("AZURE_OPENAI_API_KEY", "")
	out.Reset()
	if code := run(t.Context(), []string{"--once", "--auth-mode=in-cluster", "--namespace=apps", "--observer-id=fixture"}, &out, &errOut); code != 2 {
		t.Fatal("missing model credentials silently downgraded")
	}
}

func TestMakeModelSecretUsesKubectlEnvFile(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	stub := filepath.Join(t.TempDir(), "kubectl")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\nprintf '%s\\n' \"$*\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, context := range []string{"selected", ""} {
		t.Run("context="+context, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), "make", "-s", "model-secret", "CONTEXT="+context, "KUBECTL="+stub, "GO=must-not-run")
			cmd.Dir = root
			output, err := cmd.CombinedOutput()
			expected := "apply -f deploy/namespace.yaml\n-n k8s-health create secret generic k8s-health-model --from-env-file=.env"
			if err != nil || strings.TrimSpace(string(output)) != expected {
				t.Fatalf("Secret creation did not use kubectl and .env directly: %s", output)
			}
		})
	}
}

func TestDeployExplainsMissingSecretWithoutApplying(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	stub := filepath.Join(t.TempDir(), "kubectl")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\nprintf 'fixture kubectl: %s\\n' \"$*\"\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, context := range []string{"selected", ""} {
		t.Run("context="+context, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), "make", "-s", "deploy", "ALLOW_MODEL=yes", "CONTEXT="+context, "KUBECTL="+stub)
			cmd.Dir = root
			output, err := cmd.CombinedOutput()
			guidance := "make model-secret"
			preflight := "-n k8s-health get secret k8s-health-model -o name"
			if err == nil || !strings.Contains(string(output), guidance) ||
				!strings.Contains(string(output), preflight) ||
				strings.Contains(string(output), "CONTEXT") || strings.Contains(string(output), "--context") ||
				strings.Contains(string(output), "apply -k") || strings.Contains(string(output), "scale deployment") {
				t.Fatalf("missing-Secret preflight did not stop with actionable guidance: %s", output)
			}
		})
	}
}

func TestMakeLocalKubernetesUsesCurrentContext(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	stub := filepath.Join(t.TempDir(), "kubectl")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\nprintf '%s\\n' \"$*\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		target string
		calls  []string
	}{
		{"deploy", []string{"-n k8s-health get secret k8s-health-model -o name", "apply -k deploy", "-n k8s-health scale deployment/k8s-health --replicas=1"}},
		{"undeploy", []string{"delete -f deploy/deployment.yaml -f deploy/rbac.yaml"}},
		{"dashboard-deploy", []string{"apply -k deploy/dashboard"}},
		{"dashboard-forward", []string{"-n k8s-health port-forward --address=127.0.0.1 service/k8s-dashboard 8000:8000"}},
		{"dashboard-undeploy", []string{"delete -f deploy/dashboard/deployment.yaml -f deploy/dashboard/service.yaml -f deploy/dashboard/rbac.yaml"}},
	} {
		for _, context := range []string{"", "selected"} {
			t.Run(test.target+"/context="+context, func(t *testing.T) {
				cmd := exec.CommandContext(t.Context(), "make", "-s", test.target, "ALLOW_MODEL=yes", "CONTEXT="+context, "DASHBOARD_CONTEXT="+context, "KUBECTL="+stub)
				cmd.Dir = root
				output, err := cmd.CombinedOutput()
				if err != nil || !slices.Equal(strings.Split(strings.TrimSpace(string(output)), "\n"), test.calls) {
					t.Fatalf("incorrect Kubernetes commands: err=%v output=%s", err, output)
				}
			})
		}
	}
}

func TestMakeNamespaceScopeDefaultsAndIgnore(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, ignore string
	}{
		{name: "all"},
		{name: "ignore", ignore: "system,monitoring"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), "make", "--no-print-directory", "-n", "once", "ALLOW_MODEL=yes",
				"NAMESPACE_WATCH=", "NAMESPACE_IGNORE="+tt.ignore, "ARGS=")
			cmd.Dir = root
			output, err := cmd.CombinedOutput()
			if err != nil || strings.Contains(string(output), "--namespace-watch") || strings.Contains(string(output), "Set NAMESPACE") {
				t.Fatalf("invalid namespace defaults: %v output=%s", err, output)
			}
			if tt.ignore == "" && strings.Contains(string(output), "--namespace-ignore") ||
				tt.ignore != "" && !strings.Contains(string(output), `--namespace-ignore "`+tt.ignore+`"`) {
				t.Fatalf("incorrect ignore arguments: %s", output)
			}
		})
	}
}
func TestMakeLocalRunUsesCurrentContext(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONTEXT", "")
	t.Setenv("DASHBOARD_CONTEXT", "")
	t.Setenv("ARGS", "")
	for _, target := range []string{"run", "once", "dashboard-run"} {
		for _, context := range []string{"", "selected"} {
			t.Run(target+"/context="+context, func(t *testing.T) {
				args := []string{"--no-print-directory", "-n", target, "ALLOW_MODEL=yes", "GO=go", "NAMESPACE_WATCH=apps,other", "NAMESPACE_IGNORE=", "DASHBOARD_NAMESPACE=unused"}
				if context != "" {
					args = append(args, "CONTEXT="+context, "DASHBOARD_CONTEXT="+context)
				}
				cmd := exec.CommandContext(t.Context(), "make", args...)
				cmd.Dir = root
				output, err := cmd.CombinedOutput()
				if err != nil || strings.Contains(string(output), "--context") || strings.Contains(string(output), "Set CONTEXT") ||
					strings.Contains(string(output), "Set DASHBOARD_CONTEXT") {
					t.Fatalf("incorrect local run context flags: err=%v output=%s", err, output)
				}
				namespaceFlag := `--namespace-watch "apps,other"`
				if target == "dashboard-run" {
					namespaceFlag = "--namespace=k8s-health"
				}
				if !strings.Contains(string(output), namespaceFlag) || strings.Contains(string(output), "--namespace unused") {
					t.Fatalf("incorrect namespace: output=%s", output)
				}
			})
		}
	}
}
