/*
Purpose: Check agent CLI output, configuration failures and local Make contracts.
Flow: Offline HTTP fixtures, injected clients, command stubs and dry runs verify
readable execution summaries, colour controls, workload/PV/attachment scope, cluster-scoped incidents, current-context
selection, deployment preflights and lint command failures without live services.
Incident setup installs only the namespace and CRD, without deploying or calling AI.
Isolated command stubs verify agent and live-test targets need configuration, not confirmation flags.
*/
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

	"github.com/benc-uk/kubescout/internal/config"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// cliEnvironment isolates CLI tests from repository dotenv files and supplies synthetic model settings.
func cliEnvironment(t *testing.T) {
	t.Helper()
	t.Chdir(t.TempDir())
	t.Setenv("AZURE_OPENAI_ENDPOINT", "https://fixture/openai/v1/")
	t.Setenv("AZURE_OPENAI_API_KEY", "fixture")
	t.Setenv("AZURE_AI_MODEL_DEPLOYMENT_NAME", "fixture")
	t.Setenv("NAMESPACE_WATCH", "")
	t.Setenv("NAMESPACE_IGNORE", "")
}

// TestCLIHealthyScopeMakesNoModelOrIncidentWrites uses a local API fixture to check scoped reads
// and readable completion without model calls or incident writes.
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
		} else if r.URL.Path != "/api/v1/persistentvolumes" && r.URL.Path != "/apis/storage.k8s.io/v1/volumeattachments" &&
			!strings.Contains(r.URL.Path, "/namespaces/apps/") &&
			r.URL.Path != "/apis/kubescout.benc-uk.github.com/v1alpha1/incidents" {
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

	code := run(t.Context(), []string{"--once", "--ai-log-dir=" + t.TempDir(), "--auth-mode=kubeconfig", "--kubeconfig=" + path, "--namespace-watch=apps", "--observer-id=fixture"}, &out, &errOut)

	if code != 0 || !strings.Contains(out.String(), "Assessment completed: 0 investigations, 0 persisted incident changes") ||
		strings.Contains(out.String(), "[AI]") || strings.Contains(out.String(), "schema_version") {
		t.Fatalf("code=%d stdout=%s stderr=%s", code, &out, &errOut)
	}

	if strings.Contains(errOut.String(), "would create") {
		t.Fatal("healthy baseline published an incident")
	}
}

// TestCLIConsoleColour checks default/explicit ANSI and plain mode on redirected streams without live clients.
func TestCLIConsoleColour(t *testing.T) {
	cliEnvironment(t)

	for _, mode := range []string{"default", "always", "never"} {
		t.Run(mode, func(t *testing.T) {
			args := []string{"--auth-mode=in-cluster", "--observer-id=test", "--ai-log=false"}
			if mode != "default" {
				args = append(args, "--log-color="+mode)
			}

			var out, diag bytes.Buffer

			code := runWithClient(t.Context(), args, &out, &diag,
				func(config.Config) (dynamic.Interface, string, error) {
					return nil, "fixture", errors.New("fixture bootstrap failure")
				})
			if code != 1 || !strings.Contains(diag.String(), "[GO][BOOT]") {
				t.Fatal("unexpected fixture execution")
			}

			wantColor := mode != "never"
			if strings.Contains(out.String(), "\x1b[") != wantColor ||
				strings.Contains(diag.String(), "\x1b[") != wantColor {
				t.Fatalf("mode %s did not control both console streams", mode)
			}
		})
	}
}

// TestCLIConfigurationAndBootstrapFailures checks offline exit codes and incomplete once output,
// ensuring invalid or missing AI configuration never falls back to deterministic operation.
func TestCLIConfigurationAndBootstrapFailures(t *testing.T) {
	cliEnvironment(t)

	for _, test := range []struct {
		args []string
		code int
	}{
		{[]string{"--deterministic-only"}, 2},
		{[]string{"--report-namespace=old"}, 2},
		{[]string{"--incident-namespace=kubescout"}, 2},
		{[]string{"--simulate-incidents"}, 2},
		{[]string{"--auth-mode=in-cluster", "--namespace-watch=apps"}, 2},
		{[]string{"--help"}, 0},
	} {
		var out, errOut bytes.Buffer
		if code := run(t.Context(), test.args, &out, &errOut); code != test.code {
			t.Fatalf("code=%d: %s", code, &errOut)
		}

		if out.Len() != 0 {
			t.Fatal("invalid configuration emitted an incident")
		}

		if slices.Contains(test.args, "--help") && !strings.Contains(errOut.String(), "Usage of kubescout:") {
			t.Fatal("CLI help must use the KubeScout executable name")
		}

		if slices.Contains(test.args, "--help") && (strings.Contains(errOut.String(), "simulate-incidents") || strings.Contains(errOut.String(), "incident-namespace")) {
			t.Fatal("CLI help must not advertise removed incident options")
		}
	}

	var out, errOut bytes.Buffer

	code := runWithClient(t.Context(), []string{"--once", "--ai-log=false", "--auth-mode=in-cluster", "--namespace-watch=apps", "--observer-id=fixture"}, &out, &errOut,
		func(config.Config) (dynamic.Interface, string, error) {
			return nil, "fixture", errors.New("credentials unavailable")
		})

	if code != 1 || !strings.Contains(errOut.String(), "[GO][BOOT]") || !strings.Contains(errOut.String(), "execution incomplete") {
		t.Fatal("bootstrap failure missing readable diagnosis")
	}

	t.Setenv("AZURE_OPENAI_API_KEY", "")
	out.Reset()

	if code := run(t.Context(), []string{"--once", "--auth-mode=in-cluster", "--namespace-watch=apps", "--observer-id=fixture"}, &out, &errOut); code != 2 {
		t.Fatal("missing model credentials silently downgraded")
	}
}

// TestMakeLintRequiresPinnedToolAndPropagatesFailures checks missing-tool guidance and lint exits
// using an isolated Makefile and stub binary, without downloading tools or contacting services.
func TestMakeLintRequiresPinnedToolAndPropagatesFailures(t *testing.T) {
	makefile, err := os.ReadFile(filepath.Join("..", "..", "Makefile"))
	if err != nil {
		t.Fatal(err)
	}

	_, versionLine, ok := strings.Cut(string(makefile), "GOLANGCI_LINT_VERSION := ")
	if !ok {
		t.Fatal("linter version must be pinned")
	}

	version, _, _ := strings.Cut(versionLine, "\n")

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "Makefile"), makefile, 0600); err != nil {
		t.Fatal(err)
	}

	runLint := func() ([]byte, error) {
		cmd := exec.CommandContext(t.Context(), "make", "--no-print-directory", "-s", "lint", "GO=must-not-run")
		cmd.Dir = root

		return cmd.CombinedOutput()
	}

	result, err := runLint()
	if err == nil || !strings.Contains(string(result), "make lint-setup") {
		t.Fatalf("missing linter must fail with setup guidance: %v / %s", err, result)
	}

	binDir := filepath.Join(root, "build", "bin", "golangci-lint", strings.TrimSpace(version))
	if err := os.MkdirAll(binDir, 0700); err != nil {
		t.Fatal(err)
	}

	for _, exit := range []string{"0", "1"} {
		t.Run("exit="+exit, func(t *testing.T) {
			stub := "#!/bin/sh\nprintf '%s\\n' \"$*\"\nexit " + exit + "\n"
			if err := os.WriteFile(filepath.Join(binDir, "golangci-lint"), []byte(stub), 0700); err != nil {
				t.Fatal(err)
			}

			result, err := runLint()
			if (err != nil) != (exit != "0") || !strings.Contains(string(result), "run --config .golangci.yml ./...") {
				t.Fatalf("lint invocation or failure propagation changed: %v / %s", err, result)
			}
		})
	}
}

// TestMakeModelSecretUsesKubectlEnvFile stubs kubectl to verify direct dotenv-file provisioning
// without reading model credentials, creating a Secret or running the agent.
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

			expected := "apply -f deploy/namespace.yaml\n-n kubescout create secret generic kubescout-model --from-env-file=.env"
			if err != nil || strings.TrimSpace(string(output)) != expected {
				t.Fatalf("Secret creation did not use kubectl and .env directly: %s", output)
			}
		})
	}
}

// TestDeployExplainsMissingSecretWithoutApplying uses a failing kubectl stub to verify deployment
// stops at the Secret preflight with guidance instead of applying or activating resources.
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
			cmd := exec.CommandContext(t.Context(), "make", "-s", "deploy", "CONTEXT="+context, "KUBECTL="+stub)
			cmd.Dir = root
			output, err := cmd.CombinedOutput()
			guidance := "make model-secret"

			preflight := "-n kubescout get secret kubescout-model -o name"
			if err == nil || !strings.Contains(string(output), guidance) ||
				!strings.Contains(string(output), preflight) ||
				strings.Contains(string(output), "CONTEXT") || strings.Contains(string(output), "--context") ||
				strings.Contains(string(output), "apply -k") || strings.Contains(string(output), "scale deployment") {
				t.Fatalf("missing-Secret preflight did not stop with actionable guidance: %s", output)
			}
		})
	}
}

// TestMakeLocalKubernetesUsesCurrentContext records stubbed kubectl calls to ensure Incident setup,
// deployment, forwarding and cleanup targets use the current context without extra mutations.
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
		{"incidents-setup", []string{"apply -f deploy/namespace.yaml", "apply -f deploy/incidents-crd.yaml", "wait --for=condition=Established --timeout=60s crd/incidents.kubescout.benc-uk.github.com"}},
		{"deploy", []string{"-n kubescout get secret kubescout-model -o name", "apply -k deploy"}},
		{"undeploy", []string{"delete -f deploy/deployment.yaml -f deploy/rbac.yaml"}},
		{"dash-deploy", []string{"apply -k deploy/dashboard"}},
		{"dash-forward", []string{"-n kubescout port-forward --address=127.0.0.1 service/kubescout-dashboard 8000:8000"}},
		{"dash-undeploy", []string{"delete -f deploy/dashboard/deployment.yaml -f deploy/dashboard/service.yaml -f deploy/dashboard/rbac.yaml"}},
	} {
		for _, context := range []string{"", "selected"} {
			t.Run(test.target+"/context="+context, func(t *testing.T) {
				cmd := exec.CommandContext(t.Context(), "make", "-s", test.target, "CONTEXT="+context, "DASHBOARD_CONTEXT="+context, "KUBECTL="+stub, "GO=must-not-run")
				cmd.Dir = root

				output, err := cmd.CombinedOutput()
				if err != nil || !slices.Equal(strings.Split(strings.TrimSpace(string(output)), "\n"), test.calls) {
					t.Fatalf("incorrect Kubernetes commands: err=%v output=%s", err, output)
				}
			})
		}
	}
}

// TestMakeAICommandsNeedNoConfirmation executes isolated agent and live-test stubs without confirmation flags.
// Required observer/context validation remains intact, and no real binary, cluster or model is used.
func TestMakeAICommandsNeedNoConfirmation(t *testing.T) {
	makefile, err := os.ReadFile(filepath.Join("..", "..", "Makefile"))
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "build", "bin"), 0700); err != nil {
		t.Fatal(err)
	}

	for path, content := range map[string][]byte{
		"Makefile": makefile,
		"go-stub":  []byte("#!/bin/sh\nprintf 'context=%s args=%s\\n' \"$KUBESCOUT_LIVE_CONTEXT\" \"$*\"\n"),
		filepath.Join("build", "bin", "kubescout"): []byte("#!/bin/sh\nprintf 'agent: %s\\n' \"$*\"\n"),
	} {
		if err := os.WriteFile(filepath.Join(dir, path), content, 0700); err != nil {
			t.Fatal(err)
		}
	}

	for _, tt := range []struct {
		name, target, observer, context, want string
		fail                                  bool
	}{
		{name: "watch", target: "run", observer: "fixture", want: "agent: --auth-mode kubeconfig --observer-id fixture"},
		{name: "once", target: "once", observer: "fixture", want: "agent: --auth-mode kubeconfig --observer-id fixture --once"},
		{name: "collector", target: "integration", context: "fixture", want: "-run ^TestLive(Kubernetes|Connectivity)$ ./tests/cluster"},
		{name: "model", target: "integration-model", context: "fixture", want: "-run ^TestLiveModel$ ./tests/cluster"},
		{name: "container", target: "integration-deploy", context: "fixture", want: "-run ^TestLiveDeployment$ ./tests/cluster"},
		{name: "watch requires observer", target: "run", want: "Set OBSERVER_ID", fail: true},
		{name: "once requires observer", target: "once", want: "Set OBSERVER_ID", fail: true},
		{name: "collector requires context", target: "integration", want: "Set CONTEXT explicitly", fail: true},
		{name: "model requires context", target: "integration-model", want: "Set CONTEXT explicitly", fail: true},
		{name: "container requires context", target: "integration-deploy", want: "Set CONTEXT explicitly", fail: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), "make", "-s", tt.target, "GO="+filepath.Join(dir, "go-stub"),
				"OBSERVER_ID="+tt.observer, "CONTEXT="+tt.context)
			cmd.Dir = dir
			cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
			output, err := cmd.CombinedOutput()
			text := string(output)

			if (err != nil) != tt.fail || !strings.Contains(text, tt.want) {
				t.Fatalf("unexpected Make execution: err=%v output=%s", err, text)
			}

			if tt.fail && (strings.Contains(text, "agent:") || strings.Contains(text, "args=test")) {
				t.Fatalf("invalid configuration reached execution: %s", text)
			}

			if tt.context != "" && !strings.Contains(text, "context="+tt.context+" args=test") {
				t.Fatalf("live-test context was not forwarded: %s", text)
			}
		})
	}
}

// TestMakeNamespaceScopeDefaultsAndIgnore dry-runs Make to check all-namespace defaults
// and explicit ignore-list arguments without invoking the agent.
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
			cmd := exec.CommandContext(t.Context(), "make", "--no-print-directory", "-n", "once",
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

// TestMakeLocalRunUsesCurrentContext dry-runs local agent and dashboard targets to verify
// workload filters, namespace-free incident access and no context overrides without starting either process.
func TestMakeLocalRunUsesCurrentContext(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv("CONTEXT", "")
	t.Setenv("DASHBOARD_CONTEXT", "")
	t.Setenv("ARGS", "")

	for _, target := range []string{"run", "once", "dash-run"} {
		for _, context := range []string{"", "selected"} {
			t.Run(target+"/context="+context, func(t *testing.T) {
				args := []string{"--no-print-directory", "-n", target, "GO=go", "NAMESPACE_WATCH=apps,other", "NAMESPACE_IGNORE=", "DASHBOARD_NAMESPACE=unused"}
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

				if target == "dash-run" {
					if strings.Contains(string(output), "--namespace") {
						t.Fatalf("dashboard must not select a namespace: output=%s", output)
					}
				} else if !strings.Contains(string(output), `--namespace-watch "apps,other"`) {
					t.Fatalf("incorrect workload namespace: output=%s", output)
				}

				if strings.Contains(string(output), "--incident-namespace") {
					t.Fatalf("incidents must not select a namespace: output=%s", output)
				}
			})
		}
	}
}
