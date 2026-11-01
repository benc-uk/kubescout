/*
Purpose: Verify failure-injection menu wiring and the bounded OOM Pod fixture.
Flow: Run the shell script against a recording kubectl stub and decode its YAML.
Tests check selection, cleanup preconditions and failure handling without a cluster.
*/
package cluster

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

// runFailureScript captures scoped kubectl commands and stdin with a stub, never using cluster credentials.
func runFailureScript(t *testing.T, selection, fixtures string, failCreate bool) (string, string, string, error) {
	t.Helper()

	dir := t.TempDir()

	stub := `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$FIXTURE_DIR/calls"
[[ "$1" == --context && "$2" == fixture-context && "$3" == -n && "$4" == apps ]]
case "$5" in
  get) printf '%s' "$FIXTURE_LIST" ;;
  create)
    cat >> "$FIXTURE_DIR/bodies"
    printf '\n---\n' >> "$FIXTURE_DIR/bodies"
    if [[ "$FAIL_CREATE" == true ]]; then exit 1; fi
    ;;
  delete) cat >> "$FIXTURE_DIR/bodies" ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "kubectl"), []byte(stub), 0700); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"calls", "bodies"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}

	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FIXTURE_DIR", dir)
	t.Setenv("FIXTURE_LIST", fixtures)
	t.Setenv("FAIL_CREATE", map[bool]string{false: "false", true: "true"}[failCreate])

	cmd := exec.CommandContext(t.Context(), "bash", "../../scripts/inject-failures.sh", "fixture-context", "apps")
	cmd.Stdin = strings.NewReader(selection)
	out, runErr := cmd.CombinedOutput()

	calls, err := os.ReadFile(filepath.Join(dir, "calls"))
	if err != nil {
		t.Fatal(err)
	}

	bodies, err := os.ReadFile(filepath.Join(dir, "bodies"))
	if err != nil {
		t.Fatal(err)
	}

	return string(out), string(calls), string(bodies), runErr
}

// TestInjectOOMSelection checks standalone, combined and all-failure choices create a bounded, observable OOM Pod.
func TestInjectOOMSelection(t *testing.T) {
	for _, selection := range []string{"7", "1 7", "5"} {
		t.Run(selection, func(t *testing.T) {
			out, calls, bodies, err := runFailureScript(t, selection+"\n", "", false)
			if err != nil || !strings.Contains(out, "expected termination reason: OOMKilled") {
				t.Fatalf("OOM selection failed: %v / %s", err, out)
			}

			expectedCalls := map[string]int{"7": 1, "1 7": 2, "5": 5}[selection]
			if strings.Count(calls, "\n") != expectedCalls {
				t.Fatalf("unexpected kubectl commands: %s", calls)
			}

			pods := 0

			for _, document := range strings.Split(bodies, "\n---\n") {
				if strings.TrimSpace(document) == "" {
					continue
				}

				var pod corev1.Pod

				if err := yaml.Unmarshal([]byte(document), &pod); err != nil {
					t.Fatal(err)
				}

				if pod.Kind != "Pod" {
					continue
				}

				pods++

				if !strings.HasSuffix(pod.Name, "-oom") || pod.Labels["kubescout-demo"] != "true" ||
					pod.Spec.RestartPolicy != corev1.RestartPolicyNever || len(pod.Spec.Containers) != 1 ||
					pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
					t.Fatal("OOM fixture identity or execution policy changed")
				}

				container := pod.Spec.Containers[0]
				if !slices.Equal(container.Command, []string{"python3", "-c", "data = bytearray(128 * 1024 * 1024)"}) ||
					container.Resources.Limits.Memory().Value() != 64*1024*1024 ||
					container.Resources.Requests.Memory().Value() != 16*1024*1024 ||
					container.Resources.Limits.Cpu().MilliValue() != 100 {
					t.Fatal("OOM allocation or container resource bounds changed")
				}

				if pod.Spec.SecurityContext == nil || pod.Spec.SecurityContext.RunAsNonRoot == nil ||
					!*pod.Spec.SecurityContext.RunAsNonRoot || container.SecurityContext == nil ||
					container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem {
					t.Fatal("OOM fixture must remain non-root with a read-only root filesystem")
				}
			}

			if pods != 1 {
				t.Fatalf("expected exactly one OOM Pod, got %d", pods)
			}
		})
	}
}

// TestInjectOOMCleanup verifies removal selects only a matching fixture and sends its UID precondition.
func TestInjectOOMCleanup(t *testing.T) {
	fixtures := "Pod|health-demo-123-456-oom|fixture-uid|true\n" +
		"Pod|production-oom|other-uid|true\n" +
		"Pod|health-demo-123-789-oom|foreign-uid|false\n"

	out, calls, body, err := runFailureScript(t, "6\ny\n", fixtures, false)
	if err != nil {
		t.Fatalf("cleanup failed: %v / %s", err, out)
	}

	var options struct {
		Preconditions struct{ UID string }
	}

	if err := json.Unmarshal([]byte(body), &options); err != nil || options.Preconditions.UID != "fixture-uid" {
		t.Fatalf("missing UID deletion precondition: %s", body)
	}

	if strings.Count(calls, "\n") != 3 ||
		!strings.Contains(calls, "delete --raw=/api/v1/namespaces/apps/pods/health-demo-123-456-oom -f -") ||
		!strings.Contains(calls, "wait --for=delete pod/health-demo-123-456-oom --timeout=60s") {
		t.Fatalf("unexpected cleanup calls: %s", calls)
	}
}

// TestInjectOOMFailureAndSelection rejects mixed cleanup and propagates create failures without reporting success.
func TestInjectOOMFailureAndSelection(t *testing.T) {
	for _, selection := range []string{"6 7", "7"} {
		t.Run(selection, func(t *testing.T) {
			out, calls, _, err := runFailureScript(t, selection+"\n", "", true)
			if err == nil || strings.Contains(out, "Selected failures created.") {
				t.Fatalf("invalid selection or creation failure reported success: %s", out)
			}

			if selection == "6 7" && calls != "" {
				t.Fatalf("mixed cleanup reached Kubernetes: %s", calls)
			}
		})
	}
}
