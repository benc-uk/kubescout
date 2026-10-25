package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s-ops-agent/internal/report"
)

func TestCLIOnceWithExplicitKubeconfigContext(t *testing.T) {
	t.Chdir(t.TempDir())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Query().Get("watch") != "" {
			t.Error("once mode attempted a write or watch")
		}
		w.Header().Set("Content-Type", "application/json")
		items := []any{}
		if strings.HasSuffix(r.URL.Path, "/nodes") {
			items = append(items, map[string]any{
				"apiVersion": "v1", "kind": "Node",
				"metadata": map[string]any{"name": "node", "uid": "node-id", "resourceVersion": "1"},
				"status":   map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}},
			})
		} else if !strings.Contains(r.URL.Path, "/namespaces/apps/") {
			t.Error("unexpected namespace")
		}
		json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{"resourceVersion": "1"}, "items": items})
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "kubeconfig")
	kubeconfig := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: fixture
  cluster:
    server: %s
contexts:
- name: selected
  context:
    cluster: fixture
- name: wrong
  context:
    cluster: does-not-exist
current-context: wrong
`, server.URL)
	if err := os.WriteFile(path, []byte(kubeconfig), 0600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := run(t.Context(), []string{"--once", "--auth-mode=kubeconfig", "--context=selected", "--namespace=apps", "--deterministic-only", "--kubeconfig=" + path}, &out, &errOut)
	if code != 0 {
		t.Fatalf("code=%d stderr=%s stdout=%s", code, &errOut, &out)
	}
	if bytes.Count(out.Bytes(), []byte("\n")) != 1 {
		t.Fatal("stdout is not one report")
	}
	var r report.Report
	if err := json.Unmarshal(out.Bytes(), &r); err != nil {
		t.Fatal(err)
	}
	if r.Health != "healthy" || !r.CollectionComplete || r.Cluster != "selected" || r.Analysis.Status != "skipped" {
		t.Fatal(r)
	}
}

func TestCLIOutcomes(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, test := range []struct {
		name    string
		args    []string
		code    int
		reports int
	}{
		{"help", []string{"--help"}, 0, 0},
		{"invalid scope", []string{"--deterministic-only"}, 2, 0},
		{"removed probe command", []string{"probe", "--kind=ready"}, 2, 0},
		{"invalid authentication", []string{"--auth-mode=other", "--namespace=apps", "--deterministic-only"}, 2, 0},
		{"missing kubeconfig", []string{"--once", "--auth-mode=kubeconfig", "--context=kind-test", "--namespace=apps", "--deterministic-only", "--kubeconfig=" + filepath.Join(t.TempDir(), "not-present")}, 1, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			if code := run(t.Context(), test.args, &out, &errOut); code != test.code {
				t.Fatalf("exit=%d, stderr=%s", code, &errOut)
			}
			if bytes.Count(out.Bytes(), []byte("\n")) != test.reports {
				t.Fatalf("wrong stdout shape: %s", &out)
			}
			if test.reports == 1 {
				var r report.Report
				if err := json.Unmarshal(out.Bytes(), &r); err != nil {
					t.Fatal(err)
				}
				if r.Health != "unknown" || r.Execution != "incomplete" || r.CollectionComplete {
					t.Fatal("credential error was hidden")
				}
			}
		})
	}
}
