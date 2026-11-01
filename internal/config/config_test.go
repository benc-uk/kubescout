/*
Purpose: Exercise configuration validation and repository-root dotenv precedence.
Flow: Offline argument tables, synthetic environment values and filesystem fixtures
check namespace filters, authentication, budgets, endpoint and model requirements.
Removed runtime modes are rejected without contacting Kubernetes or a model.
*/
package config

import (
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// testEnv supplies synthetic model settings so parser tests require no real credentials.
func testEnv(key string) string {
	return map[string]string{"AZURE_OPENAI_ENDPOINT": "https://fixture/openai/v1/", "AZURE_OPENAI_API_KEY": "fixture", "AZURE_AI_MODEL_DEPLOYMENT_NAME": "fixture"}[key]
}

// TestNamespaceSelection checks namespace normalization, flag precedence and conflicting-list
// rejection using synthetic settings, including admission of newly created namespace names.
func TestNamespaceSelection(t *testing.T) {
	base := []string{"--auth-mode=kubeconfig", "--observer-id=test"}

	for _, tt := range []struct {
		name    string
		args    []string
		env     map[string]string
		watch   []string
		ignore  []string
		invalid bool
	}{
		{name: "default all"},
		{name: "watch", args: []string{"--namespace-watch=beta, apps,beta"}, watch: []string{"apps", "beta"}},
		{name: "ignore", args: []string{"--namespace-ignore=kube-system,kube-public"}, ignore: []string{"kube-public", "kube-system"}},
		{name: "environment watch", env: map[string]string{"NAMESPACE_WATCH": "beta,apps"}, watch: []string{"apps", "beta"}},
		{name: "environment ignore", env: map[string]string{"NAMESPACE_IGNORE": "system"}, ignore: []string{"system"}},
		{name: "flag replaces environment", args: []string{"--namespace-watch=apps"}, env: map[string]string{"NAMESPACE_WATCH": "invalid,name/"}, watch: []string{"apps"}},
		{name: "ignore flag replaces environment", args: []string{"--namespace-ignore=system"}, env: map[string]string{"NAMESPACE_IGNORE": "other"}, ignore: []string{"system"}},
		{name: "legacy repeatable", args: []string{"--namespace=beta", "--namespace=apps"}, watch: []string{"apps", "beta"}},
		{name: "both flags", args: []string{"--namespace-watch=apps", "--namespace-ignore=system"}, invalid: true},
		{name: "both environment", env: map[string]string{"NAMESPACE_WATCH": "apps", "NAMESPACE_IGNORE": "system"}, invalid: true},
		{name: "mixed watch", args: []string{"--namespace-watch=apps"}, env: map[string]string{"NAMESPACE_IGNORE": "system"}, invalid: true},
		{name: "mixed ignore", args: []string{"--namespace-ignore=system"}, env: map[string]string{"NAMESPACE_WATCH": "apps"}, invalid: true},
		{name: "invalid environment", env: map[string]string{"NAMESPACE_WATCH": "apps,,other"}, invalid: true},
		{name: "empty", args: []string{"--namespace-watch="}, invalid: true},
		{name: "blank", args: []string{"--namespace-ignore= "}, invalid: true},
		{name: "trailing comma", args: []string{"--namespace-watch=apps,"}, invalid: true},
		{name: "empty entry", args: []string{"--namespace-ignore=system,,other"}, invalid: true},
		{name: "wildcard", args: []string{"--namespace-watch=*"}, invalid: true},
		{name: "invalid name", args: []string{"--namespace-ignore=UPPER"}, invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, err := Parse(append(slices.Clone(base), tt.args...), func(key string) string {
				if value, ok := tt.env[key]; ok {
					return value
				}

				return testEnv(key)
			}, io.Discard)
			if (err != nil) != tt.invalid {
				t.Fatalf("unexpected configuration error: %v", err)
			}

			if tt.invalid {
				return
			}

			if !slices.Equal(c.NamespaceWatch, tt.watch) || !slices.Equal(c.NamespaceIgnore, tt.ignore) {
				t.Fatalf("watch=%v ignore=%v", c.NamespaceWatch, c.NamespaceIgnore)
			}

			for _, ns := range []string{"apps", "beta", "system", "newly-created", ""} {
				want := ns != "" && (len(tt.watch) == 0 || slices.Contains(tt.watch, ns)) && !slices.Contains(tt.ignore, ns)
				if c.WatchesNamespace(ns) != want {
					t.Fatalf("incorrect scope for %q", ns)
				}
			}

			if c.NamespaceScope() == "" {
				t.Fatal("namespace diagnostic is empty")
			}
		})
	}
}

// TestConfiguration verifies offline defaults and validation of authentication, scope and budgets,
// including rejection of removed flags and preservation of the current kubeconfig context.
func TestConfiguration(t *testing.T) {
	base := []string{"--auth-mode", "kubeconfig", "--context", "kind-test", "--namespace", "apps", "--observer-id", "test"}

	for _, test := range []struct {
		name    string
		args    []string
		invalid bool
	}{
		{"default watch", nil, false},
		{"once", []string{"--once"}, false},
		{"zero request", []string{"--request-timeout=0s"}, true},
		{"deadline ordering", []string{"--request-timeout=121s"}, true},
		{"negative grace", []string{"--startup-grace=-1s"}, true},
		{"zero grace", []string{"--startup-grace=0s"}, false},
		{"zero budget", []string{"--max-objects=0"}, true},
		{"unbounded batching", []string{"--batch-window=0s"}, true},
		{"current context", []string{"--context="}, false},
		{"blank context", []string{"--context= "}, true},
		{"incluster context", []string{"--auth-mode=in-cluster"}, true},
		{"incluster", []string{"--auth-mode=in-cluster", "--context="}, false},
		{"wildcard ns", []string{"--namespace=*"}, true},
		{"empty ns", []string{"--namespace="}, true},
		{"incident namespace", []string{"--incident-namespace=observer"}, false},
		{"empty incident namespace", []string{"--incident-namespace="}, true},
		{"wildcard incident namespace", []string{"--incident-namespace=*"}, true},
		{"invalid incident namespace", []string{"--incident-namespace=bad/name"}, true},
		{"missing observer", []string{"--observer-id="}, true},
		{"old report namespace", []string{"--report-namespace=old"}, true},
		{"extra args", []string{"unexpected"}, true},
		{"removed deterministic mode", []string{"--deterministic-only"}, true},
		{"zero model cap", []string{"--model-calls-per-hour=0"}, true},
		{"removed probe flag", []string{"--probe-file=/tmp/progress.json"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, err := Parse(append(append([]string{}, base...), test.args...), testEnv, io.Discard)
			if (err != nil) != test.invalid {
				t.Fatalf("config=%+v error=%v", c, err)
			}

			if test.name == "default watch" && c.Once {
				t.Fatal("watch must be default")
			}

			if test.name == "default watch" && c.IncidentNamespace != "k8s-health" {
				t.Fatal("unexpected report namespace default")
			}

			if test.name == "incident namespace" && c.IncidentNamespace != "observer" {
				t.Fatal("report namespace override ignored")
			}
		})
	}

	if _, err := Parse(nil, func(string) string { return "" }, io.Discard); err == nil {
		t.Fatal("scope must be explicit")
	}

	c, err := Parse([]string{"--auth-mode=kubeconfig", "--namespace=apps", "--observer-id=test"}, testEnv, io.Discard)
	if err != nil || c.Context != "" {
		t.Fatalf("omitted context must use kubeconfig's current context: %v", err)
	}
}

// TestModelEndpointAndFallback validates HTTPS endpoint restrictions and the legacy model-name
// fallback using synthetic settings without connecting to a model.
func TestModelEndpointAndFallback(t *testing.T) {
	for _, endpoint := range []string{
		"https://resource.openai.azure.com/openai/v1/", "https://resource:443/openai/v1/",
		"http://resource/openai/v1/", "https://user:pass@resource/openai/v1/", "https://resource/api/projects/test",
		"https://resource:444/openai/v1/", "https://resource/openai/v1/?key=x",
	} {
		t.Run(endpoint, func(t *testing.T) {
			env := map[string]string{"AZURE_OPENAI_ENDPOINT": endpoint, "AZURE_OPENAI_API_KEY": "test-key", "FOUNDRY_MODEL_NAME": "test-model"}
			c, err := Parse([]string{"--auth-mode=in-cluster", "--namespace=apps", "--observer-id=test"}, func(k string) string { return env[k] }, io.Discard)

			valid := endpoint == "https://resource.openai.azure.com/openai/v1/" || endpoint == "https://resource:443/openai/v1/"
			if (err == nil) != valid {
				t.Fatalf("unexpected error: %v", err)
			}

			if valid && c.Model != "test-model" {
				t.Fatal("fallback not used")
			}
		})
	}
}

// TestDotEnvOnlyRepositoryRoot uses synthetic filesystem fixtures to check root-only dotenv loading,
// existing-environment precedence and errors for malformed files.
func TestDotEnvOnlyRepositoryRoot(t *testing.T) {
	root := t.TempDir()
	write := func(path, data string) {
		t.Helper()

		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(root, "go.mod"), "module github.com/benc-uk/kubescout\n")
	write(filepath.Join(root, ".env"), "K8S_HEALTH_TEST_VALUE=file\n")

	sub := filepath.Join(root, "sub")
	if err := os.Mkdir(sub, 0700); err != nil {
		t.Fatal(err)
	}

	write(filepath.Join(sub, ".env"), "K8S_HEALTH_TEST_VALUE=wrong\n")
	t.Setenv("K8S_HEALTH_TEST_VALUE", "environment")

	if err := LoadDotEnv(sub, filepath.Join(root, "bin", "health")); err != nil {
		t.Fatal(err)
	}

	if os.Getenv("K8S_HEALTH_TEST_VALUE") != "environment" {
		t.Fatal("overrode existing environment")
	}

	if err := os.Unsetenv("K8S_HEALTH_TEST_VALUE"); err != nil {
		t.Fatal(err)
	}

	if err := LoadDotEnv(sub, filepath.Join(root, "bin", "health")); err != nil {
		t.Fatal(err)
	}

	if os.Getenv("K8S_HEALTH_TEST_VALUE") != "file" {
		t.Fatal("did not load root .env")
	}

	write(filepath.Join(root, ".env"), "malformed='")

	if err := LoadDotEnv(root, root); err == nil {
		t.Fatal("invalid dotenv silently ignored")
	}
}
