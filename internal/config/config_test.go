package config

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestConfiguration(t *testing.T) {
	base := []string{"--auth-mode", "kubeconfig", "--context", "kind-test", "--namespace", "apps", "--deterministic-only"}
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
		{"no context", []string{"--context="}, true},
		{"incluster context", []string{"--auth-mode=in-cluster"}, true},
		{"incluster", []string{"--auth-mode=in-cluster", "--context="}, false},
		{"wildcard ns", []string{"--namespace=*"}, true},
		{"empty ns", []string{"--namespace="}, true},
		{"extra args", []string{"unexpected"}, true},
		{"model not configured", []string{"--deterministic-only=false"}, true},
		{"zero model cap", []string{"--model-calls-per-hour=0"}, true},
		{"removed probe flag", []string{"--probe-file=/tmp/progress.json"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, err := Parse(append(append([]string{}, base...), test.args...), func(string) string { return "" }, io.Discard)
			if (err != nil) != test.invalid {
				t.Fatalf("config=%+v error=%v", c, err)
			}
			if test.name == "default watch" && c.Once {
				t.Fatal("watch must be default")
			}
		})
	}
	if _, err := Parse(nil, func(string) string { return "" }, io.Discard); err == nil {
		t.Fatal("scope must be explicit")
	}
}

func TestModelEndpointAndFallback(t *testing.T) {
	for _, endpoint := range []string{
		"https://resource.openai.azure.com/openai/v1/", "https://resource:443/openai/v1/",
		"http://resource/openai/v1/", "https://user:pass@resource/openai/v1/", "https://resource/api/projects/test",
		"https://resource:444/openai/v1/", "https://resource/openai/v1/?key=x",
	} {
		t.Run(endpoint, func(t *testing.T) {
			env := map[string]string{"AZURE_OPENAI_ENDPOINT": endpoint, "AZURE_OPENAI_API_KEY": "test-key", "FOUNDRY_MODEL_NAME": "test-model"}
			c, err := Parse([]string{"--auth-mode=in-cluster", "--namespace=apps"}, func(k string) string { return env[k] }, io.Discard)
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

func TestDotEnvOnlyRepositoryRoot(t *testing.T) {
	root := t.TempDir()
	write := func(path, data string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(root, "go.mod"), "module k8s-ops-agent\n")
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
