package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"k8s-ops-agent/internal/health"
	"k8s.io/apimachinery/pkg/util/validation"
)

type Config struct {
	Once, Deterministic                                                  bool
	Auth, Context, Kubeconfig, ReportFile                                string
	Namespaces                                                           []string
	RequestTimeout, RunTimeout, BatchWindow, CheckInterval, WatchTimeout time.Duration
	MaxObjects, MaxInputChars, MaxOutputTokens, CallsPerHour             int
	Endpoint, APIKey, Model                                              string
	Policy                                                               health.Policy
}

type namespaces []string

func (n *namespaces) String() string { return strings.Join(*n, ",") }
func (n *namespaces) Set(s string) error {
	if len(validation.IsDNS1123Label(s)) != 0 {
		return errors.New("namespace must be a valid explicit namespace name")
	}
	if !slices.Contains(*n, s) {
		*n = append(*n, s)
	}
	return nil
}

func Parse(args []string, getenv func(string) string, stderr io.Writer) (Config, error) {
	c := Config{Policy: health.DefaultPolicy()}
	f := flag.NewFlagSet("k8s-health", flag.ContinueOnError)
	f.SetOutput(stderr)
	var ns namespaces
	var restart int
	f.BoolVar(&c.Once, "once", false, "Collect and report once instead of watching")
	f.BoolVar(&c.Deterministic, "deterministic-only", false, "Never call the model")
	f.StringVar(&c.Auth, "auth-mode", "", "Required: kubeconfig or in-cluster")
	f.StringVar(&c.Context, "context", "", "Explicit kubeconfig context")
	f.StringVar(&c.Kubeconfig, "kubeconfig", "", "Optional kubeconfig path")
	f.Var(&ns, "namespace", "Required namespace, repeat to select more")
	f.StringVar(&c.ReportFile, "report-file", "", "Atomically replace this file with the latest report")
	f.DurationVar(&c.RequestTimeout, "request-timeout", 10*time.Second, "API/model request timeout")
	f.DurationVar(&c.RunTimeout, "run-timeout", 120*time.Second, "Once deadline or initial informer synchronisation deadline")
	f.DurationVar(&c.BatchWindow, "batch-window", 5*time.Second, "Maximum wait to batch resource changes")
	f.DurationVar(&c.CheckInterval, "check-interval", 15*time.Second, "Re-evaluate time-based rules")
	f.DurationVar(&c.WatchTimeout, "watch-timeout", 5*time.Minute, "Bound each watch connection; rebuild caches on renewal")
	f.IntVar(&c.MaxObjects, "max-objects", 5000, "Shared retained object limit across all resource types")
	f.IntVar(&c.MaxInputChars, "max-input-chars", 20000, "Maximum model evidence length, never truncated")
	f.IntVar(&c.MaxOutputTokens, "max-output-tokens", 1000, "Model output token limit")
	f.IntVar(&c.CallsPerHour, "model-calls-per-hour", 60, "Maximum model attempts in a rolling hour")
	f.DurationVar(&c.Policy.StartupGrace, "startup-grace", c.Policy.StartupGrace, "Normal pod startup/readiness grace")
	f.DurationVar(&c.Policy.RolloutGrace, "rollout-grace", c.Policy.RolloutGrace, "Deployment rollout grace")
	f.DurationVar(&c.Policy.EventLookback, "event-lookback", c.Policy.EventLookback, "Warning event lookback")
	f.IntVar(&restart, "restart-threshold", 3, "Cumulative restart information threshold")
	if err := f.Parse(args); err != nil {
		return c, err
	}
	if f.NArg() != 0 {
		return c, errors.New("unexpected positional arguments")
	}
	c.Namespaces = ns
	slices.Sort(c.Namespaces)
	if len(ns) == 0 {
		return c, errors.New("at least one --namespace is required")
	}
	switch c.Auth {
	case "kubeconfig":
		if strings.TrimSpace(c.Context) == "" {
			return c, errors.New("kubeconfig authentication requires --context")
		}
	case "in-cluster":
		if c.Context != "" || c.Kubeconfig != "" {
			return c, errors.New("in-cluster mode rejects --context and --kubeconfig")
		}
	default:
		return c, errors.New("--auth-mode must be kubeconfig or in-cluster")
	}
	if c.RequestTimeout <= 0 || c.RunTimeout < c.RequestTimeout || c.BatchWindow <= 0 || c.CheckInterval <= 0 || c.WatchTimeout < time.Second {
		return c, errors.New("durations must be positive; run timeout must cover request timeout and watch timeout must be at least 1s")
	}
	if c.Policy.StartupGrace < 0 || c.Policy.RolloutGrace < 0 || c.Policy.EventLookback <= 0 || restart <= 0 || restart > 1<<31-1 {
		return c, errors.New("invalid health rule policy")
	}
	c.Policy.RestartThreshold = int32(restart)
	if c.MaxObjects <= 0 || c.MaxInputChars <= 0 || c.MaxOutputTokens <= 0 || c.MaxOutputTokens > 1000000 || c.CallsPerHour <= 0 {
		return c, errors.New("object, input, output and call limits must be positive; max output tokens cannot exceed 1000000")
	}
	c.Endpoint, c.APIKey, c.Model = getenv("AZURE_OPENAI_ENDPOINT"), getenv("AZURE_OPENAI_API_KEY"), getenv("AZURE_AI_MODEL_DEPLOYMENT_NAME")
	if c.Model == "" {
		c.Model = getenv("FOUNDRY_MODEL_NAME")
	}
	if !c.Deterministic {
		u, err := url.Parse(c.Endpoint)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || (u.Port() != "" && u.Port() != "443") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/openai/v1/" {
			return c, errors.New("AZURE_OPENAI_ENDPOINT must be an HTTPS /openai/v1/ endpoint on port 443")
		}
		if strings.TrimSpace(c.APIKey) == "" || strings.TrimSpace(c.Model) == "" {
			return c, errors.New("set AZURE_OPENAI_API_KEY and AZURE_AI_MODEL_DEPLOYMENT_NAME, or use --deterministic-only")
		}
	}
	return c, nil
}

// LoadDotEnv never searches arbitrary parent .env files. A checkout is identified
// by this module, either at cwd or above the installed build output.
func LoadDotEnv(cwd, executable string) error {
	for _, start := range []string{cwd, filepath.Dir(executable)} {
		for dir := start; ; dir = filepath.Dir(dir) {
			data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return errors.New("cannot inspect repository module")
			}
			if strings.HasPrefix(string(data), "module k8s-ops-agent\n") {
				values, err := godotenv.Read(filepath.Join(dir, ".env"))
				if errors.Is(err, os.ErrNotExist) {
					return nil
				}
				if err != nil {
					return errors.New("cannot parse repository .env")
				}
				for key, value := range values {
					if _, exists := os.LookupEnv(key); !exists {
						if err := os.Setenv(key, value); err != nil {
							return fmt.Errorf("cannot load environment variable %q", key)
						}
					}
				}
				return nil
			}
			if filepath.Dir(dir) == dir {
				break
			}
		}
	}
	return nil
}
