/*
Purpose: Define and validate runtime options, namespace selection and model settings.
Flow: Combine CLI flags and environment values into collection, evidence-policy,
logging/console colour and AI limits; repository-root dotenv loading preserves existing values.
Configuration requires AI settings; cluster-scoped Incidents have no storage namespace option.
One recovery window covers cleared signals and confirmed absence; watch lifetimes must allow review.
*/
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

	"github.com/benc-uk/kubescout/internal/health"
	"github.com/joho/godotenv"
	"k8s.io/apimachinery/pkg/util/validation"
)

type Config struct {
	Once, AILog                                                          bool
	Auth, Context, Kubeconfig, ObserverID                                string
	LogLevel, LogColor, AILogDir                                         string
	NamespaceWatch, NamespaceIgnore                                      []string
	RequestTimeout, RunTimeout, BatchWindow, CheckInterval, WatchTimeout time.Duration
	MaxObjects, MaxInputChars, MaxOutputTokens, CallsPerHour             int
	MaxToolCalls                                                         int
	Endpoint, APIKey, Model                                              string
	Policy                                                               health.Policy
	InvestigationTimeout, Cooldown, RecoveryWindow, ReviewInterval       time.Duration
}

type namespaces []string

// DefaultMaxToolCalls is the default shared per-investigation invocation budget.
const DefaultMaxToolCalls = 64

// String renders the namespace list as a comma-separated flag value.
func (n *namespaces) String() string { return strings.Join(*n, ",") }

// Set validates a comma-separated list before appending trimmed, unique namespace names.
func (n *namespaces) Set(s string) error {
	values := strings.Split(s, ",")
	for i, value := range values {
		values[i] = strings.TrimSpace(value)
		if len(validation.IsDNS1123Label(values[i])) != 0 {
			return errors.New("namespace lists must contain comma-separated valid namespace names")
		}
	}

	for _, value := range values {
		if !slices.Contains(*n, value) {
			*n = append(*n, value)
		}
	}

	return nil
}

// WatchesNamespace checks namespace admission against the watch and ignore lists, rejecting empty names.
func (c Config) WatchesNamespace(name string) bool {
	return name != "" && (len(c.NamespaceWatch) == 0 || slices.Contains(c.NamespaceWatch, name)) && !slices.Contains(c.NamespaceIgnore, name)
}

// NamespaceScope describes the configured namespace selection for operator diagnostics.
func (c Config) NamespaceScope() string {
	if len(c.NamespaceWatch) != 0 {
		return "watching " + strings.Join(c.NamespaceWatch, ",")
	}

	if len(c.NamespaceIgnore) != 0 {
		return "watching all except " + strings.Join(c.NamespaceIgnore, ",")
	}

	return "watching all namespaces"
}

// Parse combines flags and environment settings, then validates scope, authentication and AI limits.
// Explicit namespace flags replace their corresponding environment lists; conflicting lists fail.
func Parse(args []string, getenv func(string) string, stderr io.Writer) (Config, error) {
	c := Config{Policy: health.DefaultPolicy()}

	for _, arg := range args {
		name, _, _ := strings.Cut(arg, "=")
		switch name {
		case "--deterministic-only":
			return c, errors.New("--deterministic-only was removed; the runtime requires AI configuration, and offline tests use fixtures")
		case "--report-namespace", "--incident-namespace":
			return c, errors.New(name + " was removed; Incident records are cluster-scoped")
		case "--report-file", "--output-file":
			return c, errors.New(name + " was removed; incidents are stored in Kubernetes CRDs, not console/file exports; --ai-log-dir selects diagnostic AI transcripts")
		case "--simulate-incidents":
			return c, errors.New("--simulate-incidents was removed; accepted AI decisions persist Incidents in both authentication modes")
		}
	}

	f := flag.NewFlagSet("kubescout", flag.ContinueOnError)
	f.SetOutput(stderr)

	var (
		watch, ignore namespaces
		restart       int
	)

	f.BoolVar(&c.Once, "once", false, "Run one bounded AI assessment instead of watching")
	f.StringVar(&c.Auth, "auth-mode", "", "Required: kubeconfig or in-cluster")
	f.StringVar(&c.Context, "context", "", "Kubeconfig context override (defaults to the current context)")
	f.StringVar(&c.Kubeconfig, "kubeconfig", "", "Optional kubeconfig path")
	f.Var(&watch, "namespace-watch", "Comma-separated namespaces to watch (NAMESPACE_WATCH); default is all")
	f.Var(&ignore, "namespace-ignore", "Comma-separated namespaces to ignore (NAMESPACE_IGNORE); mutually exclusive with namespace-watch")
	f.StringVar(&c.LogLevel, "log-level", "info", "Console verbosity: debug, info, warn or error")
	f.StringVar(&c.LogColor, "log-color", "always", "ANSI console colour: always or never (including pod logs and redirected output)")
	f.BoolVar(&c.AILog, "ai-log", true, "Capture rotating AI transcripts; use --ai-log=false to disable")
	f.StringVar(&c.AILogDir, "ai-log-dir", "logs", "Private AI transcript directory (10 files of at most 10 MiB)")
	f.StringVar(&c.ObserverID, "observer-id", "", "Required stable unique identity for this cluster and observation scope")
	f.DurationVar(&c.InvestigationTimeout, "investigation-timeout", 120*time.Second, "Whole AI investigation deadline")
	f.DurationVar(&c.Cooldown, "incident-cooldown", 2*time.Minute, "Coalesce ordinary changes in the same incident")
	f.DurationVar(&c.RecoveryWindow, "recovery-window", 5*time.Minute, "Continuous fresh cleared signals or resource absence before AI resolution review")
	f.DurationVar(&c.ReviewInterval, "review-interval", 15*time.Minute, "Revisit unresolved or no-action cases")
	f.DurationVar(&c.RequestTimeout, "request-timeout", 10*time.Second, "API/model request timeout")
	f.DurationVar(&c.RunTimeout, "run-timeout", 120*time.Second, "Once deadline or initial informer synchronisation deadline")
	f.DurationVar(&c.BatchWindow, "batch-window", 5*time.Second, "Maximum wait to batch resource changes")
	f.DurationVar(&c.CheckInterval, "check-interval", 15*time.Second, "Re-evaluate time-based rules")
	f.DurationVar(&c.WatchTimeout, "watch-timeout", 10*time.Minute, "Bound each watch connection; must exceed recovery window plus investigation deadline")
	f.IntVar(&c.MaxObjects, "max-objects", 5000, "Shared retained object limit across all resource types")
	f.IntVar(&c.MaxInputChars, "max-input-chars", 20000, "Maximum model evidence length, never truncated")
	f.IntVar(&c.MaxOutputTokens, "max-output-tokens", 2000, "Per-request model output token limit")
	f.IntVar(&c.CallsPerHour, "model-calls-per-hour", 60, "Maximum model attempts in a rolling hour")
	f.IntVar(&c.MaxToolCalls, "max-tool-calls", DefaultMaxToolCalls, "Maximum total tool calls per investigation across both tools (must be positive)")
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

	if !slices.Contains([]string{"debug", "info", "warn", "error"}, c.LogLevel) {
		return c, errors.New("--log-level must be debug, info, warn or error")
	}

	if !slices.Contains([]string{"always", "never"}, c.LogColor) {
		return c, errors.New("--log-color must be always or never")
	}

	if c.AILog && strings.TrimSpace(c.AILogDir) == "" {
		return c, errors.New("--ai-log-dir must not be empty when AI logging is enabled")
	}

	watchSet, ignoreSet := false, false

	f.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "namespace-watch":
			watchSet = true
		case "namespace-ignore":
			ignoreSet = true
		}
	})

	for _, setting := range []struct {
		name string
		set  bool
		list *namespaces
	}{{"NAMESPACE_WATCH", watchSet, &watch}, {"NAMESPACE_IGNORE", ignoreSet, &ignore}} {
		if value := getenv(setting.name); !setting.set && value != "" {
			if err := setting.list.Set(value); err != nil {
				return c, fmt.Errorf("%s: %w", setting.name, err)
			}
		}
	}

	if len(watch) != 0 && len(ignore) != 0 {
		return c, errors.New("namespace-watch and namespace-ignore are mutually exclusive (including environment variables)")
	}

	c.NamespaceWatch, c.NamespaceIgnore = watch, ignore
	slices.Sort(c.NamespaceWatch)
	slices.Sort(c.NamespaceIgnore)

	if c.ObserverID == "" || len(validation.IsDNS1123Label(c.ObserverID)) != 0 {
		return c, errors.New("--observer-id must be a stable explicit DNS label unique to this cluster and scope")
	}

	if c.InvestigationTimeout < c.RequestTimeout || c.Cooldown <= 0 || c.RecoveryWindow <= 0 || c.ReviewInterval <= 0 {
		return c, errors.New("investigation and scheduling durations must be positive; investigation timeout must cover request timeout")
	}

	switch c.Auth {
	case "kubeconfig":
		if c.Context != "" && strings.TrimSpace(c.Context) == "" {
			return c, errors.New("--context must be a valid kubeconfig context name")
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

	if c.RecoveryWindow <= 0 || c.InvestigationTimeout <= 0 {
		return c, errors.New("recovery and investigation durations must be positive")
	}

	if !c.Once && (c.RecoveryWindow >= c.WatchTimeout || c.InvestigationTimeout >= c.WatchTimeout-c.RecoveryWindow) {
		return c, errors.New("--watch-timeout must exceed --recovery-window plus --investigation-timeout so cache renewal permits recovery review")
	}

	if c.Policy.StartupGrace < 0 || c.Policy.RolloutGrace < 0 || c.Policy.EventLookback <= 0 || restart <= 0 || restart > 1<<31-1 {
		return c, errors.New("invalid health rule policy")
	}

	c.Policy.RestartThreshold = int32(restart)
	if c.MaxToolCalls <= 0 {
		return c, errors.New("--max-tool-calls must be positive")
	}

	if c.MaxObjects <= 0 || c.MaxInputChars <= 0 || c.MaxOutputTokens <= 0 || c.MaxOutputTokens > 1000000 || c.CallsPerHour <= 0 {
		return c, errors.New("object, input, output and call limits must be positive; max output tokens cannot exceed 1000000")
	}

	if err := c.LoadModel(getenv); err != nil {
		return c, err
	}

	return c, nil
}

// LoadModel populates required model settings.
// It validates the HTTPS endpoint and nonempty credentials without making a model request.
func (c *Config) LoadModel(getenv func(string) string) error {
	c.Endpoint, c.APIKey, c.Model = getenv("AZURE_OPENAI_ENDPOINT"), getenv("AZURE_OPENAI_API_KEY"), getenv("AZURE_AI_MODEL_DEPLOYMENT_NAME")

	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || (u.Port() != "" && u.Port() != "443") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/openai/v1/" {
		return errors.New("AZURE_OPENAI_ENDPOINT must be an HTTPS /openai/v1/ endpoint on port 443")
	}

	if strings.TrimSpace(c.APIKey) == "" || strings.TrimSpace(c.Model) == "" {
		return errors.New("AI configuration is required: set AZURE_OPENAI_API_KEY and AZURE_AI_MODEL_DEPLOYMENT_NAME")
	}

	return nil
}

// LoadDotEnv loads only this module's repository-root .env, preserving existing environment values.
// It searches upward from cwd and the executable directory, never loading arbitrary parent .env files.
func LoadDotEnv(cwd, executable string) error {
	for _, start := range []string{cwd, filepath.Dir(executable)} {
		for dir := start; ; dir = filepath.Dir(dir) {
			data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return errors.New("cannot inspect repository module")
			}

			if strings.HasPrefix(string(data), "module github.com/benc-uk/kubescout\n") {
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
