/*
Purpose: Bootstrap the KubeScout CLI and coordinate process shutdown and exit codes.
Flow: Load validated configuration, select Kubernetes access and wire collection,
AI investigation, CRD reconciliation, colour-configurable logs and optional rotating AI
transcripts into once or watch execution.
Startup diagnostics describe effective non-secret settings and component loading,
without claiming model connectivity before an investigation.
Proposal diagnostics default on in both modes, independently of AI transcripts;
normal capture failures do not replace the outcome of CRD reconciliation.
Supplemental kubectl describe shares the explicit Kubernetes authentication,
with independent guarded reads rather than replacing collection or AI tools.
Both authentication modes normally persist cluster-scoped Incidents.
Explicit read-only assessments reject Kubernetes writes and log diagnostic AI proposals,
without simulating Incident lifecycle or treating logs as incident storage.
Opt-in compiled extra-resource policies share collection and AI investigation boundaries.
*/
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/benc-uk/kubescout/internal/analysis"
	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/kube"
	"github.com/benc-uk/kubescout/internal/output"
	"github.com/benc-uk/kubescout/internal/runner"
	"k8s.io/client-go/dynamic"
)

// main connects termination signals to agent shutdown and exits with the CLI result.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

// run executes the agent CLI using the production Kubernetes client factory.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runWithClient(ctx, args, stdout, stderr, kube.Client)
}

// runWithClient validates configuration and wires collection, AI, reconciliation and output.
// It returns configuration, bootstrap or execution exit codes without storage fallback.
func runWithClient(ctx context.Context, args []string, stdout, stderr io.Writer, newClient func(config.Config) (dynamic.Interface, string, error)) int {
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(stderr, "ERROR [GO][BOOT] cannot determine working directory")
		return 2
	}

	if err := config.LoadDotEnv(cwd); err != nil {
		fmt.Fprintln(stderr, "ERROR [GO][CONFIG]", err)
		return 2
	}

	c, err := config.Parse(args, os.Getenv, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}

	if err != nil {
		fmt.Fprintln(stderr, "ERROR [GO][CONFIG]", err)
		return 2
	}

	sink := &output.Writer{Out: stdout, Err: stderr, Level: c.LogLevel, Color: c.LogColor == "always", Run: rand.Text()[:8], Secrets: []string{c.APIKey}}

	logStartup(c, sink)

	if sink.Error() != nil {
		fmt.Fprintln(stderr, "ERROR [GO][CONSOLE] console output failed")
		return 1
	}

	var proposals *output.ProposalLog

	if c.ProposalLog {
		proposals = output.NewProposalLog(c.ProposalLogDir, sink)
		if c.ReadOnly {
			if err := proposals.Write("", "Read-only AI assessment started; proposals are diagnostic only, not persisted Incidents. No Incident API reads or writes; no lifecycle state is restored."); err != nil {
				_ = sink.Log("error", "GO", "PROPOSAL", err.Error())
				return 1
			}
		} else {
			proposals.Capture("", "AI proposal capture started for CRD-backed execution; proposals do not confirm persistence. Kubernetes Incidents remain the authoritative lifecycle record.")
		}

		_ = sink.Log("info", "GO", "PROPOSAL", "AI proposal capture enabled: "+proposals.Path()+" (10 x 10 MiB; not incident storage)")
	} else {
		_ = sink.Log("info", "GO", "PROPOSAL", "AI proposal capture explicitly disabled")
	}

	var transcript *output.Transcript

	if c.AILog {
		transcript = output.NewTranscript(c.AILogDir, sink)
		_ = sink.Log("info", "GO", "TRANSCRIPT", "AI capture enabled: "+transcript.Path()+" (10 x 10 MiB, ephemeral unless mounted persistently)")

		transcript.Record("", "GO", "RUN.START", "New process; any preceding unfinished investigation may have incomplete capture.")
		defer transcript.Record("", "GO", "RUN.END", "Process execution ended.")
	} else {
		_ = sink.Log("info", "GO", "TRANSCRIPT", "AI file capture explicitly disabled")
	}

	if sink.Error() != nil {
		fmt.Fprintln(stderr, "ERROR [GO][CONSOLE] console output failed")
		return 1
	}

	_ = sink.Log("info", "GO", "BOOT", "Loading Kubernetes credentials for the selected authentication mode")

	client, cluster, err := newClient(c)
	if err != nil {
		_ = sink.Log("error", "GO", "BOOT", "Kubernetes client initialisation failed; execution incomplete")

		return 1
	}

	_ = sink.Log("info", "GO", "BOOT", fmt.Sprintf("Kubernetes client ready: auth=%s cluster/context=%q (API access checked during collection)", c.Auth, cluster))

	source := kube.NewSource(client, c)
	source.Log = sink
	source.LogPlan()

	analyzer := &analysis.Analyzer{Config: c, Kubernetes: client, Log: sink, Transcript: transcript}
	if connection, ok := client.(*kube.Connection); ok {
		analyzer.DescribeConfig = connection.DescribeConfig
	}

	r := runner.Runner{Config: c, Source: source, Analyzer: analyzer, Output: sink,
		Transcript: transcript, Proposals: proposals}
	if c.ReadOnly {
		_ = sink.Log("info", "GO", "BOOT", "Agent components ready: read-only collection, private evidence checks, serial AI assessment and proposal diagnostics")
	} else {
		r.Incidents = newIncidentReconciler(c, client)
		_ = sink.Log("info", "GO", "BOOT", "Agent components ready: collection, private evidence checks, serial AI investigation and Incident reconciliation")
	}

	code := 0
	if c.Once {
		code = r.Once(ctx)
	} else {
		code = r.Watch(ctx)
	}

	if sink.Error() != nil {
		fmt.Fprintln(stderr, "ERROR [GO][CONSOLE] console output failed")
		return 1
	}

	if code != 0 {
		_ = sink.Log("error", "GO", "SHUTDOWN", "Execution incomplete")
	}

	return code
}

// logStartup describes validated operational settings without dumping configuration or credentials.
// Model configuration is reported separately from connectivity and subsequent AI decisions.
func logStartup(c config.Config, sink *output.Writer) {
	mode, deadline := "watch", "initial cache sync deadline"
	if c.Once {
		mode, deadline = "once", "assessment deadline"
	}

	_ = sink.Log("info", "GO", "BOOT", fmt.Sprintf("KubeScout starting: mode=%s observer=%s", mode, c.ObserverID))
	_ = sink.Log("info", "GO", "CONFIG", "Configuration validated: CLI flags and environment; working-directory .env loading preserves existing environment values")

	context, kubeconfig := c.Context, c.Kubeconfig
	if context == "" {
		context = "current context"
	}

	if kubeconfig == "" {
		kubeconfig = "standard kubeconfig loading"
	}

	if c.Auth == "kubeconfig" {
		_ = sink.Log("info", "GO", "CONFIG", fmt.Sprintf("Kubernetes access: auth=%s context=%q kubeconfig=%q; no authentication fallback", c.Auth, context, kubeconfig))
	} else {
		_ = sink.Log("info", "GO", "CONFIG", "Kubernetes access: auth=in-cluster using ServiceAccount credentials; no authentication fallback")
	}

	_ = sink.Log("info", "GO", "CONFIG", "Workload scope: "+c.NamespaceScope()+"; Nodes, PersistentVolumes and VolumeAttachments remain cluster-scoped")
	if len(c.NamespaceWatch) == 0 {
		_ = sink.Log("info", "GO", "CONFIG", "Workload reads use all-namespace list/watch requests; ignored namespaces are filtered before cache admission and model input, not by RBAC")
	} else {
		_ = sink.Log("info", "GO", "CONFIG", "Workload reads use namespace-scoped list/watch requests")
	}

	_ = sink.Log("info", "GO", "CONFIG", fmt.Sprintf("Console: level=%s color=%s; normal activity to stdout, warnings/errors to stderr", c.LogLevel, c.LogColor))

	_ = sink.Log("info", "GO", "CONFIG", fmt.Sprintf("Collection limits: max objects=%d request timeout=%s %s=%s", c.MaxObjects, c.RequestTimeout, deadline, c.RunTimeout))
	if !c.Once {
		check := "check/index refresh"
		if c.ReadOnly {
			check = "check"
		}

		_ = sink.Log("info", "GO", "CONFIG", fmt.Sprintf("Watch scheduling: batch=%s %s=%s watch connection timeout=%s", c.BatchWindow, check, c.CheckInterval, c.WatchTimeout))
		if c.ReadOnly {
			_ = sink.Log("info", "GO", "CONFIG", fmt.Sprintf("Read-only review: cooldown=%s proposal/no-action review=%s; no stored Incident recovery or lifecycle", c.Cooldown, c.ReviewInterval))
		} else {
			_ = sink.Log("info", "GO", "CONFIG", fmt.Sprintf("Incident review: cooldown=%s unresolved/no-action review=%s continuous fresh recovery window=%s", c.Cooldown, c.ReviewInterval, c.RecoveryWindow))
		}
	}

	checks := make([]string, 0, len(health.Checks))
	for _, check := range health.Checks {
		checks = append(checks, check.Name)
	}

	_ = sink.Log("info", "GO", "CHECK", "Private evidence checks loaded: "+strings.Join(checks, ", ")+"; signals schedule AI review, never publish incidents")

	_ = sink.Log("info", "GO", "CHECK", fmt.Sprintf("Evidence policy: startup grace=%s rollout grace=%s Warning event lookback=%s restart threshold=%d", c.Policy.StartupGrace, c.Policy.RolloutGrace, c.Policy.EventLookback, c.Policy.RestartThreshold))
	if len(c.Policy.ExtraResources) == 0 {
		_ = sink.Log("info", "GO", "CONFIG", "Extra-resource policies: disabled; built-in inventory only")
	} else {
		_ = sink.Log("info", "GO", "CONFIG", fmt.Sprintf("%d configured extra-resource policies loaded from %q; selected status facts and local CEL hints only", len(c.Policy.ExtraResources), c.ExtraResourcesFile))
		for _, resource := range c.Policy.ExtraResources {
			_ = sink.Log("info", "GO", "CONFIG", fmt.Sprintf("Extra-resource policy %s: %s %s/%s/%s scope=%s fields=%d rules=%d", resource.ID, resource.Kind, resource.Group, resource.Version, resource.Resource, resource.Scope, len(resource.Fields), len(resource.Rules)))
		}
	}

	_ = sink.Log("info", "GO", "MODEL", fmt.Sprintf("Responses API configured: endpoint=%s deployment=%q; connection not yet tested", c.Endpoint, c.Model))
	_ = sink.Log("info", "GO", "MODEL", fmt.Sprintf("Embedded AI instructions ready: prompt=%s; typed resource inspection and Warning event tools; provider storage and HTTP retries disabled", incident.PromptVersion))

	_ = sink.Log("info", "GO", "MODEL", fmt.Sprintf("AI limits: one investigation at a time, deadline=%s, HTTP attempts/rolling hour=%d, tools/investigation=%d, initial input=%d bytes, output=%d tokens/request", c.InvestigationTimeout, c.CallsPerHour, c.MaxToolCalls, c.MaxInputChars, c.MaxOutputTokens))
	if c.ReadOnly {
		_ = sink.Log("info", "GO", "BOOT", "Read-only assessment: Kubernetes GET/list/watch only; fresh validated AI proposals go to the diagnostic log, not Incident records")
	} else {
		_ = sink.Log("info", "GO", "BOOT", "Only validated AI decisions persist to cluster-scoped Incident records; AI no-action is valid, collection/model failures create no incidents")
	}
}

// newIncidentReconciler wires cluster-scoped Kubernetes persistence for either authentication mode.
func newIncidentReconciler(c config.Config, client dynamic.Interface) incident.Reconciler {
	return incident.Reconciler{
		Repository: incident.Kubernetes{Client: client, Observer: c.ObserverID, Limit: c.MaxObjects, Timeout: c.RequestTimeout},
		Observer:   c.ObserverID,
		Model:      c.Model,
	}
}
