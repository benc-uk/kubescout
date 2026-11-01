/*
Purpose: Bootstrap the KubeScout CLI and coordinate process shutdown and exit codes.
Flow: Load validated configuration, select Kubernetes access and wire collection,
AI investigation, CRD reconciliation, colour-configurable logs and optional rotating AI
transcripts into once or watch execution.
Both authentication modes persist cluster-scoped Incidents; there is no runtime simulation path.
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
	"syscall"

	"github.com/benc-uk/kubescout/internal/analysis"
	"github.com/benc-uk/kubescout/internal/config"
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

	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintln(stderr, "ERROR [GO][BOOT] cannot determine executable location")
		return 2
	}

	if err := config.LoadDotEnv(cwd, exe); err != nil {
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

	var transcript *output.Transcript

	if c.AILog {
		transcript = output.NewTranscript(c.AILogDir, sink)
		_ = sink.Log("info", "GO", "TRANSCRIPT", "AI capture enabled: "+transcript.Path()+" (10 x 10 MiB, ephemeral unless mounted persistently)")

		transcript.Record("", "GO", "RUN.START", "New process; any preceding unfinished investigation may have incomplete capture.")
		defer transcript.Record("", "GO", "RUN.END", "Process execution ended.")
	} else {
		_ = sink.Log("info", "GO", "TRANSCRIPT", "AI file capture explicitly disabled")
	}

	client, cluster, err := newClient(c)
	if err != nil {
		_ = sink.Log("error", "GO", "BOOT", "Kubernetes client initialisation failed; execution incomplete")

		return 1
	}

	source := kube.NewSource(client, c)
	source.Log = sink
	r := runner.Runner{Config: c, Source: source, Analyzer: &analysis.Analyzer{Config: c, Kubernetes: client, Log: sink, Transcript: transcript}, Output: sink,
		Transcript: transcript, Incidents: newIncidentReconciler(c, client)}
	_ = sink.Log("info", "GO", "BOOT", fmt.Sprintf("AI investigation for %s, %s (once=%t)", cluster, c.NamespaceScope(), c.Once))

	_ = sink.Log("info", "GO", "BOOT", "Accepted AI decisions persist to cluster-scoped Incident records")

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

// newIncidentReconciler wires cluster-scoped Kubernetes persistence for either authentication mode.
func newIncidentReconciler(c config.Config, client dynamic.Interface) incident.Reconciler {
	return incident.Reconciler{
		Repository: incident.Kubernetes{Client: client, Observer: c.ObserverID, Limit: c.MaxObjects, Timeout: c.RequestTimeout},
		Observer:   c.ObserverID,
		Model:      c.Model,
	}
}
