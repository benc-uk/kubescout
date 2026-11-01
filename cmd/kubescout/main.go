/*
Purpose: Bootstrap the KubeScout CLI and coordinate process shutdown and exit codes.
Flow: Load validated configuration, select Kubernetes access and wire collection,
AI investigation, incident reconciliation and output into once or watch execution.
Both authentication modes persist Incidents; there is no runtime simulation path.
*/
package main

import (
	"context"
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
		fmt.Fprintln(stderr, "ERROR: cannot determine working directory")
		return 2
	}

	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintln(stderr, "ERROR: cannot determine executable location")
		return 2
	}

	if err := config.LoadDotEnv(cwd, exe); err != nil {
		fmt.Fprintln(stderr, "ERROR:", err)
		return 2
	}

	c, err := config.Parse(args, os.Getenv, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}

	if err != nil {
		fmt.Fprintln(stderr, "ERROR:", err)
		return 2
	}

	sink := output.Writer{Out: stdout, Err: stderr, File: c.OutputFile}

	client, cluster, err := newClient(c)
	if err != nil {
		out := output.Result{SchemaVersion: "3.0", Type: "execution", Status: "incomplete", Changes: []incident.Change{}, Errors: []string{err.Error()}}
		if c.Once {
			if err := sink.WriteResult(out); err != nil {
				fmt.Fprintln(stderr, "ERROR:", err)
			}
		}

		fmt.Fprintln(stderr, "ERROR: Kubernetes client initialisation failed")

		return 1
	}

	r := runner.Runner{Config: c, Source: kube.NewSource(client, c), Analyzer: &analysis.Analyzer{Config: c, Kubernetes: client}, Output: sink,
		Incidents: newIncidentReconciler(c, client)}
	fmt.Fprintf(stderr, "INFO: AI investigation for %s, %s (once=%t)\n", cluster, c.NamespaceScope(), c.Once)

	fmt.Fprintf(stderr, "INFO: persisting Incidents in namespace %s\n", c.IncidentNamespace)

	code := 0
	if c.Once {
		code = r.Once(ctx)
	} else {
		code = r.Watch(ctx)
	}

	if code != 0 {
		fmt.Fprintln(stderr, "ERROR: execution or report output incomplete")
	}

	return code
}

// newIncidentReconciler wires namespaced Kubernetes persistence for either authentication mode.
func newIncidentReconciler(c config.Config, client dynamic.Interface) incident.Reconciler {
	return incident.Reconciler{
		Repository: incident.Kubernetes{Client: client, Namespace: c.IncidentNamespace, Observer: c.ObserverID, Limit: c.MaxObjects, Timeout: c.RequestTimeout},
		Observer:   c.ObserverID,
		Model:      c.Model,
	}
}
