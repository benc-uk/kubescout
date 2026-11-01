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

	"k8s-ops-agent/internal/analysis"
	"k8s-ops-agent/internal/config"
	"k8s-ops-agent/internal/incident"
	"k8s-ops-agent/internal/kube"
	"k8s-ops-agent/internal/report"
	"k8s-ops-agent/internal/runner"
	"k8s.io/client-go/dynamic"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runWithClient(ctx, args, stdout, stderr, kube.Client)
}

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
	sink := report.Writer{Out: stdout, Err: stderr, File: c.OutputFile}
	client, cluster, err := newClient(c)
	if err != nil {
		out := report.Result{SchemaVersion: "3.0", Type: "execution", Status: "incomplete", Changes: []incident.Change{}, Errors: []string{err.Error()}}
		if c.Once {
			if err := sink.WriteResult(out); err != nil {
				fmt.Fprintln(stderr, "ERROR:", err)
			}
		}
		fmt.Fprintln(stderr, "ERROR: Kubernetes client initialisation failed")
		return 1
	}
	var repository incident.Repository = &incident.Memory{Log: stderr}
	if c.Auth == "in-cluster" {
		repository = incident.Kubernetes{Client: client, Namespace: c.IncidentNamespace, Observer: c.ObserverID, Limit: c.MaxObjects, Timeout: c.RequestTimeout}
	}
	r := runner.Runner{Config: c, Source: kube.NewSource(client, c), Analyzer: &analysis.Analyzer{Config: c, Kubernetes: client}, Output: sink,
		Incidents: incident.Reconciler{Repository: repository, Observer: c.ObserverID, Model: c.Model, Simulated: c.Auth == "kubeconfig"}}
	fmt.Fprintf(stderr, "INFO: AI investigation for %s, %s (once=%t)\n", cluster, c.NamespaceScope(), c.Once)
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
