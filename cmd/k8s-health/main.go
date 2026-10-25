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
	"time"

	"k8s-ops-agent/internal/analysis"
	"k8s-ops-agent/internal/config"
	"k8s-ops-agent/internal/kube"
	"k8s-ops-agent/internal/report"
	"k8s-ops-agent/internal/runner"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
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
	sink := report.Writer{Out: stdout, File: c.ReportFile}
	client, cluster, err := kube.Client(c)
	if err != nil {
		out := report.New(cluster, c.Namespaces, time.Now())
		out.Execution, out.Errors = "incomplete", []string{err.Error()}
		out.Analysis = report.AnalysisState("skipped", "Kubernetes client unavailable")
		if err := sink.Write(out); err != nil {
			fmt.Fprintln(stderr, "ERROR:", err)
		}
		fmt.Fprintln(stderr, "ERROR: Kubernetes client initialisation failed")
		return 1
	}
	r := runner.Runner{Config: c, Cluster: cluster, Source: kube.NewSource(client, c), Analyzer: &analysis.Analyzer{Config: c}, Sink: sink}
	fmt.Fprintf(stderr, "INFO: observing %s in namespaces %v (once=%t, deterministic-only=%t)\n", cluster, c.Namespaces, c.Once, c.Deterministic)
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
