/*
Purpose: Bootstrap the standalone dashboard with explicit Kubernetes authentication.
Flow: Validate flags, select the bind address and serve namespaced ClusterIssue
views and human triage until shutdown, reporting startup failures through exit codes.
The dashboard runs separately from AI investigation and incident-spec publication.
*/
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/benc-uk/kubescout/internal/dashboard"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
)

// main connects termination signals to dashboard shutdown and exits with the server result.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	os.Exit(run(ctx, os.Args[1:], os.Stderr, dashboard.Client, net.Listen))
}

// run validates dashboard options, authenticates explicitly and serves until cancellation.
// Kubeconfig mode binds only loopback; startup and server failures produce nonzero exit codes.
func run(ctx context.Context, args []string, stderr io.Writer, client func(dashboard.ClientOptions) (dynamic.Interface, error), listen func(string, string) (net.Listener, error)) int {
	flags := flag.NewFlagSet("kubescout-dashboard", flag.ContinueOnError)
	flags.SetOutput(stderr)
	namespace := flags.String("namespace", "k8s-health", "Namespace containing ClusterIssues")

	var options dashboard.ClientOptions
	flags.StringVar(&options.AuthMode, "auth-mode", "in-cluster", "Kubernetes authentication: in-cluster (:8000) or kubeconfig (127.0.0.1:8000)")
	flags.StringVar(&options.Context, "context", "", "Kubeconfig context override (defaults to the current context)")
	flags.StringVar(&options.Kubeconfig, "kubeconfig", "", "Kubeconfig file (otherwise KUBECONFIG or the standard default)")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}

		return 2
	}

	if flags.NArg() != 0 || len(validation.IsDNS1123Label(*namespace)) != 0 {
		fmt.Fprintln(stderr, "ERROR: expected one valid --namespace and no positional arguments")
		return 2
	}

	if err := options.Validate(); err != nil {
		fmt.Fprintln(stderr, "ERROR:", err)
		return 2
	}

	kube, err := client(options)
	if err != nil {
		fmt.Fprintf(stderr, "ERROR: dashboard Kubernetes authentication failed (%s); check the selected credentials and context\n", options.AuthMode)
		return 1
	}

	reader, err := dashboard.NewReader(kube, *namespace)
	if err != nil {
		fmt.Fprintln(stderr, "ERROR:", err)
		return 2
	}

	address := ":8000"
	if options.AuthMode == "kubeconfig" {
		address = "127.0.0.1:8000"
	}

	listener, err := listen("tcp", address)
	if err != nil {
		fmt.Fprintln(stderr, "ERROR: cannot listen on port 8000")
		return 1
	}

	logger := log.New(stderr, "", 0)
	logger.Printf("INFO: dashboard with human triage listening on %s for namespace %s", address, *namespace)

	if err := dashboard.Serve(ctx, listener, dashboard.Handler(reader, logger), stderr); err != nil {
		logger.Print("ERROR: dashboard HTTP server failed")
		return 1
	}

	return 0
}
