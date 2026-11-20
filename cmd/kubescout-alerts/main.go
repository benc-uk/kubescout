/*
Purpose: Provide a standalone alert-test watcher with startup replay and dry-run output.
Flow: Authenticate with the current kubeconfig context and project Added events
and optional stored open records into alerts using the identity anchor as source.
Startup replay skips resolved records and reports invalid lifecycle states.
Normal mode sends bounded HTTP POSTs; --dry-run writes indented JSON to stdout
without an endpoint. Diagnostics use stderr; stdout failures stop the watcher.
No AI, Kubernetes writes or durable delivery.
*/
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/incident"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	watchtools "k8s.io/client-go/tools/watch"
)

type alert struct {
	Title           string `json:"title"`
	Severity        string `json:"severity"`
	Impact          string `json:"impact"`
	Source          string `json:"source"`
	Diagnosis       string `json:"diagnosis"`
	Explanation     string `json:"explanation"`
	Recommendations string `json:"recommendations"`
}

type alertOptions struct {
	includeExisting bool
	dryRun          bool
}

// main runs the separate watcher until interrupted, reporting failures without endpoint credentials.
func main() {
	logger := log.New(os.Stderr, "", log.LstdFlags)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, logger); err != nil {
		logger.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

// run loads local configuration and starts an Incident-only watch; dry-run needs no alert endpoint.
func run(ctx context.Context, args []string, stdout io.Writer, logger *log.Logger) error {
	options, err := parseArgs(args, logger.Writer())
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}

		return err
	}

	cwd, err := os.Getwd()
	if err != nil {
		return errors.New("cannot determine working directory")
	}

	if err := config.LoadDotEnv(cwd); err != nil {
		return err
	}

	endpoint := os.Getenv("ALERT_ENDPOINT_URL")
	if !options.dryRun {
		if err := validateEndpoint(endpoint); err != nil {
			return err
		}
	}

	resource, err := incidentClient()
	if err != nil {
		return err
	}

	return watchNew(ctx, resource, endpoint, options, stdout, logger)
}

// parseArgs validates startup replay and dry-run flags, rejecting positional arguments.
func parseArgs(args []string, output io.Writer) (alertOptions, error) {
	flags := flag.NewFlagSet("kubescout-alerts", flag.ContinueOnError)
	flags.SetOutput(output)

	var options alertOptions
	flags.BoolVar(&options.includeExisting, "include-existing", false, "Process existing open Incidents at startup before watching new ones (skips resolved; repeats on every restart)")
	flags.BoolVar(&options.dryRun, "dry-run", false, "Print indented alert JSON to stdout instead of sending HTTP alerts (no endpoint required)")

	if err := flags.Parse(args); err != nil {
		return alertOptions{}, err
	}

	if flags.NArg() != 0 {
		return alertOptions{}, errors.New("unexpected positional arguments; usage: go run ./cmd/kubescout-alerts [--include-existing] [--dry-run]")
	}

	return options, nil
}

// validateEndpoint accepts an absolute HTTP(S) destination without disclosing invalid URL contents.
func validateEndpoint(endpoint string) error {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Hostname() == "" || parsed.Fragment != "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.New("set ALERT_ENDPOINT_URL to an absolute HTTP(S) URL in .env or the environment")
	}

	return nil
}

// incidentClient uses only the selected kubeconfig context and returns a cluster-scoped Incident client.
func incidentClient() (dynamic.ResourceInterface, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()

	loaded, err := rules.Load()
	if err != nil || loaded.CurrentContext == "" {
		return nil, errors.New("cannot load kubeconfig with a current context")
	}

	settings, err := clientcmd.NewNonInteractiveClientConfig(*loaded, loaded.CurrentContext,
		&clientcmd.ConfigOverrides{}, rules).ClientConfig()
	if err != nil {
		return nil, errors.New("cannot load credentials for the current kubeconfig context")
	}

	settings.UserAgent = "kubescout-alerts/go"

	client, err := dynamic.NewForConfig(settings)
	if err != nil {
		return nil, errors.New("cannot initialize Kubernetes client")
	}

	return client.Resource(schema.GroupVersionResource{
		Group: "kubescout.benc-uk.github.com", Version: "v1alpha1", Resource: "incidents",
	}), nil
}

// listIncidents reads one bounded startup page without exposing Kubernetes error response bodies.
func listIncidents(ctx context.Context, resource dynamic.ResourceInterface, options metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	snapshot, err := resource.List(listCtx, options)
	if err != nil {
		return nil, errors.New("cannot list Incidents; check current context, connectivity and list/watch permissions")
	}

	return snapshot, nil
}

// watchNew optionally processes open startup records, then handles only subsequent Added events.
// Dry-run writes JSON without POSTs; stdout failure or expired watch history stops processing.
func watchNew(ctx context.Context, resource dynamic.ResourceInterface, endpoint string, options alertOptions, stdout io.Writer, logger *log.Logger) error {
	listOptions := metav1.ListOptions{Limit: 1}
	if options.includeExisting {
		listOptions.Limit = 100
	}

	snapshot, err := listIncidents(ctx, resource, listOptions)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}

		return err
	}

	stream, err := watchtools.NewRetryWatcherWithContext(ctx, snapshot.GetResourceVersion(), &cache.ListWatch{
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			timeout := int64(300)
			options.TimeoutSeconds = &timeout

			return resource.Watch(ctx, options)
		},
	})
	if err != nil {
		return errors.New("cannot start Incident watch from the initial list")
	}
	defer func() {
		stream.Stop()
		<-stream.Done()
	}()

	client := &http.Client{
		Timeout:       30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")

	processAlert := func(object *unstructured.Unstructured) error {
		payload, err := alertFor(object)
		if err != nil {
			logger.Printf("ERROR: Incident %s: %s", object.GetName(), err)
			return nil
		}

		if options.dryRun {
			if err := encoder.Encode(payload); err != nil {
				return fmt.Errorf("cannot write dry-run alert for Incident %s to stdout", object.GetName())
			}

			return nil
		}

		if err := postWithRetry(ctx, client, endpoint, payload, object.GetName(), logger); err != nil {
			if ctx.Err() != nil {
				return nil
			}

			logger.Printf("ERROR: Incident %s: %s; giving up after 3 retries", object.GetName(), err)

			return nil
		}

		logger.Printf("INFO: Incident %s sent", object.GetName())

		return nil
	}

	if options.dryRun {
		logger.Print("INFO: dry-run mode; printing alert JSON to stdout without HTTP alerts")
	}

	if options.includeExisting {
		logger.Print("INFO: processing existing open Incidents; resolved records are skipped; startup replay repeats on every restart")

		for {
			for i := range snapshot.Items {
				if ctx.Err() != nil {
					return nil
				}

				state, found, err := unstructured.NestedString(snapshot.Items[i].Object, "spec", "lifecycle", "state")
				if err != nil || !found || (state != "open" && state != "resolved") {
					logger.Printf("ERROR: Incident %s: missing or invalid alert field: spec.lifecycle.state", snapshot.Items[i].GetName())
					continue
				}

				if state == "resolved" {
					continue
				}

				if err := processAlert(&snapshot.Items[i]); err != nil {
					return err
				}
			}

			listOptions.Continue = snapshot.GetContinue()
			if listOptions.Continue == "" {
				break
			}

			snapshot, err = listIncidents(ctx, resource, listOptions)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}

				return err
			}
		}

		logger.Print("INFO: watching newly created Incidents; updates are ignored")
	} else {
		logger.Print("INFO: watching newly created Incidents; existing records and updates are ignored")
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, open := <-stream.ResultChan():
			if ctx.Err() != nil {
				return nil
			}

			if !open || event.Type == watch.Error {
				return errors.New("Incident watch failed or history expired; restart to establish a new baseline")
			}

			if event.Type != watch.Added {
				continue
			}

			object, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				return errors.New("Incident watch returned an unexpected object")
			}

			if err := processAlert(object); err != nil {
				return err
			}
		}
	}
}

// alertFor projects analysis and the identity anchor name, reporting invalid fields without their values.
func alertFor(object *unstructured.Unstructured) (alert, error) {
	raw, found, err := unstructured.NestedMap(object.Object, "spec", "analysis")
	if err != nil || !found {
		return alert{}, errors.New("missing or invalid alert field: spec.analysis")
	}

	var analysis incident.Analysis
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &analysis); err != nil {
		return alert{}, errors.New("invalid alert field types: spec.analysis")
	}

	if len(analysis.Recommendations) == 0 {
		return alert{}, errors.New("missing or invalid alert field: spec.analysis.recommendations")
	}

	resourceName, found, err := unstructured.NestedString(object.Object, "spec", "identity", "anchor", "name")
	if err != nil || !found || strings.TrimSpace(resourceName) == "" {
		return alert{}, errors.New("missing or invalid alert field: spec.identity.anchor.name")
	}

	for _, field := range []struct {
		name  string
		value string
	}{
		{"title", analysis.Title},
		{"severity", analysis.Severity},
		{"impact", analysis.Impact},
		{"diagnosis", analysis.Diagnosis},
		{"explanation", analysis.Explanation},
	} {
		if strings.TrimSpace(field.value) == "" {
			return alert{}, fmt.Errorf("missing or invalid alert field: spec.analysis.%s", field.name)
		}
	}

	for i, text := range analysis.Recommendations {
		if strings.TrimSpace(text) == "" {
			return alert{}, fmt.Errorf("missing or invalid alert field: spec.analysis.recommendations[%d]", i)
		}
	}

	return alert{
		Title:       analysis.Title,
		Severity:    strings.ToUpper(analysis.Severity[:1]) + analysis.Severity[1:],
		Impact:      strings.ToUpper(analysis.Impact[:1]) + analysis.Impact[1:],
		Source:      resourceName,
		Diagnosis:   analysis.Diagnosis,
		Explanation: analysis.Explanation,

		Recommendations: strings.Join(analysis.Recommendations, "\n"),
	}, nil
}

// postWithRetry retries failed POSTs up to three times, waiting five seconds after each failure.
// Cancellation interrupts both requests and waits; retries use the original payload.
func postWithRetry(ctx context.Context, client *http.Client, endpoint string, payload alert, name string, logger *log.Logger) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := post(ctx, client, endpoint, payload)
		if err == nil || attempt == 3 {
			return err
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		logger.Printf("WARN: Incident %s: %s; retry %d/3 in 5 seconds", name, err, attempt+1)

		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// post makes one bounded JSON POST without redirects, retries or logging response bodies or URLs.
func post(ctx context.Context, client *http.Client, endpoint string, payload alert) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return errors.New("cannot encode alert payload")
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return errors.New("cannot construct alert POST")
	}

	request.Header.Set("Content-Type", "application/json")

	response, err := client.Do(request)
	if err != nil {
		return errors.New("alert POST failed; check endpoint connectivity")
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("alert POST returned HTTP %d", response.StatusCode)
	}

	return nil
}
