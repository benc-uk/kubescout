/*
Purpose: Construct the dynamic Kubernetes client for the selected auth mode.
Flow: Validated configuration selects in-cluster credentials or a kubeconfig
context without cross-mode fallback, returning a client and cluster identifier.
The same private REST configuration supplies independent guarded describe reads.
This bootstrap performs no AI assessment or workload operation.
Read-only assessments reject non-GET Kubernetes requests at the shared transport boundary.
*/
package kube

import (
	"errors"
	"net/http"
	"net/url"
	"os"

	"github.com/benc-uk/kubescout/internal/config"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Connection shares explicit authentication with collection and supplemental describe.
type Connection struct {
	dynamic.Interface
	DescribeConfig *rest.Config
}

type readOnlyTransport struct {
	base http.RoundTripper
}

// RoundTrip rejects Kubernetes writes before sending them while retaining GET/list/watch access.
func (t readOnlyTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method != http.MethodGet {
		return nil, errors.New("read-only Kubernetes access rejects non-GET requests")
	}

	return t.base.RoundTrip(request)
}

// Client builds a rate-limited dynamic client and cluster identifier for the selected auth mode,
// carrying the same private REST configuration for guarded describe without authentication fallback.
func Client(c config.Config) (dynamic.Interface, string, error) {
	var (
		rc  *rest.Config
		err error
	)

	identifier := c.Context
	if c.Auth == "in-cluster" {
		rc, err = rest.InClusterConfig()
		identifier = os.Getenv("KUBERNETES_SERVICE_HOST")
	} else {
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		rules.ExplicitPath = c.Kubeconfig

		loaded, loadErr := rules.Load()
		if loadErr != nil {
			return nil, identifier, errors.New("kubeconfig could not be loaded")
		}

		if identifier == "" {
			identifier = loaded.CurrentContext
		}

		if identifier == "" {
			return nil, identifier, errors.New("kubeconfig has no current context; select one or provide --context")
		}

		rc, err = clientcmd.NewNonInteractiveClientConfig(*loaded, identifier, &clientcmd.ConfigOverrides{}, rules).ClientConfig()
	}

	if err != nil {
		return nil, identifier, errors.New("Kubernetes credentials could not be loaded for the selected authentication mode")
	}

	u, err := url.Parse(rc.Host)
	if err != nil || u.Hostname() == "" {
		return nil, identifier, errors.New("invalid Kubernetes API endpoint")
	}

	rc.UserAgent = "kubescout/go"
	// Long-lived watches have their own context deadline; lists use request-timeout.
	rc.Timeout = 0
	rc.QPS, rc.Burst = 10, 20

	if c.ReadOnly {
		rc.Wrap(func(base http.RoundTripper) http.RoundTripper {
			return readOnlyTransport{base: base}
		})
	}

	client, err := dynamic.NewForConfig(rc)
	if err != nil {
		return nil, identifier, errors.New("Kubernetes client initialisation failed")
	}

	return &Connection{Interface: client, DescribeConfig: rest.CopyConfig(rc)}, identifier, nil
}
