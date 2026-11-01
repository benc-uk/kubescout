/*
Purpose: Construct the standalone dashboard's bounded Kubernetes client.
Flow: Explicit in-cluster or kubeconfig options select authentication without
mode fallback. The resulting dynamic client applies request timeouts, rate
limits and response-body bounds for dashboard issue reads and human triage.
*/
package dashboard

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type ClientOptions struct {
	AuthMode   string
	Context    string
	Kubeconfig string
}

type boundedTransport struct{ next http.RoundTripper }

type boundedBody struct {
	io.ReadCloser
	remaining int64
}

// Validate rejects unsupported authentication modes and incompatible kubeconfig overrides.
func (o ClientOptions) Validate() error {
	switch o.AuthMode {
	case "in-cluster":
		if o.Context != "" || o.Kubeconfig != "" {
			return errors.New("--context and --kubeconfig require --auth-mode=kubeconfig")
		}
	case "kubeconfig":
		if o.Context != "" && strings.TrimSpace(o.Context) == "" {
			return errors.New("--context must be a valid kubeconfig context name")
		}
	default:
		return errors.New("--auth-mode must be in-cluster or kubeconfig")
	}

	return nil
}

// Client loads credentials for the explicitly selected mode and context without authentication fallback.
// It returns a bounded dashboard client and sanitizes credential-loading failures.
func Client(options ClientOptions) (dynamic.Interface, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}

	var (
		config *rest.Config
		err    error
	)
	if options.AuthMode == "in-cluster" {
		config, err = rest.InClusterConfig()
	} else {
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		rules.ExplicitPath = options.Kubeconfig

		loaded, loadErr := rules.Load()
		if loadErr != nil {
			return nil, errors.New("kubeconfig could not be loaded")
		}

		selected := options.Context
		if selected == "" {
			selected = loaded.CurrentContext
		}

		if selected == "" {
			return nil, errors.New("kubeconfig has no current context; select one or provide --context")
		}

		config, err = clientcmd.NewNonInteractiveClientConfig(*loaded, selected,
			&clientcmd.ConfigOverrides{}, rules).ClientConfig()
	}

	if err != nil {
		return nil, errors.New("Kubernetes credentials could not be loaded for the selected authentication mode and context")
	}

	return newClient(config)
}

// newClient copies the REST configuration and applies dashboard timeouts, rate limits, and response bounds.
func newClient(config *rest.Config) (dynamic.Interface, error) {
	config = rest.CopyConfig(config)
	config.UserAgent = "kubescout-dashboard/go"
	config.Timeout = RequestTimeout
	config.QPS, config.Burst = 5, 10
	config.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return boundedTransport{next}
	})

	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, errors.New("dashboard Kubernetes client initialization failed")
	}

	return client, nil
}

// RoundTrip delegates the HTTP request and wraps successful response bodies with a read limit.
func (t boundedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	response, err := t.next.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	response.Body = &boundedBody{ReadCloser: response.Body, remaining: MaxResponseBytes}

	return response, nil
}

// Read enforces the remaining response budget and detects overflow without returning excess bytes.
func (b *boundedBody) Read(p []byte) (int, error) {
	if int64(len(p)) > b.remaining+1 {
		p = p[:b.remaining+1]
	}

	n, err := b.ReadCloser.Read(p)
	if int64(n) > b.remaining {
		allowed := int(b.remaining)
		b.remaining = 0

		return allowed, errResponseSize
	}

	b.remaining -= int64(n)

	return n, err
}
