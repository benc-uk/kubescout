package kube

import (
	"errors"
	"net/url"
	"os"

	"k8s-ops-agent/internal/config"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func Client(c config.Config) (dynamic.Interface, string, error) {
	var rc *rest.Config
	var err error
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
	rc.UserAgent = "k8s-health/go"
	// Long-lived watches have their own context deadline; lists use request-timeout.
	rc.Timeout = 0
	rc.QPS, rc.Burst = 10, 20
	client, err := dynamic.NewForConfig(rc)
	if err != nil {
		return nil, identifier, errors.New("Kubernetes client initialisation failed")
	}
	return client, identifier, nil
}
