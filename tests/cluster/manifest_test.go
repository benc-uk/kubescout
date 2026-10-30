package cluster

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/scheme"
)

func manifests(t *testing.T, name string) []runtime.Object {
	t.Helper()
	f, err := os.Open(filepath.Join("..", "..", "deploy", name))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	decoder := yaml.NewYAMLOrJSONDecoder(f, 4096)
	var objects []runtime.Object
	for {
		var raw json.RawMessage
		err := decoder.Decode(&raw)
		if err == io.EOF {
			return objects
		}
		if err != nil {
			t.Fatal(err)
		}
		obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(raw, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		objects = append(objects, obj)
	}
}

func deploymentManifest(t *testing.T) *appsv1.Deployment {
	t.Helper()
	objects := manifests(t, "deployment.yaml")
	if len(objects) != 1 {
		t.Fatal("expected one Deployment")
	}
	d, ok := objects[0].(*appsv1.Deployment)
	if !ok {
		t.Fatal("deployment manifest has the wrong kind")
	}
	return d
}

func TestDeploymentContract(t *testing.T) {
	d := deploymentManifest(t)
	if d.Namespace != "k8s-health" || d.Spec.Replicas == nil || *d.Spec.Replicas != 1 || d.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType {
		t.Fatal("expected a single-replica Recreate Deployment")
	}
	p := d.Spec.Template.Spec
	if p.ServiceAccountName != "k8s-health" || p.AutomountServiceAccountToken == nil || !*p.AutomountServiceAccountToken {
		t.Fatal("dedicated in-cluster identity missing")
	}
	if p.SecurityContext == nil || p.SecurityContext.RunAsNonRoot == nil || !*p.SecurityContext.RunAsNonRoot || p.SecurityContext.RunAsUser == nil || *p.SecurityContext.RunAsUser != 65532 || p.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatal("unsafe pod security context")
	}
	if len(p.Containers) != 1 {
		t.Fatal("unexpected containers")
	}
	c := p.Containers[0]
	expectedArgs := []string{"--auth-mode=in-cluster", "--namespace=default", "--deterministic-only"}
	if !slices.Equal(c.Args, expectedArgs) {
		t.Fatalf("unexpected observer arguments: %v", c.Args)
	}
	if c.ImagePullPolicy != corev1.PullNever || c.SecurityContext == nil || c.SecurityContext.ReadOnlyRootFilesystem == nil || !*c.SecurityContext.ReadOnlyRootFilesystem || c.SecurityContext.AllowPrivilegeEscalation == nil || *c.SecurityContext.AllowPrivilegeEscalation || !slices.Contains(c.SecurityContext.Capabilities.Drop, corev1.Capability("ALL")) {
		t.Fatal("unsafe container security context")
	}
	if c.StartupProbe != nil || c.LivenessProbe != nil || c.ReadinessProbe != nil {
		t.Fatal("observer probes are deferred until production hardening")
	}
	if len(p.Volumes) != 0 || len(c.VolumeMounts) != 0 || p.SecurityContext.FSGroup != nil {
		t.Fatal("observer must not configure a writable progress volume")
	}
	if len(c.Env) != 0 || len(c.EnvFrom) != 0 {
		t.Fatal("default Deployment must not inject model credentials")
	}
}

func TestRBACContract(t *testing.T) {
	for _, obj := range manifests(t, "rbac.yaml") {
		var rules []rbacv1.PolicyRule
		switch value := obj.(type) {
		case *rbacv1.ClusterRole:
			if len(value.Rules) != 1 || len(value.Rules[0].Resources) != 1 || value.Rules[0].Resources[0] != "nodes" {
				t.Fatal("cluster role must be node-only")
			}
			rules = value.Rules
		case *rbacv1.Role:
			if value.Namespace != "default" {
				t.Fatal("unexpected namespace scope")
			}
			rules = value.Rules
		case *corev1.ServiceAccount:
			if value.Namespace != "k8s-health" {
				t.Fatal("wrong ServiceAccount namespace")
			}
		}
		for _, rule := range rules {
			if len(rule.Verbs) != 2 || !slices.Contains(rule.Verbs, "list") || !slices.Contains(rule.Verbs, "watch") {
				t.Fatal("observer permissions must be list/watch only")
			}
			for _, resource := range rule.Resources {
				if !slices.Contains([]string{"nodes", "pods", "events", "deployments", "jobs"}, resource) {
					t.Fatalf("unexpected resource permission %s", resource)
				}
			}
		}
	}
}
