package cluster

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"k8s-ops-agent/internal/incident"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
)

func TestDashboardDeploymentContract(t *testing.T) {
	objects := manifests(t, "dashboard/deployment.yaml")
	if len(objects) != 1 {
		t.Fatal("expected one independent Deployment")
	}
	d, ok := objects[0].(*appsv1.Deployment)
	if !ok || d.Name != "k8s-dashboard" || d.Namespace != "k8s-health" || d.Spec.Replicas == nil || *d.Spec.Replicas != 1 {
		t.Fatal("incorrect dashboard deployment")
	}
	p := d.Spec.Template.Spec
	if p.ServiceAccountName != "k8s-dashboard" || len(p.Containers) != 1 || len(p.Volumes) != 0 ||
		p.AutomountServiceAccountToken == nil || !*p.AutomountServiceAccountToken {
		t.Fatal("dashboard must use a separate in-cluster identity without extra containers/volumes")
	}
	if p.SecurityContext == nil || p.SecurityContext.RunAsNonRoot == nil || !*p.SecurityContext.RunAsNonRoot ||
		p.SecurityContext.RunAsUser == nil || *p.SecurityContext.RunAsUser != 65532 ||
		p.SecurityContext.SeccompProfile == nil || p.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatal("unsafe dashboard pod")
	}
	c := p.Containers[0]
	if c.Name != "dashboard" || c.Image != "localhost/k8s-dashboard:dev" || c.ImagePullPolicy != corev1.PullNever ||
		!slices.Equal(c.Args, []string{"--auth-mode=in-cluster", "--namespace=k8s-health"}) || len(c.Env) != 0 || len(c.EnvFrom) != 0 ||
		len(c.Ports) != 1 || c.Ports[0].Name != "http" || c.Ports[0].ContainerPort != 8000 {
		t.Fatal("incorrect dashboard runtime; no model settings allowed")
	}
	if c.SecurityContext == nil || c.SecurityContext.ReadOnlyRootFilesystem == nil || !*c.SecurityContext.ReadOnlyRootFilesystem ||
		c.SecurityContext.AllowPrivilegeEscalation == nil || *c.SecurityContext.AllowPrivilegeEscalation ||
		c.SecurityContext.Capabilities == nil || !slices.Equal(c.SecurityContext.Capabilities.Drop, []corev1.Capability{"ALL"}) ||
		len(c.Resources.Requests) != 2 || len(c.Resources.Limits) != 2 {
		t.Fatal("unsafe dashboard container")
	}
	services := manifests(t, "dashboard/service.yaml")
	if len(services) != 1 {
		t.Fatal("expected one Service")
	}
	service, ok := services[0].(*corev1.Service)
	if !ok || service.Spec.Type != corev1.ServiceTypeClusterIP || len(service.Spec.Ports) != 1 ||
		service.Spec.Ports[0].Port != 8000 || service.Spec.Ports[0].TargetPort.StrVal != "http" ||
		service.Spec.Selector["app.kubernetes.io/name"] != d.Spec.Template.Labels["app.kubernetes.io/name"] {
		t.Fatal("dashboard must expose only ClusterIP port 8000")
	}
}

func TestDashboardRBACAndBundle(t *testing.T) {
	objects := manifests(t, "dashboard/rbac.yaml")
	if len(objects) != 3 {
		t.Fatal("expected ServiceAccount, Role and RoleBinding only")
	}
	seen := map[string]bool{}
	for _, obj := range objects {
		switch v := obj.(type) {
		case *corev1.ServiceAccount:
			seen["sa"] = v.Name == "k8s-dashboard" && v.Namespace == "k8s-health"
		case *rbacv1.Role:
			seen["role"] = v.Name == "k8s-dashboard" && v.Namespace == "k8s-health" && len(v.Rules) == 1 &&
				slices.Equal(v.Rules[0].APIGroups, []string{incident.GVR.Group}) &&
				slices.Equal(v.Rules[0].Resources, []string{"clusterissues"}) &&
				slices.Equal(v.Rules[0].Verbs, []string{"get", "list"}) &&
				len(v.Rules[0].ResourceNames) == 0 && len(v.Rules[0].NonResourceURLs) == 0
		case *rbacv1.RoleBinding:
			seen["binding"] = v.Name == "k8s-dashboard" && v.Namespace == "k8s-health" &&
				v.RoleRef.Kind == "Role" && v.RoleRef.Name == "k8s-dashboard" && len(v.Subjects) == 1 &&
				v.Subjects[0].Kind == "ServiceAccount" && v.Subjects[0].Name == "k8s-dashboard" && v.Subjects[0].Namespace == "k8s-health"
		default:
			t.Fatal("unexpected dashboard permission type")
		}
	}
	if !seen["sa"] || !seen["role"] || !seen["binding"] {
		t.Fatal("dashboard access must be namespaced get/list only")
	}
	for _, name := range []string{"kustomization.yaml", "dashboard/kustomization.yaml"} {
		data, err := os.ReadFile(filepath.Join("..", "..", "deploy", name))
		if err != nil {
			t.Fatal(err)
		}
		if name == "kustomization.yaml" && strings.Contains(string(data), "dashboard") {
			t.Fatal("dashboard must not be deployed with the agent")
		}
		if name == "dashboard/kustomization.yaml" &&
			(strings.Contains(string(data), "crd") || strings.Contains(string(data), "namespace.yaml") || strings.Contains(string(data), "../")) {
			t.Fatal("dashboard bundle must not own shared resources")
		}
	}
}
