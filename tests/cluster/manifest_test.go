/*
Purpose: Check agent deployment, RBAC and Incident schema contracts offline.
Flow: Decode checked-in manifests and validate synthetic configuration and incidents
to check inactive deployment defaults, workload read permissions, incident writes
and lifecycle.state migration plus the bounded transcript mount without activating a cluster or contacting a model.
Shared decoding helpers also supply manifests to opt-in integration fixtures.
*/
package cluster

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"k8s.io/kube-openapi/pkg/validation/strfmt"
	"k8s.io/kube-openapi/pkg/validation/validate"
)

// manifests reads checked-in deployment YAML into typed objects without contacting Kubernetes.
func manifests(t *testing.T, name string) []runtime.Object {
	t.Helper()

	f, err := os.Open(filepath.Join("..", "..", "deploy", name))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	d := yaml.NewYAMLOrJSONDecoder(f, 4096)
	out := []runtime.Object{}

	for {
		var raw json.RawMessage

		err := d.Decode(&raw)
		if err == io.EOF {
			return out
		}

		if err != nil {
			t.Fatal(err)
		}

		obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(raw, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		out = append(out, obj)
	}
}

// deploymentManifest loads the agent manifest and requires exactly one typed Deployment.
func deploymentManifest(t *testing.T) *appsv1.Deployment {
	t.Helper()

	objects := manifests(t, "deployment.yaml")
	if len(objects) != 1 {
		t.Fatal("expected one Deployment")
	}

	d, ok := objects[0].(*appsv1.Deployment)
	if !ok {
		t.Fatal("wrong deployment kind")
	}

	return d
}

// TestDeploymentContract checks offline agent manifests for explicit activation, hardened runtime,
// namespace-filter defaults and required AI configuration references without reading credentials.
func TestDeploymentContract(t *testing.T) {
	d := deploymentManifest(t)
	if d.Spec.Replicas == nil || *d.Spec.Replicas != 0 || d.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType {
		t.Fatal("deployment must require explicit activation")
	}

	p := d.Spec.Template.Spec
	if p.ServiceAccountName != "kubescout" || p.AutomountServiceAccountToken == nil || !*p.AutomountServiceAccountToken {
		t.Fatal("dedicated identity missing")
	}

	if p.SecurityContext == nil || p.SecurityContext.RunAsNonRoot == nil || !*p.SecurityContext.RunAsNonRoot || p.SecurityContext.RunAsUser == nil || *p.SecurityContext.RunAsUser != 65532 || p.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatal("unsafe pod security")
	}

	if len(p.Containers) != 1 {
		t.Fatal("unexpected container")
	}

	c := p.Containers[0]
	if c.Name != "agent" {
		t.Fatal("AI runtime container must be named agent")
	}

	options, err := config.Parse(c.Args, func(key string) string {
		switch key {
		case "AZURE_OPENAI_ENDPOINT":
			return "https://fixture.example/openai/v1/"
		case "AZURE_OPENAI_API_KEY", "AZURE_AI_MODEL_DEPLOYMENT_NAME":
			return "fixture"
		}

		return ""
	}, io.Discard)
	if err != nil || options.Auth != "in-cluster" || options.IncidentNamespace != "kubescout" || options.ObserverID != "kind-all" ||
		len(options.NamespaceWatch) != 0 || len(options.NamespaceIgnore) != 0 {
		t.Fatal("incorrect AI runtime arguments")
	}

	if len(c.Env) != 2 || c.Env[0].Name != "NAMESPACE_WATCH" || c.Env[1].Name != "NAMESPACE_IGNORE" ||
		c.Env[0].Value != "" || c.Env[1].Value != "" || c.Env[0].ValueFrom != nil || c.Env[1].ValueFrom != nil {
		t.Fatal("default deployment must watch all namespaces with explicit optional filters")
	}

	if len(c.EnvFrom) != 1 || c.EnvFrom[0].SecretRef == nil || c.EnvFrom[0].SecretRef.Name != "kubescout-model" {
		t.Fatal("explicit runtime model Secret reference missing")
	}

	if c.ImagePullPolicy != corev1.PullNever || c.SecurityContext == nil || c.SecurityContext.ReadOnlyRootFilesystem == nil || !*c.SecurityContext.ReadOnlyRootFilesystem || c.SecurityContext.AllowPrivilegeEscalation == nil || *c.SecurityContext.AllowPrivilegeEscalation || !slices.Contains(c.SecurityContext.Capabilities.Drop, corev1.Capability("ALL")) {
		t.Fatal("unsafe container")
	}
}

// TestTranscriptDeploymentStorage checks the only writable mount without enabling the agent or loosening its root filesystem.
func TestTranscriptDeploymentStorage(t *testing.T) {
	p := deploymentManifest(t).Spec.Template.Spec

	c := p.Containers[0]
	if len(p.Volumes) != 1 || p.Volumes[0].Name != "ai-transcripts" || p.Volumes[0].EmptyDir == nil ||
		p.Volumes[0].EmptyDir.SizeLimit == nil || p.Volumes[0].EmptyDir.SizeLimit.Value() != 128*1024*1024 ||
		len(c.VolumeMounts) != 1 || c.VolumeMounts[0].Name != "ai-transcripts" || c.VolumeMounts[0].MountPath != "/var/log/kubescout" ||
		p.SecurityContext.FSGroup == nil || *p.SecurityContext.FSGroup != 65532 ||
		!slices.Contains(c.Args, "--ai-log-dir=/var/log/kubescout/ai") ||
		c.SecurityContext.ReadOnlyRootFilesystem == nil || !*c.SecurityContext.ReadOnlyRootFilesystem ||
		c.StartupProbe != nil || c.ReadinessProbe != nil || c.LivenessProbe != nil {
		t.Fatal("unexpected storage or probes")
	}
}

// TestRBACContract checks offline manifests for allow-listed workload reads and namespaced
// Incident reconciliation permissions, rejecting unexpected access.
func TestRBACContract(t *testing.T) {
	incidents, binding, workloads, workloadBinding := false, false, false, false

	for _, obj := range manifests(t, "rbac.yaml") {
		var rules []rbacv1.PolicyRule

		switch v := obj.(type) {
		case *rbacv1.ClusterRole:
			rules = v.Rules
			switch v.Name {
			case "kubescout-nodes":
				if len(rules) != 1 || !slices.Equal(rules[0].Resources, []string{"nodes"}) {
					t.Fatal("incorrect node permissions")
				}
			case "kubescout-resources":
				workloads = len(rules) == 5 &&
					slices.Equal(rules[0].Resources, []string{"pods", "services"}) && slices.Equal(rules[0].APIGroups, []string{""}) &&
					slices.Equal(rules[1].Resources, []string{"events"}) && slices.Equal(rules[1].APIGroups, []string{""}) &&
					slices.Equal(rules[2].Resources, []string{"deployments", "replicasets", "statefulsets", "daemonsets"}) && slices.Equal(rules[2].APIGroups, []string{"apps"}) &&
					slices.Equal(rules[3].Resources, []string{"jobs"}) && slices.Equal(rules[3].APIGroups, []string{"batch"}) &&
					slices.Equal(rules[4].Resources, []string{"endpointslices"}) && slices.Equal(rules[4].APIGroups, []string{"discovery.k8s.io"})
			default:
				t.Fatal("unexpected cluster permissions")
			}
		case *rbacv1.ClusterRoleBinding:
			if v.Name == "kubescout-resources" {
				workloadBinding = v.RoleRef.Kind == "ClusterRole" && v.RoleRef.Name == v.Name && len(v.Subjects) == 1 &&
					v.Subjects[0].Name == "kubescout" && v.Subjects[0].Namespace == "kubescout"
			}
		case *rbacv1.Role:
			if v.Name == "kubescout-incidents" {
				if v.Namespace != "kubescout" || len(v.Rules) != 1 || !slices.Equal(v.Rules[0].Resources, []string{"incidents"}) || !slices.Equal(v.Rules[0].Verbs, []string{"get", "list", "create", "patch"}) || !slices.Equal(v.Rules[0].APIGroups, []string{incident.GVR.Group}) {
					t.Fatal("incident permissions exceed reconciliation needs")
				}

				incidents = true

				continue
			}

			t.Fatal("unexpected namespaced permissions")
		case *rbacv1.RoleBinding:
			if v.Name == "kubescout-incidents" {
				binding = v.Namespace == "kubescout" && v.RoleRef.Name == v.Name && len(v.Subjects) == 1 && v.Subjects[0].Name == "kubescout" && v.Subjects[0].Namespace == "kubescout"
			}
		}

		for _, r := range rules {
			if slices.Equal(r.Resources, []string{"events"}) && slices.Equal(r.Verbs, []string{"list", "watch"}) {
				continue
			}

			if !slices.Equal(r.Verbs, []string{"get", "list", "watch"}) {
				t.Fatal("workload permissions must be read-only")
			}

			for _, resource := range r.Resources {
				if !slices.Contains([]string{"nodes", "pods", "services", "events", "deployments", "replicasets", "statefulsets", "daemonsets", "endpointslices", "jobs"}, resource) {
					t.Fatal("unexpected workload access")
				}
			}
		}
	}

	if !incidents || !binding || !workloads || !workloadBinding {
		t.Fatal("incident reconciliation or multi-namespace read permissions missing")
	}
}

// TestIncidentSchemaAndMigration validates synthetic incidents against the checked-in CRD,
// requiring lifecycle.state and immutable identity while preserving safe record handling.
func TestIncidentSchemaAndMigration(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "incidents-crd.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	jsonData, err := yaml.ToJSON(data)
	if err != nil {
		t.Fatal(err)
	}

	var crd struct {
		Metadata struct{ Name string }
		Spec     struct {
			Group, Scope string
			Names        struct{ Kind, Plural, Singular string }
			Versions     []struct {
				AdditionalPrinterColumns []struct{ Name, JSONPath string }
				Schema                   struct {
					OpenAPI spec.Schema `json:"openAPIV3Schema"`
				} `json:"schema"`
			}
		}
	}
	if json.Unmarshal(jsonData, &crd) != nil {
		t.Fatal("invalid CRD")
	}

	if crd.Metadata.Name != "incidents."+incident.GVR.Group || crd.Spec.Group != incident.GVR.Group ||
		crd.Spec.Scope != "Namespaced" || crd.Spec.Names.Kind != "Incident" ||
		crd.Spec.Names.Plural != "incidents" || crd.Spec.Names.Singular != "incident" || len(crd.Spec.Versions) != 1 {
		t.Fatal("incorrect incident API")
	}

	schema := crd.Spec.Versions[0].Schema.OpenAPI
	if !slices.ContainsFunc(crd.Spec.Versions[0].AdditionalPrinterColumns, func(column struct{ Name, JSONPath string }) bool {
		return column.Name == "State" && column.JSONPath == ".spec.lifecycle.state"
	}) {
		t.Fatal("kubectl printer column must use lifecycle.state")
	}

	familySchema := schema.Properties["spec"].Properties["identity"].Properties["family"]
	for _, family := range incident.Families {
		if err := validate.AgainstSchema(&familySchema, family, strfmt.Default); err != nil {
			t.Fatalf("CRD rejects validated incident family %q: %v", family, err)
		}
	}

	identity := schema.Properties["spec"].Properties["identity"]

	rules, _ := json.Marshal(identity.Extensions["x-kubernetes-validations"])
	if !strings.Contains(string(rules), "self == oldSelf") {
		t.Fatal("identity not immutable")
	}

	if schema.Properties["spec"].Extensions["x-kubernetes-validations"] != nil {
		t.Fatal("incident conclusion cannot be immutable")
	}

	r := health.Resource{Kind: "Pod", Namespace: "apps", Name: "pod", UID: "uid"}

	value := incident.Incident{Identity: incident.Identity{Observer: "fixture", Anchor: r, Family: "container-failure"}, Resources: []health.Resource{r},
		Analysis:  incident.Analysis{Title: "Crash loop", Severity: "medium", Impact: "degraded", Diagnosis: "repeated-exit", Explanation: "Observed repeated exits", EvidenceIDs: []string{"evidence"}, Recommendations: []string{"Inspect configuration"}, Uncertainty: "Cause unknown", Model: "fixture", PromptVersion: incident.PromptVersion},
		Lifecycle: incident.Lifecycle{State: "open", Revision: 1, RaisedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}}
	for _, state := range []string{"open", "resolved"} {
		value.Lifecycle.State = state
		if state == "resolved" {
			now := time.Now().UTC()
			value.Lifecycle.ResolvedAt = &now
		}

		raw, _ := json.Marshal(value)

		var payload any
		json.Unmarshal(raw, &payload)

		obj := map[string]any{"apiVersion": incident.APIVersion, "kind": "Incident", "metadata": map[string]any{"name": incident.Name(value.Identity)}, "spec": payload}
		if err := validate.AgainstSchema(&schema, obj, strfmt.Default); err != nil {
			t.Fatal(err)
		}

		lifecycle := payload.(map[string]any)["lifecycle"].(map[string]any)
		lifecycle["phase"] = lifecycle["state"]
		delete(lifecycle, "state")

		if err := validate.AgainstSchema(&schema, obj, strfmt.Default); err == nil {
			t.Fatal("old lifecycle.phase must require migration")
		}
	}

	bundle, err := os.ReadFile(filepath.Join("..", "..", "deploy", "kustomization.yaml"))
	if err != nil || strings.Contains(string(bundle), "healthreports-crd") || !strings.Contains(string(bundle), "incidents-crd.yaml") {
		t.Fatal("active bundle retains legacy writer schema")
	}

	makefile, err := os.ReadFile(filepath.Join("..", "..", "Makefile"))
	if err != nil || strings.Contains(string(makefile), "delete -k deploy") || strings.Contains(string(makefile), "ARGS ?= --deterministic-only") {
		t.Fatal("unsafe migration or old deterministic default")
	}
}
