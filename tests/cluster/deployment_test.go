//go:build integration

/*
Purpose: Validate bounded in-cluster AI execution and its authoritative stored Incidents.
Flow: Integration tests require container and model authorization, explicit context,
an image and a preinstalled CRD; they accept AI no-action as successful completion.
Fixtures create a model Secret, workloads and RBAC in an owned namespace, with
ownership-checked namespace cleanup and UID-preconditioned cluster RBAC deletion.
*/
package cluster

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/incident"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
)

// TestLiveDeployment requires container and model authorization to run a bounded in-cluster agent.
// It creates an owned model Secret, RBAC and workloads, then checks readable completion and stored
// AI provenance; successful AI no-action is valid and fixture cleanup removes owned resources.
func TestLiveDeployment(t *testing.T) {
	if os.Getenv("KUBESCOUT_LIVE_CONTAINER") != "yes" {
		t.Skip("requires explicit container validation approval")
	}

	requireLiveModel(t)
	f := newFixtureStage(t, true, "ai-container")

	image := os.Getenv("KUBESCOUT_IMAGE")
	if image == "" {
		t.Fatal("KUBESCOUT_IMAGE required")
	}

	client, err := dynamic.NewForConfig(f.config)
	if err != nil {
		t.Fatal(err)
	}

	store := client.Resource(incident.GVR).Namespace(f.namespace)
	if _, err := store.List(t.Context(), metav1.ListOptions{Limit: 1}); err != nil {
		t.Fatal("Incident CRD must be installed separately")
	}

	f.installRBAC()

	if err := config.LoadDotEnv(filepath.Join("..", ".."), f.binary); err != nil {
		t.Fatal("invalid model configuration")
	}

	c, err := config.Parse([]string{"--auth-mode=in-cluster", "--namespace-watch=" + f.namespace, "--observer-id=" + f.namespace}, os.Getenv, &bytes.Buffer{})
	if err != nil {
		t.Fatal("model configuration missing or invalid")
	}

	_, err = f.client.CoreV1().Secrets(f.namespace).Create(t.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "kubescout-model"},
		StringData: map[string]string{"AZURE_OPENAI_ENDPOINT": c.Endpoint, "AZURE_OPENAI_API_KEY": c.APIKey, "AZURE_AI_MODEL_DEPLOYMENT_NAME": c.Model},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal("could not create owned temporary model Secret")
	}

	f.gatedPod("ai-evidence")

	spec := deploymentManifest(t).Spec.Template.Spec
	spec.RestartPolicy = corev1.RestartPolicyNever
	spec.Containers[0].Image = image
	spec.Containers[0].Args = []string{"--once", "--auth-mode=in-cluster", "--namespace-watch=" + f.namespace, "--incident-namespace=" + f.namespace,
		"--observer-id=" + f.namespace, "--startup-grace=0s", "--model-calls-per-hour=6", "--request-timeout=30s", "--run-timeout=120s",
		"--ai-log-dir=/var/log/kubescout/ai"}

	pod, err := f.client.CoreV1().Pods(f.namespace).Create(t.Context(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "agent-validation"}, Spec: spec}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	poll(t, 3*time.Minute, "bounded in-cluster AI completion", func() (bool, error) {
		p, err := f.client.CoreV1().Pods(f.namespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if p.Status.Phase == corev1.PodFailed {
			return false, errors.New("AI container failed")
		}

		return p.Status.Phase == corev1.PodSucceeded, nil
	})

	logs, err := f.client.CoreV1().Pods(f.namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: "agent", LimitBytes: new(int64(200000))}).DoRaw(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(logs), "Assessment completed") {
		t.Fatal("container did not produce a completed AI execution")
	}

	repository := incident.Kubernetes{Client: client, Namespace: f.namespace, Observer: f.namespace, Limit: c.MaxObjects, Timeout: c.RequestTimeout}

	records, err := repository.ListOpen(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	for _, record := range records {
		if record.Value.Identity.Observer != f.namespace || record.Value.Analysis.Model != c.Model ||
			record.Value.Analysis.PromptVersion != incident.PromptVersion || record.Value.Analysis.Title == "" {
			t.Fatal("stored incident lacks validated AI provenance")
		}
	}

	t.Logf("bounded container AI validation: %d stored AI incidents; no-action is valid", len(records))
}

// installRBAC creates live fixture identities and permissions, narrowing workload access to
// the owned namespace and registering UID-preconditioned cleanup for cluster-scoped node RBAC.
func (f *fixture) installRBAC() {
	f.t.Helper()

	for _, obj := range manifests(f.t, "rbac.yaml") {
		switch value := obj.(type) {
		case *corev1.ServiceAccount:
			value.Namespace = f.namespace
			if _, err := f.client.CoreV1().ServiceAccounts(f.namespace).Create(f.t.Context(), value, metav1.CreateOptions{}); err != nil {
				f.t.Fatal(err)
			}
		case *rbacv1.Role:
			value.Namespace = f.namespace
			if _, err := f.client.RbacV1().Roles(f.namespace).Create(f.t.Context(), value, metav1.CreateOptions{}); err != nil {
				f.t.Fatal(err)
			}
		case *rbacv1.RoleBinding:
			value.Namespace, value.Subjects[0].Namespace = f.namespace, f.namespace
			if _, err := f.client.RbacV1().RoleBindings(f.namespace).Create(f.t.Context(), value, metav1.CreateOptions{}); err != nil {
				f.t.Fatal(err)
			}
		case *rbacv1.ClusterRole:
			if value.Name == "kubescout-resources" {
				role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: value.Name, Namespace: f.namespace}, Rules: value.Rules}
				if _, err := f.client.RbacV1().Roles(f.namespace).Create(f.t.Context(), role, metav1.CreateOptions{}); err != nil {
					f.t.Fatal(err)
				}

				continue
			}

			value.Name = f.namespace + "-nodes"

			created, err := f.client.RbacV1().ClusterRoles().Create(f.t.Context(), value, metav1.CreateOptions{})
			if err != nil {
				f.t.Fatal(err)
			}

			f.t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				if err := f.client.RbacV1().ClusterRoles().Delete(ctx, created.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &created.UID}}); err != nil {
					f.t.Error(err)
				}
			})
		case *rbacv1.ClusterRoleBinding:
			if value.Name == "kubescout-resources" {
				binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: value.Name, Namespace: f.namespace},
					Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "kubescout", Namespace: f.namespace}},
					RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: value.RoleRef.Name}}
				if _, err := f.client.RbacV1().RoleBindings(f.namespace).Create(f.t.Context(), binding, metav1.CreateOptions{}); err != nil {
					f.t.Fatal(err)
				}

				continue
			}

			value.Name, value.RoleRef.Name, value.Subjects[0].Namespace = f.namespace+"-nodes", f.namespace+"-nodes", f.namespace

			created, err := f.client.RbacV1().ClusterRoleBindings().Create(f.t.Context(), value, metav1.CreateOptions{})
			if err != nil {
				f.t.Fatal(err)
			}

			f.t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				if err := f.client.RbacV1().ClusterRoleBindings().Delete(ctx, created.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &created.UID}}); err != nil {
					f.t.Error(err)
				}
			})
		}
	}
}
