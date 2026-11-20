//go:build integration

/*
Purpose: Validate bounded in-cluster AI execution and its authoritative stored Incidents.
Flow: Explicitly selected integration tests require a context, model configuration,
an image and a preinstalled CRD; they accept AI no-action as successful completion.
Fixtures create a model Secret, workloads and RBAC in an owned namespace, with
ownership-checked namespace cleanup followed by preconditioned deletion of test-owned
cluster Incidents and RBAC. Offline HTTP fixtures verify incident cleanup boundaries.
*/
package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/incident"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// TestLiveDeployment runs a bounded in-cluster agent with required model, image and context configuration.
// It creates an owned model Secret, RBAC and workloads, then checks readable completion and stored
// AI provenance; successful AI no-action is valid and fixture cleanup removes owned resources.
func TestLiveDeployment(t *testing.T) {
	f := newFixtureStage(t, "ai-container")

	image := os.Getenv("KUBESCOUT_IMAGE")
	if image == "" {
		t.Fatal("KUBESCOUT_IMAGE required")
	}

	client, err := dynamic.NewForConfig(f.config)
	if err != nil {
		t.Fatal(err)
	}

	store := client.Resource(incident.GVR)
	if _, err := store.List(t.Context(), metav1.ListOptions{Limit: 1}); err != nil {
		t.Fatal("cluster-scoped Incident CRD must be installed separately")
	}

	f.incidents = store
	f.installRBAC()

	if err := config.LoadDotEnv(filepath.Join("..", "..")); err != nil {
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
	spec.Containers[0].Args = []string{"--once", "--auth-mode=in-cluster", "--namespace-watch=" + f.namespace,
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

	repository := incident.Kubernetes{Client: client, Observer: f.namespace, Limit: c.MaxObjects, Timeout: c.RequestTimeout}

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
// the owned namespace and registering UID-preconditioned cleanup for cluster-scoped Node and Incident RBAC.
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

			value.Name = f.namespace + "-" + strings.TrimPrefix(value.Name, "kubescout-")

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

			value.Name = f.namespace + "-" + strings.TrimPrefix(value.Name, "kubescout-")
			value.RoleRef.Name, value.Subjects[0].Namespace = value.Name, f.namespace

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

// cleanupFixtureIncidents deletes only a complete, bounded set of the fixture observer's Incidents.
// Both ownership fields are verified before any UID/resourceVersion-preconditioned deletion.
func cleanupFixtureIncidents(ctx context.Context, store dynamic.ResourceInterface, observer string) error {
	page, err := store.List(ctx, metav1.ListOptions{Limit: 1000,
		LabelSelector: "app.kubernetes.io/managed-by=kubescout,kubescout.benc-uk.github.com/observer=" + observer})
	if err != nil {
		return err
	}

	if page.GetContinue() != "" || len(page.Items) > 1000 || page.GetResourceVersion() == "" {
		return errors.New("fixture incident cleanup requires a complete bounded list")
	}

	for _, obj := range page.Items {
		owner, _, err := unstructured.NestedString(obj.Object, "spec", "identity", "observer")
		if err != nil || owner != observer || obj.GetLabels()["app.kubernetes.io/managed-by"] != "kubescout" ||
			obj.GetLabels()["kubescout.benc-uk.github.com/observer"] != observer ||
			obj.GetNamespace() != "" || obj.GetName() == "" || obj.GetUID() == "" || obj.GetResourceVersion() == "" {
			return errors.New("fixture incident ownership or identity changed; refusing cleanup")
		}
	}

	for _, obj := range page.Items {
		uid, version := obj.GetUID(), obj.GetResourceVersion()
		if err := store.Delete(ctx, obj.GetName(), metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version},
		}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

// TestFixtureIncidentCleanup uses offline HTTP fixtures to verify cluster-only, ownership-checked deletion.
// Invalid or incomplete lists must not delete even an otherwise valid first record.
func TestFixtureIncidentCleanup(t *testing.T) {
	for _, failure := range []string{"", "label", "observer", "namespace", "uid", "version", "pagination", "list", "delete"} {
		t.Run(failure, func(t *testing.T) {
			deletes := 0
			path := "/apis/kubescout.benc-uk.github.com/v1alpha1/incidents"

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")

				switch {
				case r.Method == http.MethodGet && r.URL.Path == path:
					if r.URL.Query().Get("labelSelector") != "app.kubernetes.io/managed-by=kubescout,kubescout.benc-uk.github.com/observer=owned" {
						t.Error("cleanup must select only the fixture observer")
					}

					if failure == "list" {
						w.WriteHeader(http.StatusForbidden)
						return
					}

					obj := &unstructured.Unstructured{Object: map[string]any{
						"apiVersion": incident.APIVersion, "kind": "Incident",
						"metadata": map[string]any{"name": "inc-fixture", "uid": "owned-uid", "resourceVersion": "1",
							"labels": map[string]any{"app.kubernetes.io/managed-by": "kubescout", "kubescout.benc-uk.github.com/observer": "owned"}},
						"spec": map[string]any{"identity": map[string]any{"observer": "owned"}},
					}}
					first := obj.DeepCopy()
					first.SetName("inc-first")

					switch failure {
					case "label":
						obj.SetLabels(map[string]string{"app.kubernetes.io/managed-by": "other"})
					case "observer":
						obj.Object["spec"] = map[string]any{"identity": map[string]any{"observer": "other"}}
					case "namespace":
						obj.SetNamespace("old-storage")
					case "uid":
						obj.SetUID("")
					case "version":
						obj.SetResourceVersion("")
					}

					page := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{*first, *obj}}
					page.SetAPIVersion(incident.APIVersion)
					page.SetKind("IncidentList")
					page.SetResourceVersion("1")

					if failure == "pagination" {
						page.SetContinue("more")
					}

					_ = json.NewEncoder(w).Encode(page)
				case r.Method == http.MethodDelete && (r.URL.Path == path+"/inc-first" || r.URL.Path == path+"/inc-fixture"):
					deletes++

					var options metav1.DeleteOptions
					if json.NewDecoder(r.Body).Decode(&options) != nil || options.Preconditions == nil ||
						options.Preconditions.UID == nil || *options.Preconditions.UID != "owned-uid" ||
						options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != "1" {
						t.Error("cleanup must use UID and resourceVersion preconditions")
					}

					if failure == "delete" {
						w.WriteHeader(http.StatusConflict)
						return
					}

					_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "Status", "status": "Success"})
				default:
					t.Error("cleanup escaped cluster-scoped Incident endpoints")
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()

			client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}

			err = cleanupFixtureIncidents(t.Context(), client.Resource(incident.GVR), "owned")

			wantDeletes := 0
			if failure == "" {
				wantDeletes = 2
			} else if failure == "delete" {
				wantDeletes = 1
			}

			if (err != nil) != (failure != "") || deletes != wantDeletes {
				t.Fatalf("cleanup err=%v deletes=%d want=%d", err, deletes, wantDeletes)
			}
		})
	}
}
