//go:build integration

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"k8s-ops-agent/internal/config"
	"k8s-ops-agent/internal/report"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestLiveDeployment(t *testing.T) {
	if os.Getenv("K8S_HEALTH_LIVE_CONTAINER") != "yes" {
		t.Skip("requires explicit container/in-cluster validation authorisation")
	}
	f := newFixtureStage(t, false, "stage3")
	image := os.Getenv("K8S_HEALTH_IMAGE")
	engine := os.Getenv("K8S_HEALTH_CONTAINER_ENGINE")
	if image == "" || engine == "" {
		t.Fatal("K8S_HEALTH_IMAGE and K8S_HEALTH_CONTAINER_ENGINE are required")
	}
	binding := f.installRBAC()
	d := deploymentManifest(t)
	d.Namespace = f.namespace
	d.Spec.Template.Spec.Containers[0].Image = image
	d.Spec.Template.Spec.Containers[0].Args = []string{"--auth-mode=in-cluster", "--namespace=" + f.namespace, "--deterministic-only",
		"--check-interval=2s", "--request-timeout=2s", "--run-timeout=10s", "--watch-timeout=8s"}
	created, err := f.client.AppsV1().Deployments(f.namespace).Create(t.Context(), d, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("test Deployment creation failed: %v", err)
	}
	pod := f.observerPod()
	f.awaitObserverAssessment(pod.Name, false)
	t.Log("incomplete RBAC: observer reports unknown and retries without probes")
	if _, err := f.client.RbacV1().RoleBindings(f.namespace).Create(t.Context(), binding, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.awaitObserverAssessment(pod.Name, true)
	pod, err = f.client.CoreV1().Pods(f.namespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pod.Status.ContainerStatuses) != 1 || pod.Status.ContainerStatuses[0].RestartCount != 0 {
		t.Fatal("observer restarted during dependency failure")
	}
	f.verifyPermissions()
	f.localContainer(engine, image)
	if os.Getenv("K8S_HEALTH_LIVE_MODEL") == "yes" {
		f.modelPod(image)
	} else {
		t.Log("in-cluster model validation skipped: ALLOW_MODEL=yes not supplied")
	}
	current, err := f.client.AppsV1().Deployments(f.namespace).Get(t.Context(), created.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	current.Spec.Replicas = new(int32(0))
	start := time.Now()
	if _, err := f.client.AppsV1().Deployments(f.namespace).Update(t.Context(), current, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.noPods()
	if time.Since(start) > 15*time.Second {
		t.Fatal("in-cluster termination exceeded 15s grace")
	}
	t.Logf("in-cluster Deployment shutdown and pod removal=%s", time.Since(start).Round(time.Millisecond))
}

func (f *fixture) installRBAC() *rbacv1.RoleBinding {
	f.t.Helper()
	var pending *rbacv1.RoleBinding
	for _, obj := range manifests(f.t, "rbac.yaml") {
		switch value := obj.(type) {
		case *corev1.ServiceAccount:
			value.Namespace = f.namespace
			if _, err := f.client.CoreV1().ServiceAccounts(f.namespace).Create(f.t.Context(), value, metav1.CreateOptions{}); err != nil {
				f.t.Fatal(err)
			}
		case *rbacv1.ClusterRole:
			value.Name = f.namespace + "-nodes"
			created, err := f.client.RbacV1().ClusterRoles().Create(f.t.Context(), value, metav1.CreateOptions{})
			if err != nil {
				f.t.Fatal(err)
			}
			f.t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := f.client.RbacV1().ClusterRoles().Delete(ctx, created.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &created.UID}}); err != nil {
					f.t.Errorf("test ClusterRole cleanup: %v", err)
				}
			})
		case *rbacv1.ClusterRoleBinding:
			value.Name, value.RoleRef.Name = f.namespace+"-nodes", f.namespace+"-nodes"
			value.Subjects[0].Namespace = f.namespace
			created, err := f.client.RbacV1().ClusterRoleBindings().Create(f.t.Context(), value, metav1.CreateOptions{})
			if err != nil {
				f.t.Fatal(err)
			}
			f.t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := f.client.RbacV1().ClusterRoleBindings().Delete(ctx, created.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &created.UID}}); err != nil {
					f.t.Errorf("test ClusterRoleBinding cleanup: %v", err)
				}
			})
		case *rbacv1.Role:
			value.Namespace = f.namespace
			if _, err := f.client.RbacV1().Roles(f.namespace).Create(f.t.Context(), value, metav1.CreateOptions{}); err != nil {
				f.t.Fatal(err)
			}
		case *rbacv1.RoleBinding:
			value.Namespace, value.Subjects[0].Namespace = f.namespace, f.namespace
			pending = value
		}
	}
	if pending == nil {
		f.t.Fatal("missing scoped RoleBinding")
	}
	return pending
}

func (f *fixture) observerPod() *corev1.Pod {
	f.t.Helper()
	var pod *corev1.Pod
	poll(f.t, 90*time.Second, "observer container start", func() (bool, error) {
		list, err := f.client.CoreV1().Pods(f.namespace).List(f.t.Context(), metav1.ListOptions{LabelSelector: "app.kubernetes.io/name=k8s-health"})
		if err != nil {
			return false, err
		}
		for i := range list.Items {
			p := &list.Items[i]
			if len(p.Status.ContainerStatuses) == 1 && p.Status.ContainerStatuses[0].State.Running != nil {
				pod = p
				return true, nil
			}
		}
		return false, nil
	})
	return pod
}

func (f *fixture) verifyPermissions() {
	f.t.Helper()
	rc := rest.CopyConfig(f.config)
	rc.Impersonate.UserName = "system:serviceaccount:" + f.namespace + ":k8s-health"
	client, err := kubernetes.NewForConfig(rc)
	if err != nil {
		f.t.Fatal(err)
	}
	for _, check := range []struct {
		group, resource, verb, namespace, subresource string
		allowed                                       bool
	}{
		{"", "nodes", "list", "", "", true}, {"", "nodes", "watch", "", "", true},
		{"", "pods", "list", f.namespace, "", true}, {"", "events", "watch", f.namespace, "", true},
		{"apps", "deployments", "watch", f.namespace, "", true}, {"batch", "jobs", "list", f.namespace, "", true},
		{"", "secrets", "list", f.namespace, "", false}, {"", "pods", "list", "kube-system", "", false},
		{"", "pods", "create", f.namespace, "", false}, {"", "pods", "delete", f.namespace, "", false},
		{"", "pods", "create", f.namespace, "exec", false}, {"", "pods", "get", f.namespace, "log", false},
		{"", "nodes", "patch", "", "", false},
	} {
		review, err := client.AuthorizationV1().SelfSubjectAccessReviews().Create(f.t.Context(), &authorizationv1.SelfSubjectAccessReview{
			Spec: authorizationv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &authorizationv1.ResourceAttributes{
				Group: check.group, Resource: check.resource, Verb: check.verb, Namespace: check.namespace, Subresource: check.subresource,
			}},
		}, metav1.CreateOptions{})
		if err != nil || review.Status.Allowed != check.allowed {
			f.t.Fatalf("RBAC mismatch: %s %s/%s in %q", check.verb, check.resource, check.subresource, check.namespace)
		}
	}
	if _, err := client.CoreV1().Nodes().List(f.t.Context(), metav1.ListOptions{}); err != nil {
		f.t.Fatal("node read denied")
	}
	if _, err := client.CoreV1().Pods(f.namespace).List(f.t.Context(), metav1.ListOptions{}); err != nil {
		f.t.Fatal("scoped pod read denied")
	}
	if _, err := client.CoreV1().Secrets(f.namespace).List(f.t.Context(), metav1.ListOptions{}); !apierrors.IsForbidden(err) {
		f.t.Fatal("Secret list was not forbidden")
	}
	if _, err := client.CoreV1().Pods("kube-system").List(f.t.Context(), metav1.ListOptions{}); !apierrors.IsForbidden(err) {
		f.t.Fatal("unselected namespace list was not forbidden")
	}
	f.t.Log("ServiceAccount RBAC: scoped reads allowed; Secrets, other namespaces, logs, exec and writes denied")
}

func (f *fixture) localContainer(engine, image string) {
	f.t.Helper()
	token, err := f.client.CoreV1().ServiceAccounts(f.namespace).CreateToken(f.t.Context(), "k8s-health",
		&authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: new(int64(600))}}, metav1.CreateOptions{})
	if err != nil {
		f.t.Fatal("short-lived ServiceAccount token request failed")
	}
	ca := f.config.CAData
	if len(ca) == 0 && f.config.CAFile != "" {
		ca, err = os.ReadFile(f.config.CAFile)
		if err != nil {
			f.t.Fatal("cannot read cluster CA")
		}
	}
	if f.config.Insecure || len(ca) == 0 {
		f.t.Fatal("local container validation requires verified Kubernetes TLS")
	}
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["kind"] = &clientcmdapi.Cluster{Server: f.config.Host, CertificateAuthorityData: ca}
	cfg.AuthInfos["observer"] = &clientcmdapi.AuthInfo{Token: token.Status.Token}
	cfg.Contexts["observer"] = &clientcmdapi.Context{Cluster: "kind", AuthInfo: "observer"}
	data, err := clientcmd.Write(*cfg)
	if err != nil {
		f.t.Fatal("cannot encode scoped temporary kubeconfig")
	}
	path := filepath.Join(f.t.TempDir(), "kubeconfig")
	// Parent directory is private; the mapped non-root container UID needs read access.
	if err := os.WriteFile(path, data, 0644); err != nil {
		f.t.Fatal("cannot write scoped temporary kubeconfig")
	}
	ctx, cancel := context.WithTimeout(f.t.Context(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, engine, "run", "--rm", "--network=host", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges",
		"--volume", path+":/run/kubeconfig:ro", image, "--once", "--auth-mode=kubeconfig", "--context=observer",
		"--kubeconfig=/run/kubeconfig", "--namespace="+f.namespace, "--deterministic-only", "--request-timeout=5s", "--run-timeout=30s")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		f.t.Fatalf("local container validation failed: %s", stderr.String())
	}
	var out report.Report
	if json.Unmarshal(stdout.Bytes(), &out) != nil || !out.CollectionComplete || out.Execution != "completed" {
		f.t.Fatal("local container did not produce a complete report")
	}
	f.t.Log("local container: read-only filesystem, non-root image, scoped short-lived kubeconfig, verified TLS, complete JSON report")
}

func (f *fixture) awaitObserverAssessment(name string, complete bool) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.t.Context(), time.Minute)
	defer cancel()
	poll(f.t, time.Minute, "observer collection assessment in pod logs", func() (bool, error) {
		data, err := f.client.CoreV1().Pods(f.namespace).GetLogs(name, &corev1.PodLogOptions{Container: "observer", TailLines: new(int64(100))}).DoRaw(ctx)
		if err != nil {
			return false, err
		}
		for _, line := range bytes.Split(data, []byte("\n")) {
			if len(line) == 0 || line[0] != '{' {
				continue
			}
			var out report.Report
			if err := json.Unmarshal(line, &out); err != nil {
				return false, errors.New("invalid JSON in observer output")
			}
			if out.Type != "assessment" || out.CollectionComplete != complete {
				continue
			}
			if complete && out.Execution == "completed" {
				return true, nil
			}
			if !complete && out.Execution == "incomplete" && out.Health == "unknown" && len(out.Errors) > 0 {
				return true, nil
			}
		}
		return false, nil
	})
	f.t.Logf("in-cluster pod logs confirmed collection_complete=%t", complete)
}

func (f *fixture) modelPod(image string) {
	f.t.Helper()
	if err := config.LoadDotEnv(filepath.Join("..", ".."), f.binary); err != nil {
		f.t.Fatal("cannot load model configuration")
	}
	c, err := config.Parse([]string{"--auth-mode=in-cluster", "--namespace=" + f.namespace}, os.Getenv, &bytes.Buffer{})
	if err != nil {
		f.t.Fatal("model configuration is invalid or missing")
	}
	_, err = f.client.CoreV1().Secrets(f.namespace).Create(f.t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "model-runtime"},
		StringData: map[string]string{"AZURE_OPENAI_ENDPOINT": c.Endpoint, "AZURE_OPENAI_API_KEY": c.APIKey, "AZURE_AI_MODEL_DEPLOYMENT_NAME": c.Model},
	}, metav1.CreateOptions{})
	if err != nil {
		f.t.Fatal("temporary model Secret creation failed")
	}
	fixture := f.gatedPod("model-evidence")
	spec := deploymentManifest(f.t).Spec.Template.Spec
	container := &spec.Containers[0]
	container.Image = image
	container.Args = []string{"--once", "--auth-mode=in-cluster", "--namespace=" + f.namespace, "--startup-grace=0s", "--request-timeout=60s", "--run-timeout=90s", "--max-output-tokens=2000"}
	container.EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "model-runtime"}}}}
	spec.RestartPolicy = corev1.RestartPolicyNever
	p, err := f.client.CoreV1().Pods(f.namespace).Create(f.t.Context(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "model-validation"}, Spec: spec}, metav1.CreateOptions{})
	if err != nil {
		f.t.Fatal("model validation pod creation failed")
	}
	poll(f.t, 2*time.Minute, "in-cluster model completion", func() (bool, error) {
		current, err := f.client.CoreV1().Pods(f.namespace).Get(f.t.Context(), p.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if current.Status.Phase == corev1.PodFailed {
			return false, errors.New("model validation pod failed")
		}
		return current.Status.Phase == corev1.PodSucceeded, nil
	})
	data, err := f.client.CoreV1().Pods(f.namespace).GetLogs(p.Name, &corev1.PodLogOptions{Container: "observer", LimitBytes: new(int64(200000))}).DoRaw(f.t.Context())
	if err != nil {
		f.t.Fatal("cannot retrieve model validation report")
	}
	validated := false
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(line) == 0 || line[0] != '{' {
			continue
		}
		var out report.Report
		if json.Unmarshal(line, &out) == nil && out.CollectionComplete && out.Execution == "completed" && out.Analysis.Status == "completed" && len(out.Analysis.Items) > 0 {
			validated = true
		}
	}
	if !validated {
		f.t.Fatal("in-cluster model output was not validated")
	}
	f.t.Log("in-cluster model: one successful request with Secret-injected credentials and ServiceAccount Kubernetes authentication")
	f.deletePod(fixture)
	f.deletePod(p)
	if err := f.client.CoreV1().Secrets(f.namespace).Delete(f.t.Context(), "model-runtime", metav1.DeleteOptions{}); err != nil {
		f.t.Fatal("temporary Secret cleanup failed")
	}
}
