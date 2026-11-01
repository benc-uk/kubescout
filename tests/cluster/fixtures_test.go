//go:build integration

/*
Purpose: Build workload failure, recovery and load fixtures for live integration tests.
Flow: Authorized test callers create deployments, gated Pods and failed Jobs in
their owned namespace, then observe readiness or change images to drive evidence.
Explicit workload deletion uses UID preconditions; fixture teardown owns namespace
cleanup. These helpers make Kubernetes calls, not model or incident decisions.
*/
package cluster

import (
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// container returns a sleeping, resource-bounded workload specification for live test fixtures.
func container() corev1.Container {
	return corev1.Container{Name: "fixture", Image: fixtureImage, Command: []string{"sh", "-c", "sleep 600"},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1m"), corev1.ResourceMemory: resource.MustParse("4Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("32Mi")},
		}}
}

// deployment creates a single-replica Recreate workload in the owned live namespace.
func (f *fixture) deployment() *appsv1.Deployment {
	f.t.Helper()

	labels := map[string]string{"app": "health-stage2"}

	d, err := f.client.AppsV1().Deployments(f.namespace).Create(f.t.Context(), &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "rollout"},
		Spec: appsv1.DeploymentSpec{Replicas: new(int32(1)), Selector: &metav1.LabelSelector{MatchLabels: labels},
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{TerminationGracePeriodSeconds: new(int64(0)), Containers: []corev1.Container{container()}}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		f.t.Fatalf("create deployment: %v", err)
	}

	return d
}

// image updates the live fixture deployment's first container image to drive failure or recovery.
func (f *fixture) image(name, image string) {
	f.t.Helper()

	d, err := f.client.AppsV1().Deployments(f.namespace).Get(f.t.Context(), name, metav1.GetOptions{})
	if err != nil {
		f.t.Fatal(err)
	}

	d.Spec.Template.Spec.Containers[0].Image = image
	if _, err := f.client.AppsV1().Deployments(f.namespace).Update(f.t.Context(), d, metav1.UpdateOptions{}); err != nil {
		f.t.Fatal(err)
	}
}

// available waits for the live deployment's current generation to have one updated, available replica.
func (f *fixture) available(name string) {
	f.t.Helper()
	poll(f.t, 90*time.Second, "deployment availability", func() (bool, error) {
		d, err := f.client.AppsV1().Deployments(f.namespace).Get(f.t.Context(), name, metav1.GetOptions{})
		return err == nil && d.Status.ObservedGeneration >= d.Generation && d.Status.AvailableReplicas == 1 && d.Status.UpdatedReplicas == 1, err
	})
}

// gatedPod creates a live Pod held by a scheduling gate to produce stable Pending evidence.
func (f *fixture) gatedPod(name string) *corev1.Pod {
	f.t.Helper()

	p, err := f.client.CoreV1().Pods(f.namespace).Create(f.t.Context(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PodSpec{TerminationGracePeriodSeconds: new(int64(0)), Containers: []corev1.Container{container()},
			SchedulingGates: []corev1.PodSchedulingGate{{Name: "k8s-health-stage2/hold"}}},
	}, metav1.CreateOptions{})
	if err != nil {
		f.t.Fatalf("create gated pod: %v", err)
	}

	return p
}

// deletePod removes a live fixture Pod with a UID precondition, accepting an already absent Pod.
func (f *fixture) deletePod(p *corev1.Pod) {
	f.t.Helper()

	err := f.client.CoreV1().Pods(f.namespace).Delete(f.t.Context(), p.Name, metav1.DeleteOptions{
		GracePeriodSeconds: new(int64(0)), Preconditions: &metav1.Preconditions{UID: &p.UID},
	})
	if err != nil && !apierrors.IsNotFound(err) {
		f.t.Fatal(err)
	}
}

// job creates a live workload that exits unsuccessfully with retries disabled to expose Job failure.
func (f *fixture) job() *batchv1.Job {
	f.t.Helper()

	c := container()
	c.Command = []string{"sh", "-c", "exit 1"}

	job, err := f.client.BatchV1().Jobs(f.namespace).Create(f.t.Context(), &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "failed-job"},
		Spec: batchv1.JobSpec{BackoffLimit: new(int32(0)), Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever, TerminationGracePeriodSeconds: new(int64(0)), Containers: []corev1.Container{c},
		}}},
	}, metav1.CreateOptions{})
	if err != nil {
		f.t.Fatal(err)
	}

	return job
}

// deleteWorkloads deletes the fixture deployment and Job with UID preconditions and foreground
// propagation, then waits for all Pods in the owned namespace to disappear.
func (f *fixture) deleteWorkloads(deploymentUID, jobUID types.UID) {
	f.t.Helper()

	policy := metav1.DeletePropagationForeground

	opts := metav1.DeleteOptions{PropagationPolicy: &policy, GracePeriodSeconds: new(int64(0)), Preconditions: &metav1.Preconditions{UID: &deploymentUID}}
	if err := f.client.AppsV1().Deployments(f.namespace).Delete(f.t.Context(), "rollout", opts); err != nil {
		f.t.Fatal(err)
	}

	opts.Preconditions.UID = &jobUID
	if err := f.client.BatchV1().Jobs(f.namespace).Delete(f.t.Context(), "failed-job", opts); err != nil {
		f.t.Fatal(err)
	}

	f.noPods()
}

// noPods waits for the owned live namespace to contain no Pods, failing if cleanup stalls.
func (f *fixture) noPods() {
	f.t.Helper()
	poll(f.t, 30*time.Second, "fixture pod cleanup", func() (bool, error) {
		list, err := f.client.CoreV1().Pods(f.namespace).List(f.t.Context(), metav1.ListOptions{})
		return err == nil && len(list.Items) == 0, err
	})
}
