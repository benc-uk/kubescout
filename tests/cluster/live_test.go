//go:build integration

package cluster

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s-ops-agent/internal/report"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestLiveKubernetes(t *testing.T) {
	f := newFixture(t, false)
	baseline, code := f.once([]string{"--deterministic-only"}, nil)
	if code != 0 || !baseline.CollectionComplete || baseline.Health != "healthy" {
		t.Fatalf("baseline did not complete healthy: exit=%d health=%s errors=%v", code, baseline.Health, baseline.Errors)
	}
	t.Log("live --once baseline completed, model skipped")
	main := f.watch("--rollout-grace=5s")
	main.await(15*time.Second, func(r report.Report) bool { return r.CollectionComplete && r.Health == "healthy" })
	t.Logf("METRIC baseline_rss_kib=%d", main.sampleRSS())
	d := f.deployment()
	f.available(d.Name)
	badImage := "busybox:k8s-health-stage2-does-not-exist"
	f.image(d.Name, badImage)
	var failing corev1.Pod
	poll(t, 90*time.Second, "container image pull failure", func() (bool, error) {
		pods, err := f.client.CoreV1().Pods(f.namespace).List(t.Context(), metav1.ListOptions{LabelSelector: "app=health-stage2"})
		if err != nil {
			return false, err
		}
		for _, p := range pods.Items {
			for _, c := range p.Status.ContainerStatuses {
				if c.State.Waiting != nil && slices.Contains([]string{"ErrImagePull", "ImagePullBackOff"}, c.State.Waiting.Reason) {
					failing = p
					return true, nil
				}
			}
		}
		return false, nil
	})
	observed := time.Now()
	out := main.await(detectionLimit, func(r report.Report) bool { return finding(r, "pod.container.waiting", failing.UID) })
	metric(t, "image_failure_after_api_confirmation", observed, out, detectionLimit)
	imagePresent := false
	waitingID := ""
	for _, item := range out.Findings {
		if item.Rule == "pod.container.waiting" && item.Resource.UID == string(failing.UID) && slices.Contains(item.Evidence, "image="+badImage) {
			imagePresent = true
			waitingID = item.ID
		}
	}
	if !imagePresent {
		t.Fatal("configured image/tag missing from live failure evidence")
	}
	deploymentFailed := func(r report.Report) bool {
		for _, item := range r.Findings {
			if item.Rule == "deployment.replicas" && item.Resource.UID == string(d.UID) && item.Severity == "warning" {
				return r.CollectionComplete
			}
		}
		return false
	}
	if !deploymentFailed(out) {
		out = main.await(25*time.Second, deploymentFailed)
	}
	t.Log("deployment rollout deficit reported after accelerated 5s grace")
	restored := time.Now()
	f.image(d.Name, fixtureImage)
	f.available(d.Name)
	observed = time.Now()
	resolved := false
	out = main.await(detectionLimit, func(r report.Report) bool {
		resolved = resolved || slices.Contains(r.Changes.Resolved, waitingID)
		if !r.CollectionComplete || r.Health != "healthy" || r.At.Before(restored) {
			return false
		}
		for _, item := range r.Findings {
			if item.Resource.UID == string(failing.UID) && item.Actionable() {
				return false
			}
		}
		return resolved
	})
	metric(t, "image_recovery_after_api_confirmation", observed, out, detectionLimit)

	job := f.job()
	poll(t, 90*time.Second, "terminal Job failure", func() (bool, error) {
		j, err := f.client.BatchV1().Jobs(f.namespace).Get(t.Context(), job.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, c := range j.Status.Conditions {
			if (c.Type == batchv1.JobFailed || c.Type == batchv1.JobFailureTarget) && c.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	})
	observed = time.Now()
	out = main.await(detectionLimit, func(r report.Report) bool { return finding(r, "job.failed", job.UID) })
	metric(t, "job_failure_after_api_confirmation", observed, out, detectionLimit)
	f.deleteWorkloads(d.UID, job.UID)
	main.stop()

	timedRules(t, f)
	burst(t, f)
}

func TestLiveConnectivity(t *testing.T) {
	f := newFixture(t, false)
	reconnect(t, f)
}

func timedRules(t *testing.T, f *fixture) {
	p := f.gatedPod("timed-pending")
	poll(t, 10*time.Second, "gated Pending pod", func() (bool, error) {
		current, err := f.client.CoreV1().Pods(f.namespace).Get(t.Context(), p.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if current.Status.Phase == corev1.PodPending && len(current.Status.Conditions) > 0 {
			p = current
			return true, nil
		}
		return false, nil
	})
	resourceVersion := p.ResourceVersion
	observer := f.watch("--startup-grace=8s", "--check-interval=2s")
	out := observer.await(15*time.Second, func(r report.Report) bool {
		return finding(r, "pod.unschedulable", p.UID) || finding(r, "pod.pending", p.UID)
	})
	current, err := f.client.CoreV1().Pods(f.namespace).Get(t.Context(), p.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if current.ResourceVersion != resourceVersion {
		t.Fatal("timed-rule fixture changed while waiting; no-event assertion is not valid")
	}
	age := out.At.Sub(p.CreationTimestamp.Time)
	if age < 8*time.Second || age > 15*time.Second {
		t.Fatalf("timed rule age=%s, expected [8s,15s]", age)
	}
	t.Logf("METRIC timed_rule_age=%s resource_version_unchanged=true", age.Round(time.Millisecond))
	observer.stop()
	f.deletePod(p)
	f.noPods()
}

func burst(t *testing.T, f *fixture) {
	observer := f.watch("--startup-grace=0s")
	observer.await(15*time.Second, func(r report.Report) bool { return r.CollectionComplete && r.Health == "healthy" })
	initialRSS := observer.sampleRSS()
	for round := 0; round < 2; round++ {
		pods := make([]*corev1.Pod, 0, 50)
		uids := map[types.UID]bool{}
		for i := 0; i < 50; i++ {
			p := f.gatedPod(fmt.Sprintf("burst-%d-%02d", round, i))
			pods = append(pods, p)
			uids[p.UID] = true
		}
		created := time.Now()
		out := observer.await(detectionLimit, func(r report.Report) bool {
			if !r.CollectionComplete {
				return false
			}
			found := map[types.UID]bool{}
			for _, item := range r.Findings {
				if uids[types.UID(item.Resource.UID)] && (item.Rule == "pod.pending" || item.Rule == "pod.unschedulable") {
					found[types.UID(item.Resource.UID)] = true
				}
			}
			return len(found) == 50
		})
		metric(t, fmt.Sprintf("burst_%d_last_create_to_report", round+1), created, out, detectionLimit)
		t.Logf("METRIC burst_%d_rss_kib=%d baseline_rss_kib=%d", round+1, observer.sampleRSS(), initialRSS)
		if round == 0 {
			limited, code := f.once([]string{"--deterministic-only", "--max-objects=10"}, nil)
			if code != 1 || limited.Health != "unknown" || limited.CollectionComplete || !strings.Contains(strings.Join(limited.Errors, " "), "budget") {
				t.Fatalf("shared budget not enforced: exit=%d health=%s errors=%v", code, limited.Health, limited.Errors)
			}
			bounded := f.watch("--max-objects=10")
			bounded.await(10*time.Second, func(r report.Report) bool {
				return r.Health == "unknown" && strings.Contains(strings.Join(r.Errors, " "), "budget")
			})
			bounded.stop()
			t.Log("live object limit enforced in both once and watch modes")
		}
		if round == 1 {
			t.Log("verifying graceful shutdown with all 50 burst pods still present")
			observer.stop()
		}
		start := time.Now()
		for _, p := range pods {
			f.deletePod(p)
		}
		f.noPods()
		if round == 1 {
			t.Log("second burst fixtures removed after loaded-observer shutdown")
			continue
		}
		out = observer.await(detectionLimit, func(r report.Report) bool {
			if !r.CollectionComplete || r.Health != "healthy" {
				return false
			}
			for _, item := range r.Findings {
				if uids[types.UID(item.Resource.UID)] && item.Actionable() {
					return false
				}
			}
			return true
		})
		t.Logf("METRIC burst_%d_delete_to_recovery=%s", round+1, out.EmittedAt.Sub(start).Round(time.Millisecond))
	}
	observer.stop()
}

func reconnect(t *testing.T, f *fixture) {
	p := f.proxy(proxyPass)
	observer := f.watch("--kubeconfig="+p.path, "--context=stage2-proxy")
	observer.await(15*time.Second, func(r report.Report) bool { return r.CollectionComplete && r.Health == "healthy" })
	start := time.Now()
	p.disconnect()
	out := observer.await(detectionLimit, func(r report.Report) bool { return !r.CollectionComplete && r.Health == "unknown" })
	metric(t, "disconnect_to_unknown", start, out, detectionLimit)
	if len(out.Changes.Resolved) != 0 {
		t.Fatal("connection failure resolved findings")
	}
	start = time.Now()
	p.mode.Store(proxyPass)
	out = observer.await(recoveryLimit, func(r report.Report) bool { return r.CollectionComplete && r.Health == "healthy" })
	metric(t, "reconnect_to_complete", start, out, recoveryLimit)
	t.Logf("proxy lists=%d watches=%d", p.lists.Load(), p.watches.Load())
	observer.stop()

	silent := f.proxy(proxySilent)
	observer = f.watch("--kubeconfig="+silent.path, "--context=stage2-proxy", "--watch-timeout=6s", "--request-timeout=2s", "--startup-grace=0s")
	observer.await(10*time.Second, func(r report.Report) bool { return r.CollectionComplete && r.Health == "healthy" })
	pod := f.gatedPod("silent-change")
	start = time.Now()
	out = observer.await(10*time.Second, func(r report.Report) bool {
		return !r.CollectionComplete && r.Health == "unknown" && strings.Contains(strings.Join(r.Errors, " "), "watch")
	})
	metric(t, "silent_stream_change_to_unknown", start, out, 10*time.Second)
	silent.mode.Store(proxyPass)
	start = time.Now()
	out = observer.await(recoveryLimit, func(r report.Report) bool {
		return finding(r, "pod.unschedulable", pod.UID) || finding(r, "pod.pending", pod.UID)
	})
	metric(t, "silent_stream_recovery", start, out, recoveryLimit)
	observer.stop()
	f.deletePod(pod)
	f.noPods()

	renew := f.proxy(proxyPass)
	observer = f.watch("--kubeconfig="+renew.path, "--context=stage2-proxy", "--watch-timeout=8s", "--request-timeout=2s")
	observer.await(10*time.Second, func(r report.Report) bool { return r.CollectionComplete && r.Health == "healthy" })
	out = observer.await(12*time.Second, func(r report.Report) bool {
		return r.Health == "unknown" && strings.Contains(strings.Join(r.Errors, " "), "watch")
	})
	start = out.EmittedAt
	out = observer.await(recoveryLimit, func(r report.Report) bool { return r.CollectionComplete && r.Health == "healthy" })
	metric(t, "renewal_unknown_window", start, out, recoveryLimit)
	t.Logf("renewal lists=%d watches=%d", renew.lists.Load(), renew.watches.Load())
	observer.stop()
}

func TestLiveModel(t *testing.T) {
	f := newFixture(t, true)
	p := f.gatedPod("model-pending")
	poll(t, 10*time.Second, "model fixture Pending state", func() (bool, error) {
		current, err := f.client.CoreV1().Pods(f.namespace).Get(t.Context(), p.Name, metav1.GetOptions{})
		return err == nil && current.Status.Phase == corev1.PodPending, err
	})
	args := []string{"--startup-grace=0s", "--request-timeout=60s", "--run-timeout=90s", "--max-output-tokens=2000"}
	out, code := f.once(args, nil)
	if code != 0 || !out.CollectionComplete || out.Health != "unhealthy" || out.Analysis.Status != "completed" || len(out.Analysis.Items) == 0 {
		t.Fatalf("live model validation failed: exit=%d health=%s analysis=%s reason=%s errors=%v", code, out.Health, out.Analysis.Status, out.Analysis.Reason, out.Errors)
	}
	t.Logf("live model success: validated_items=%d findings=%d", len(out.Analysis.Items), len(out.Findings))
	out, code = f.once(args, map[string]string{"AZURE_AI_MODEL_DEPLOYMENT_NAME": "k8s-health-missing-" + f.namespace})
	if code != 1 || !out.CollectionComplete || out.Health != "unhealthy" || out.Analysis.Status != "failed" || len(out.Findings) == 0 {
		t.Fatalf("live model failure was not preserved correctly: exit=%d health=%s analysis=%s", code, out.Health, out.Analysis.Status)
	}
	t.Log("live model failure: invalid deployment retained unhealthy findings and returned exit 1")
	t.Log("real model request budget: two attempts, no watch-mode model calls")
}
