//go:build integration

/*
Purpose: Exercise live collector failures, recovery, freshness, load and object limits.
Flow: Opt-in integration tests use an explicit context and owned workload fixtures
with ownership-checked namespace cleanup; collector checks make no model calls.
A separately selected model test runs bounded collection and AI with a fixture
incident repository; readable completion and stored fixtures, not stdout JSON,
verify the result. It accepts no-action and never writes live Incidents.
*/
package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/analysis"
	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/kube"
	"github.com/benc-uk/kubescout/internal/output"
	"github.com/benc-uk/kubescout/internal/runner"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// TestLiveKubernetes exercises failure, recovery, timed rules and load using owned workloads
// in an explicitly selected live cluster; it checks collector evidence, not AI incidents.
func TestLiveKubernetes(t *testing.T) {
	f := newFixture(t)

	baseline, code := f.once(nil, nil)
	if code != 0 || !baseline.CollectionComplete || baseline.Health != "healthy" {
		t.Fatalf("baseline did not complete healthy: exit=%d health=%s errors=%v", code, baseline.Health, baseline.Errors)
	}

	t.Log("live collector API baseline completed; no alternate deterministic runtime or model call")

	main := f.watch("--rollout-grace=5s")
	main.await(15*time.Second, func(r observation) bool { return r.CollectionComplete && r.Health == "healthy" })
	t.Logf("METRIC baseline_rss_kib=%d", main.sampleRSS())

	d := f.deployment()
	f.available(d.Name)

	badImage := "busybox:kubescout-stage2-does-not-exist"
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
	out := main.await(detectionLimit, func(r observation) bool { return finding(r, "pod.container.waiting", failing.UID) })
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

	deploymentFailed := func(r observation) bool {
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
	out = main.await(detectionLimit, func(r observation) bool {
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
	out = main.await(detectionLimit, func(r observation) bool { return finding(r, "job.failed", job.UID) })
	metric(t, "job_failure_after_api_confirmation", observed, out, detectionLimit)
	f.deleteWorkloads(d.UID, job.UID)
	main.stop()

	timedRules(t, f)
	burst(t, f)
}

// TestLiveConnectivity checks collector freshness and recovery through fixture-local API faults
// in an explicitly selected live cluster without model calls or cluster-wide network changes.
func TestLiveConnectivity(t *testing.T) {
	f := newFixture(t)
	reconnect(t, f)
}

// timedRules creates a gated live Pod and verifies grace-based evidence appears without a
// resource-version change, then stops collection and removes the workload.
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
	out := observer.await(15*time.Second, func(r observation) bool {
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

// burst creates two 50-Pod live bursts to check evidence latency, shared object limits and
// shutdown under load, logging test-process memory and cleaning up the owned Pods.
func burst(t *testing.T, f *fixture) {
	observer := f.watch("--startup-grace=0s")
	observer.await(15*time.Second, func(r observation) bool { return r.CollectionComplete && r.Health == "healthy" })
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
		out := observer.await(detectionLimit, func(r observation) bool {
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
			limited, code := f.once([]string{"--max-objects=10"}, nil)
			if code != 1 || limited.Health != "unknown" || limited.CollectionComplete || !strings.Contains(strings.Join(limited.Errors, " "), "budget") {
				t.Fatalf("shared budget not enforced: exit=%d health=%s errors=%v", code, limited.Health, limited.Errors)
			}

			bounded := f.watch("--max-objects=10")
			bounded.await(10*time.Second, func(r observation) bool {
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

		out = observer.await(detectionLimit, func(r observation) bool {
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

// reconnect injects fixture-local disconnects, silent streams and watch renewal to verify unknown
// observations during outages and fresh evidence after recovery, without calling a model.
func reconnect(t *testing.T, f *fixture) {
	p := f.proxy(proxyPass)
	observer := f.watch("--kubeconfig="+p.path, "--context=stage2-proxy")
	observer.await(15*time.Second, func(r observation) bool { return r.CollectionComplete && r.Health == "healthy" })

	start := time.Now()

	p.disconnect()

	out := observer.await(detectionLimit, func(r observation) bool { return !r.CollectionComplete && r.Health == "unknown" })
	metric(t, "disconnect_to_unknown", start, out, detectionLimit)

	if len(out.Changes.Resolved) != 0 {
		t.Fatal("connection failure resolved findings")
	}

	start = time.Now()

	p.mode.Store(proxyPass)

	out = observer.await(recoveryLimit, func(r observation) bool { return r.CollectionComplete && r.Health == "healthy" })
	metric(t, "reconnect_to_complete", start, out, recoveryLimit)
	t.Logf("proxy lists=%d watches=%d", p.lists.Load(), p.watches.Load())
	observer.stop()

	silent := f.proxy(proxySilent)
	observer = f.watch("--kubeconfig="+silent.path, "--context=stage2-proxy", "--watch-timeout=6s", "--request-timeout=2s", "--startup-grace=0s")
	observer.await(10*time.Second, func(r observation) bool { return r.CollectionComplete && r.Health == "healthy" })

	pod := f.gatedPod("silent-change")
	start = time.Now()
	out = observer.await(10*time.Second, func(r observation) bool {
		return !r.CollectionComplete && r.Health == "unknown" && strings.Contains(strings.Join(r.Errors, " "), "watch")
	})
	metric(t, "silent_stream_change_to_unknown", start, out, 10*time.Second)
	silent.mode.Store(proxyPass)

	start = time.Now()
	out = observer.await(recoveryLimit, func(r observation) bool {
		return finding(r, "pod.unschedulable", pod.UID) || finding(r, "pod.pending", pod.UID)
	})
	metric(t, "silent_stream_recovery", start, out, recoveryLimit)
	observer.stop()
	f.deletePod(pod)
	f.noPods()

	renew := f.proxy(proxyPass)
	observer = f.watch("--kubeconfig="+renew.path, "--context=stage2-proxy", "--watch-timeout=8s", "--request-timeout=2s")
	observer.await(10*time.Second, func(r observation) bool { return r.CollectionComplete && r.Health == "healthy" })
	out = observer.await(12*time.Second, func(r observation) bool {
		return r.Health == "unknown" && strings.Contains(strings.Join(r.Errors, " "), "watch")
	})
	start = out.EmittedAt
	out = observer.await(recoveryLimit, func(r observation) bool { return r.CollectionComplete && r.Health == "healthy" })
	metric(t, "renewal_unknown_window", start, out, recoveryLimit)
	t.Logf("renewal lists=%d watches=%d", renew.lists.Load(), renew.watches.Load())
	observer.stop()
}

// TestLiveModel runs bounded live collection and AI investigation with required model configuration.
// A test-only memory repository verifies accepted changes without writing live Incidents.
func TestLiveModel(t *testing.T) {
	f := newFixture(t)
	p := f.gatedPod("model-pending")

	poll(t, 10*time.Second, "model fixture Pending state", func() (bool, error) {
		current, err := f.client.CoreV1().Pods(f.namespace).Get(t.Context(), p.Name, metav1.GetOptions{})
		return err == nil && current.Status.Phase == corev1.PodPending, err
	})

	if err := config.LoadDotEnv(filepath.Join("..", "..")); err != nil {
		t.Fatal("invalid model configuration")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Second)
	defer cancel()

	c, err := config.Parse(f.args("--once", "--startup-grace=0s", "--model-calls-per-hour=6", "--request-timeout=30s", "--investigation-timeout=120s", "--run-timeout=120s"), os.Getenv, io.Discard)
	if err != nil {
		t.Fatal("model configuration missing or invalid")
	}

	client, err := dynamic.NewForConfig(f.config)
	if err != nil {
		t.Fatal("cannot create the live fixture client")
	}

	var out, diag bytes.Buffer

	repo := &incident.Memory{}

	r := runner.Runner{Config: c, Source: kube.NewSource(client, c), Analyzer: &analysis.Analyzer{Config: c, Kubernetes: client},
		Incidents: incident.Reconciler{Repository: repo, Observer: c.ObserverID, Model: c.Model}, Output: &output.Writer{Out: &out, Err: &diag}}
	if r.Once(ctx) != 0 {
		t.Fatalf("bounded live AI run failed: %s", &diag)
	}

	if !strings.Contains(out.String(), "Assessment completed") {
		t.Fatal("missing readable completion")
	}

	for _, record := range repo.Records {
		if record.Value.Identity.Observer != c.ObserverID || record.Value.Analysis.Model != c.Model ||
			record.Value.Analysis.PromptVersion != incident.PromptVersion || record.Value.Analysis.Title == "" {
			t.Fatal("fixture incident lacks validated AI provenance")
		}
	}

	t.Logf("validated AI result with %d stored fixture incidents; no-action is valid; at most six real model HTTP requests; no live Incident writes", len(repo.Records))
}
