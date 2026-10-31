//go:build integration

package cluster

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s-ops-agent/internal/config"
	"k8s-ops-agent/internal/health"
	"k8s-ops-agent/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	detectionLimit = 10 * time.Second
	recoveryLimit  = 15 * time.Second
	shutdownLimit  = 5 * time.Second
	rssLimitKiB    = 256 * 1024
	fixtureImage   = "busybox:1.37.0"
)

type fixture struct {
	t                                 *testing.T
	client                            *kubernetes.Clientset
	config                            *rest.Config
	context, namespace, binary, owner string
	uid                               types.UID
}

func newFixture(t *testing.T, model bool) *fixture { return newFixtureStage(t, model, "collector") }
func newFixtureStage(t *testing.T, model bool, stage string) *fixture {
	t.Helper()
	target := os.Getenv("K8S_HEALTH_LIVE_CONTEXT")
	if target == "" {
		t.Fatal("K8S_HEALTH_LIVE_CONTEXT must explicitly select a context")
	}
	if model && os.Getenv("K8S_HEALTH_LIVE_MODEL") != "yes" {
		t.Skip("requires explicit model request authorisation")
	}
	rc, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{CurrentContext: target}).ClientConfig()
	if err != nil {
		t.Fatal("cannot load explicitly selected kubeconfig")
	}
	rc.Timeout, rc.QPS, rc.Burst = 5*time.Second, 30, 50
	client, err := kubernetes.NewForConfig(rc)
	if err != nil {
		t.Fatal(err)
	}
	owner := "k8s-health-" + stage
	ns, err := client.CoreV1().Namespaces().Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: owner + "-", Labels: map[string]string{"app.kubernetes.io/managed-by": owner}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	binary, err := filepath.Abs(filepath.Join("..", "..", "build", "bin", "k8s-health"))
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, client: client, config: rc, context: target, namespace: ns.Name, uid: ns.UID, binary: binary, owner: owner}
	t.Logf("owned fixture context=%s namespace=%s uid=%s", target, ns.Name, ns.UID)
	t.Cleanup(f.cleanup)
	return f
}
func (f *fixture) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ns, err := f.client.CoreV1().Namespaces().Get(ctx, f.namespace, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return
	}
	if err != nil {
		f.t.Errorf("cannot verify cleanup ownership: %v", err)
		return
	}
	if ns.UID != f.uid || ns.Labels["app.kubernetes.io/managed-by"] != f.owner {
		f.t.Error("namespace ownership changed; refusing cleanup")
		return
	}
	if err := f.client.CoreV1().Namespaces().Delete(ctx, f.namespace, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &f.uid}}); err != nil {
		f.t.Error(err)
		return
	}
	for ctx.Err() == nil {
		_, err := f.client.CoreV1().Namespaces().Get(ctx, f.namespace, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return
		}
		if err != nil {
			f.t.Error(err)
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	f.t.Error("owned namespace cleanup timed out")
}
func (f *fixture) args(extra ...string) []string {
	return append([]string{"--auth-mode=kubeconfig", "--context=" + f.context, "--namespace=" + f.namespace, "--observer-id=" + f.namespace,
		"--request-timeout=5s", "--run-timeout=90s"}, extra...)
}

// observation is a test-only view of the collector and pure rules. It is not an
// executable mode, public output contract or incident publication mechanism.
type observation struct {
	CollectionComplete bool
	Health             string
	Findings           []health.Finding
	Errors             []string
	EmittedAt          time.Time
	At                 time.Time
	Changes            struct{ Resolved []string }
}

func (f *fixture) collector(extra []string) (*kube.Source, config.Config) {
	f.t.Helper()
	env := func(k string) string {
		return map[string]string{"AZURE_OPENAI_ENDPOINT": "https://fixture/openai/v1/", "AZURE_OPENAI_API_KEY": "unused", "AZURE_AI_MODEL_DEPLOYMENT_NAME": "unused"}[k]
	}
	cfg, err := config.Parse(f.args(extra...), env, io.Discard)
	if err != nil {
		f.t.Fatal(err)
	}
	client, _, err := kube.Client(cfg)
	if err != nil {
		f.t.Fatal(err)
	}
	return kube.NewSource(client, cfg), cfg
}
func observed(snapshot health.Snapshot, err error, policy health.Policy, before []health.Finding) observation {
	out := observation{Health: "unknown", EmittedAt: time.Now(), At: snapshot.At, Findings: before}
	if err != nil {
		out.Errors = []string{err.Error()}
		return out
	}
	out.CollectionComplete = true
	out.Findings = health.Evaluate(snapshot, policy)
	out.Health = health.Status(out.Findings)
	current := map[string]bool{}
	for _, f := range out.Findings {
		current[f.ID] = true
	}
	for _, f := range before {
		if !current[f.ID] {
			out.Changes.Resolved = append(out.Changes.Resolved, f.ID)
		}
	}
	return out
}
func (f *fixture) once(extra []string, _ map[string]string) (observation, int) {
	source, cfg := f.collector(extra)
	snapshot, err := source.Collect(f.t.Context(), time.Now())
	out := observed(snapshot, err, cfg.Policy, nil)
	if err != nil {
		return out, 1
	}
	return out, 0
}

type observer struct {
	t        *testing.T
	source   *kube.Source
	config   config.Config
	done     chan struct{}
	cancel   context.CancelFunc
	last     []health.Finding
	stopOnce sync.Once
	peakRSS  int64
}

func (f *fixture) watch(extra ...string) *observer {
	source, cfg := f.collector(extra)
	ctx, cancel := context.WithCancel(f.t.Context())
	o := &observer{t: f.t, source: source, config: cfg, done: make(chan struct{}), cancel: cancel}
	go func() { defer close(o.done); source.Run(ctx) }()
	f.t.Cleanup(o.stop)
	return o
}
func (o *observer) await(limit time.Duration, matches func(observation) bool) observation {
	o.t.Helper()
	ctx, cancel := context.WithTimeout(o.t.Context(), limit)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, err := o.source.Snapshot(time.Now())
		out := observed(snapshot, err, o.config.Policy, o.last)
		if out.CollectionComplete {
			o.last = out.Findings
		}
		if matches(out) {
			return out
		}
		select {
		case <-ctx.Done():
			o.t.Fatalf("collector evidence timed out: health=%s errors=%v", out.Health, out.Errors)
		case <-ticker.C:
		}
	}
}
func (o *observer) stop() {
	o.stopOnce.Do(func() {
		o.cancel()
		select {
		case <-o.done:
		case <-time.After(shutdownLimit):
			o.t.Error("collector shutdown exceeded limit")
		}
	})
}
func (o *observer) sampleRSS() int64 {
	o.t.Helper()
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		o.t.Fatal(err)
	}
	var rss int64
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 1 && fields[0] == "VmRSS:" {
			rss, err = strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				o.t.Fatal(err)
			}
		}
	}
	o.peakRSS = max(o.peakRSS, rss)
	o.t.Logf("collector test-process RSS=%d KiB (not standalone agent RSS)", rss)
	return rss
}
func poll(t *testing.T, limit time.Duration, description string, check func() (bool, error)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), limit)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := check()
		if err != nil {
			t.Fatalf("%s: %v", description, err)
		}
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s timed out", description)
		case <-ticker.C:
		}
	}
}
func finding(r observation, rule string, uid types.UID) bool {
	if !r.CollectionComplete {
		return false
	}
	for _, f := range r.Findings {
		if f.Rule == rule && f.Resource.UID == string(uid) {
			return true
		}
	}
	return false
}
func metric(t *testing.T, name string, since time.Time, r observation, limit time.Duration) {
	t.Helper()
	elapsed := max(0, r.EmittedAt.Sub(since))
	if elapsed > limit {
		t.Fatalf("%s exceeded bound", name)
	}
	t.Logf("collector evidence %s=%s", name, elapsed)
}

func requireLiveModel(t *testing.T) {
	t.Helper()
	if os.Getenv("K8S_HEALTH_LIVE_MODEL") != "yes" {
		t.Skip("requires explicit authorisation for up to six real model requests")
	}
}
