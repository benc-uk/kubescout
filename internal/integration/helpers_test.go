//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"k8s-ops-agent/internal/report"
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

func newFixture(t *testing.T, model bool) *fixture {
	t.Helper()
	return newFixtureStage(t, model, "stage2")
}

func newFixtureStage(t *testing.T, model bool, stage string) *fixture {
	t.Helper()
	if os.Getenv("K8S_HEALTH_LIVE_WRITES") != "yes" {
		t.Skip("requires K8S_HEALTH_LIVE_WRITES=yes: creates and deletes an isolated namespace")
	}
	target := os.Getenv("K8S_HEALTH_LIVE_CONTEXT")
	if target == "" {
		t.Fatal("K8S_HEALTH_LIVE_CONTEXT must explicitly select a kubeconfig context")
	}
	if model && os.Getenv("K8S_HEALTH_LIVE_MODEL") != "yes" {
		t.Skip("requires K8S_HEALTH_LIVE_MODEL=yes: makes two real model requests")
	}
	binary, err := filepath.Abs(filepath.Join("..", "..", "build", "bin", "k8s-health"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatal("run make build first")
	}
	rc, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{CurrentContext: target},
	).ClientConfig()
	if err != nil {
		t.Fatal("cannot load the explicitly selected kubeconfig context")
	}
	rc.Timeout, rc.QPS, rc.Burst = 5*time.Second, 30, 50
	client, err := kubernetes.NewForConfig(rc)
	if err != nil {
		t.Fatal("cannot construct fixture client")
	}
	owner := "k8s-health-" + stage
	ns, err := client.CoreV1().Namespaces().Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		GenerateName: owner + "-", Labels: map[string]string{"app.kubernetes.io/managed-by": owner},
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cannot create test namespace: %v", err)
	}
	f := &fixture{t: t, client: client, config: rc, context: target, namespace: ns.Name, uid: ns.UID, binary: binary, owner: owner}
	t.Logf("fixture context=%s namespace=%s uid=%s", target, ns.Name, ns.UID)
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
		f.t.Errorf("cannot verify namespace before cleanup: %v", err)
		return
	}
	if ns.UID != f.uid || ns.Labels["app.kubernetes.io/managed-by"] != f.owner {
		f.t.Errorf("refusing cleanup: namespace ownership changed for %s", f.namespace)
		return
	}
	if err := f.client.CoreV1().Namespaces().Delete(ctx, f.namespace, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &f.uid}}); err != nil {
		f.t.Errorf("namespace cleanup failed: %v", err)
		return
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := f.client.CoreV1().Namespaces().Get(ctx, f.namespace, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			f.t.Logf("cleanup verified: namespace %s removed", f.namespace)
			return
		}
		if err != nil && ctx.Err() == nil {
			f.t.Errorf("cleanup verification failed: %v", err)
			return
		}
		select {
		case <-ctx.Done():
			f.t.Errorf("namespace %s still exists after cleanup deadline", f.namespace)
			return
		case <-ticker.C:
		}
	}
}

func (f *fixture) args(extra ...string) []string {
	return append([]string{"--auth-mode=kubeconfig", "--context=" + f.context, "--namespace=" + f.namespace,
		"--request-timeout=5s", "--run-timeout=30s"}, extra...)
}

func (f *fixture) once(extra []string, env map[string]string) (report.Report, int) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.t.Context(), 100*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, f.binary, f.args(append([]string{"--once"}, extra...)...)...)
	cmd.Env = environment(env)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	code := 0
	if err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			f.t.Fatalf("observer could not run: %v", err)
		}
		code = exit.ExitCode()
	}
	var result report.Report
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		f.t.Fatalf("observer did not emit exactly one JSON report (exit=%d): %s", code, stderr.String())
	}
	return result, code
}

func environment(overrides map[string]string) []string {
	values := os.Environ()
	for key, value := range overrides {
		prefix := key + "="
		filtered := values[:0]
		for _, existing := range values {
			if !strings.HasPrefix(existing, prefix) {
				filtered = append(filtered, existing)
			}
		}
		values = append(filtered, prefix+value)
	}
	return values
}

type lockedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}
func (b *lockedBuffer) text() string { b.mu.Lock(); defer b.mu.Unlock(); return b.Buffer.String() }

type observer struct {
	t          *testing.T
	cmd        *exec.Cmd
	reports    chan report.Report
	readErrors chan error
	done       chan struct{}
	stderr     lockedBuffer
	waitErr    error
	readerDone chan struct{}
	stopReader context.CancelFunc
	stopOnce   sync.Once
	peakRSS    int64
}

func (f *fixture) watch(extra ...string) *observer {
	f.t.Helper()
	args := f.args(append([]string{"--deterministic-only"}, extra...)...)
	o := &observer{t: f.t, reports: make(chan report.Report, 256), readErrors: make(chan error, 1), done: make(chan struct{}), readerDone: make(chan struct{})}
	o.cmd = exec.Command(f.binary, args...)
	o.cmd.Stderr = &o.stderr
	pipe, err := o.cmd.StdoutPipe()
	if err != nil {
		f.t.Fatal(err)
	}
	if err := o.cmd.Start(); err != nil {
		f.t.Fatal(err)
	}
	readCtx, cancel := context.WithCancel(context.Background())
	o.stopReader = cancel
	go func() {
		defer close(o.readerDone)
		scanner := bufio.NewScanner(pipe)
		scanner.Buffer(make([]byte, 4096), 4<<20)
		for scanner.Scan() {
			var r report.Report
			if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
				o.readErrors <- errors.New("observer emitted invalid JSON")
				return
			}
			select {
			case o.reports <- r:
			case <-readCtx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case o.readErrors <- err:
			default:
			}
		}
	}()
	go func() { o.waitErr = o.cmd.Wait(); close(o.done) }()
	f.t.Cleanup(o.stop)
	return o
}

func (o *observer) await(limit time.Duration, matches func(report.Report) bool) report.Report {
	o.t.Helper()
	timer := time.NewTimer(limit)
	defer timer.Stop()
	var last report.Report
	for {
		select {
		case r := <-o.reports:
			last = r
			if r.SchemaVersion != "2.0" {
				o.t.Fatal("unexpected report schema")
			}
			if matches(r) {
				return r
			}
		case err := <-o.readErrors:
			o.t.Fatalf("report stream error: %v", err)
		case <-o.done:
			o.t.Fatalf("observer exited unexpectedly: %v; %s", o.waitErr, o.stderr.text())
		case <-timer.C:
			details := []string{}
			for _, item := range last.Findings {
				if len(details) == 10 {
					break
				}
				details = append(details, item.Rule+"/"+item.Severity+"/"+item.Resource.Name)
			}
			o.t.Fatalf("report not received within %s; last health=%s complete=%t findings=%v errors=%v; %s", limit, last.Health, last.CollectionComplete, details, last.Errors, o.stderr.text())
		}
	}
}

func (o *observer) stop() {
	o.t.Helper()
	o.stopOnce.Do(func() {
		start := time.Now()
		o.sampleRSS()
		select {
		case <-o.done:
		default:
			if err := o.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
				o.t.Errorf("SIGTERM failed: %v", err)
			}
			select {
			case <-o.done:
			case <-time.After(shutdownLimit):
				o.t.Error("observer exceeded 5s shutdown limit")
				if err := o.cmd.Process.Kill(); err != nil {
					o.t.Errorf("failed to terminate test observer: %v", err)
				}
				<-o.done
			}
		}
		o.stopReader()
		<-o.readerDone
		if o.waitErr != nil {
			o.t.Errorf("observer exit failed: %v; %s", o.waitErr, o.stderr.text())
		}
		if runtime.GOOS == "linux" && o.cmd.ProcessState != nil {
			if usage, ok := o.cmd.ProcessState.SysUsage().(*syscall.Rusage); ok {
				o.peakRSS = max(o.peakRSS, usage.Maxrss)
			}
		}
		o.t.Logf("observer shutdown=%s peak_rss_kib=%d", time.Since(start).Round(time.Millisecond), o.peakRSS)
		if o.peakRSS >= rssLimitKiB {
			o.t.Errorf("observer peak RSS %d KiB must be below %d KiB", o.peakRSS, rssLimitKiB)
		}
	})
}

func (o *observer) sampleRSS() int64 {
	o.t.Helper()
	if runtime.GOOS != "linux" {
		o.t.Fatal("RSS acceptance measurement currently requires Linux /proc")
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", o.cmd.Process.Pid))
	if err != nil {
		select {
		case <-o.done:
			return o.peakRSS
		default:
			o.t.Fatalf("cannot measure observer RSS: %v", err)
		}
	}
	var rss int64
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if fields[0] == "VmRSS:" || fields[0] == "VmHWM:" {
			value, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				o.t.Fatal("invalid RSS measurement")
			}
			if fields[0] == "VmRSS:" {
				rss = value
			}
			o.peakRSS = max(o.peakRSS, value)
		}
	}
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
			t.Fatalf("%s timed out after %s", description, limit)
		case <-ticker.C:
		}
	}
}

func finding(r report.Report, rule string, uid types.UID) bool {
	if r.Type != "assessment" || !r.CollectionComplete {
		return false
	}
	for _, f := range r.Findings {
		if f.Rule == rule && f.Resource.UID == string(uid) {
			return true
		}
	}
	return false
}

func metric(t *testing.T, name string, since time.Time, r report.Report, limit time.Duration) {
	t.Helper()
	elapsed := max(0, r.EmittedAt.Sub(since))
	if elapsed > limit {
		t.Fatalf("%s latency %s exceeds %s", name, elapsed, limit)
	}
	t.Logf("METRIC %s=%s limit=%s", name, elapsed.Round(time.Millisecond), limit)
}
