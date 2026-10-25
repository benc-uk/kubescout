package runner

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s-ops-agent/internal/config"
	"k8s-ops-agent/internal/health"
	"k8s-ops-agent/internal/report"
)

type fakeSource struct {
	mu      sync.Mutex
	objects []health.Object
	err     error
	events  chan struct{}
}

func (s *fakeSource) Run(ctx context.Context)  { <-ctx.Done() }
func (s *fakeSource) Changes() <-chan struct{} { return s.events }
func (s *fakeSource) Err() error               { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *fakeSource) Snapshot(now time.Time) (health.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return health.Snapshot{At: now, Objects: append([]health.Object{}, s.objects...)}, s.err
}
func (s *fakeSource) Collect(_ context.Context, now time.Time) (health.Snapshot, error) {
	return s.Snapshot(now)
}
func (s *fakeSource) set(objects []health.Object, err error) {
	s.mu.Lock()
	s.objects, s.err = objects, err
	s.mu.Unlock()
	select {
	case s.events <- struct{}{}:
	default:
	}
}

type fakeAnalyzer struct {
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
	fail    bool
}

func (a *fakeAnalyzer) Analyze(ctx context.Context, _ []health.Finding) report.Analysis {
	a.calls.Add(1)
	if a.started != nil {
		a.started <- struct{}{}
	}
	if a.release != nil {
		select {
		case <-a.release:
		case <-ctx.Done():
			return report.AnalysisState("failed", "cancelled")
		}
	}
	if a.fail {
		return report.AnalysisState("failed", "invalid model output")
	}
	return report.AnalysisState("completed", "")
}

type channelSink struct{ reports chan report.Report }

func (s channelSink) Write(r report.Report) error { s.reports <- r; return nil }

func options() config.Config {
	return config.Config{Namespaces: []string{"apps"}, Deterministic: true, BatchWindow: 5 * time.Millisecond, CheckInterval: 10 * time.Millisecond, CallsPerHour: 60, RunTimeout: time.Second, Policy: health.DefaultPolicy()}
}
func pending() health.Object {
	return health.Object{Resource: health.Resource{Kind: "Pod", Name: "pod", Namespace: "apps", UID: "id"}, Created: time.Now(), Phase: "Pending"}
}
func failedPod() health.Object { p := pending(); p.Phase = "Failed"; return p }
func waitReport(t *testing.T, ch <-chan report.Report, matches func(report.Report) bool) report.Report {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case r := <-ch:
			if matches(r) {
				return r
			}
		case <-timer.C:
			t.Fatal("report not received")
			return report.Report{}
		}
	}
}
func start(t *testing.T, c config.Config, source *fakeSource, analyzer *fakeAnalyzer) (<-chan report.Report, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	ch := make(chan report.Report, 100)
	done := make(chan int, 1)
	r := Runner{Config: c, Cluster: "test", Source: source, Analyzer: analyzer, Sink: channelSink{ch}}
	go func() { done <- r.Watch(ctx) }()
	stop := func() {
		cancel()
		select {
		case code := <-done:
			if code != 0 {
				t.Errorf("exit %d", code)
			}
		case <-time.After(time.Second):
			t.Fatal("runner did not stop")
		}
	}
	return ch, stop
}

func TestTimedRulesWithoutEventsAndNoRepeatedReports(t *testing.T) {
	c := options()
	c.Policy.StartupGrace = 40 * time.Millisecond
	source := &fakeSource{events: make(chan struct{}, 1), objects: []health.Object{pending()}}
	a := &fakeAnalyzer{}
	ch, stop := start(t, c, source, a)
	defer stop()
	waitReport(t, ch, func(r report.Report) bool { return r.Health == "healthy" })
	waitReport(t, ch, func(r report.Report) bool { return r.Health == "unhealthy" })
	select {
	case r := <-ch:
		t.Fatalf("duplicate report %+v", r)
	case <-time.After(60 * time.Millisecond):
	}
	if a.calls.Load() != 0 {
		t.Fatal("deterministic mode called the model")
	}
}

func TestFailuresRetainFindingsWithoutFalseResolution(t *testing.T) {
	source := &fakeSource{events: make(chan struct{}, 1), objects: []health.Object{failedPod()}}
	ch, stop := start(t, options(), source, &fakeAnalyzer{})
	defer stop()
	waitReport(t, ch, func(r report.Report) bool { return r.Health == "unhealthy" })
	source.set(nil, errors.New("watch disconnected"))
	out := waitReport(t, ch, func(r report.Report) bool { return r.Health == "unknown" })
	if out.CollectionComplete || len(out.Findings) != 1 || len(out.Changes.Resolved) != 0 {
		t.Fatal("stale cache falsely resolved findings")
	}
	source.set(nil, nil)
	out = waitReport(t, ch, func(r report.Report) bool { return r.Health == "healthy" })
	if len(out.Changes.Resolved) != 1 {
		t.Fatal("fresh recovery did not resolve finding")
	}
}

func TestModelCannotBlockDetectionAndStaleResultsDiscarded(t *testing.T) {
	c := options()
	c.Deterministic = false
	source := &fakeSource{events: make(chan struct{}, 1), objects: []health.Object{failedPod()}}
	a := &fakeAnalyzer{started: make(chan struct{}, 1), release: make(chan struct{})}
	ch, stop := start(t, c, source, a)
	defer stop()
	select {
	case <-a.started:
	case <-time.After(time.Second):
		t.Fatal("analysis did not start")
	}
	waitReport(t, ch, func(r report.Report) bool { return r.Health == "unhealthy" })
	source.set(nil, nil)
	waitReport(t, ch, func(r report.Report) bool { return r.Health == "healthy" })
	close(a.release)
	out := waitReport(t, ch, func(r report.Report) bool { return r.Type == "analysis" })
	if out.Analysis.Status != "superseded" || len(out.Analysis.Items) != 0 {
		t.Fatal("stale advice published")
	}
	if a.calls.Load() != 1 {
		t.Fatal("unexpected model call")
	}
}

func TestDeferredAnalysisUsesLatestEvidence(t *testing.T) {
	c := options()
	c.Deterministic = false
	source := &fakeSource{events: make(chan struct{}, 1), objects: []health.Object{failedPod()}}
	a := &fakeAnalyzer{started: make(chan struct{}, 2), release: make(chan struct{})}
	ch, stop := start(t, c, source, a)
	defer stop()
	<-a.started
	p := failedPod()
	p.UID = "new-uid"
	source.set([]health.Object{p}, nil)
	waitReport(t, ch, func(r report.Report) bool { return r.Analysis.Status == "deferred" })
	a.release <- struct{}{}
	waitReport(t, ch, func(r report.Report) bool { return r.Analysis.Status == "superseded" })
	select {
	case <-a.started:
	case <-time.After(time.Second):
		t.Fatal("latest deferred analysis never started")
	}
	a.release <- struct{}{}
	waitReport(t, ch, func(r report.Report) bool { return r.Type == "analysis" && r.Analysis.Status == "completed" })
	if a.calls.Load() != 2 {
		t.Fatal("unexpected call count")
	}
}

func TestModelFailuresDoNotRetryUnchangedFindings(t *testing.T) {
	c := options()
	c.Deterministic = false
	source := &fakeSource{events: make(chan struct{}, 1), objects: []health.Object{failedPod()}}
	a := &fakeAnalyzer{fail: true}
	ch, stop := start(t, c, source, a)
	defer stop()
	out := waitReport(t, ch, func(r report.Report) bool { return r.Type == "analysis" })
	if out.Health != "unhealthy" || out.Execution != "incomplete" || out.Analysis.Status != "failed" {
		t.Fatal(out)
	}
	time.Sleep(40 * time.Millisecond)
	if a.calls.Load() != 1 {
		t.Fatal("failed model was repeatedly called for unchanged findings")
	}
}

func TestHourlyCapAndExpiryBoundary(t *testing.T) {
	now := time.Now()
	s := state{calls: []time.Time{now.Add(-time.Hour + time.Second)}}
	if s.allow(now, 1) {
		t.Fatal("hourly cap ignored")
	}
	if !s.allow(now.Add(time.Second), 1) {
		t.Fatal("cap did not expire at the boundary")
	}
	c := options()
	c.Deterministic, c.CallsPerHour = false, 1
	source := &fakeSource{events: make(chan struct{}, 1), objects: []health.Object{failedPod()}}
	a := &fakeAnalyzer{}
	ch, stop := start(t, c, source, a)
	defer stop()
	waitReport(t, ch, func(r report.Report) bool { return r.Type == "analysis" })
	p := failedPod()
	p.UID = "another"
	source.set([]health.Object{p}, nil)
	out := waitReport(t, ch, func(r report.Report) bool { return r.Analysis.Status == "rate_limited" })
	if out.Health != "unhealthy" || a.calls.Load() != 1 {
		t.Fatal("rate limit blocked health or allowed a model call")
	}
}

func TestContinuousEventsCannotStarveBatch(t *testing.T) {
	c := options()
	c.CheckInterval = time.Hour
	c.BatchWindow = 15 * time.Millisecond
	source := &fakeSource{events: make(chan struct{}, 1)}
	ch, stop := start(t, c, source, &fakeAnalyzer{})
	defer stop()
	waitReport(t, ch, func(r report.Report) bool { return r.Health == "healthy" })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			source.set([]health.Object{failedPod()}, nil)
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	started := time.Now()
	waitReport(t, ch, func(r report.Report) bool { return r.Health == "unhealthy" })
	cancel()
	<-done
	if time.Since(started) > 250*time.Millisecond {
		t.Fatal("continuous events starved the batching deadline")
	}
}

func TestOnceContracts(t *testing.T) {
	for _, test := range []struct {
		name                     string
		objects                  []health.Object
		collectError             error
		deterministic, modelFail bool
		code                     int
		health                   string
		calls                    int32
	}{
		{"healthy skip", nil, nil, false, false, 0, "healthy", 0},
		{"unhealthy success", []health.Object{failedPod()}, nil, false, false, 0, "unhealthy", 1},
		{"unhealthy deterministic", []health.Object{failedPod()}, nil, true, false, 0, "unhealthy", 0},
		{"model failed", []health.Object{failedPod()}, nil, false, true, 1, "unhealthy", 1},
		{"collection failed", nil, errors.New("forbidden"), false, false, 1, "unknown", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := options()
			c.Deterministic = test.deterministic
			a := &fakeAnalyzer{fail: test.modelFail}
			ch := make(chan report.Report, 2)
			r := Runner{Config: c, Source: &fakeSource{objects: test.objects, err: test.collectError}, Analyzer: a, Sink: channelSink{ch}}
			if code := r.Once(t.Context()); code != test.code {
				t.Fatalf("code %d", code)
			}
			out := <-ch
			if out.Health != test.health || a.calls.Load() != test.calls || len(ch) != 0 {
				t.Fatalf("invalid once contract: %+v", out)
			}
		})
	}
}
