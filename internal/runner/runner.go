package runner

import (
	"context"
	"slices"
	"sync"
	"time"

	"k8s-ops-agent/internal/config"
	"k8s-ops-agent/internal/health"
	"k8s-ops-agent/internal/report"
	"k8s.io/client-go/util/workqueue"
)

type Analyzer interface {
	Analyze(context.Context, []health.Finding) report.Analysis
}
type Source interface {
	Run(context.Context)
	Changes() <-chan struct{}
	Err() error
	Snapshot(time.Time) (health.Snapshot, error)
	Collect(context.Context, time.Time) (health.Snapshot, error)
}
type Sink interface{ Write(report.Report) error }

type Runner struct {
	Config   config.Config
	Cluster  string
	Source   Source
	Analyzer Analyzer
	Sink     Sink
}

func (r *Runner) Once(ctx context.Context) int {
	ctx, cancel := context.WithTimeout(ctx, r.Config.RunTimeout)
	defer cancel()
	out := report.New(r.Cluster, r.Config.Namespaces, time.Now())
	snapshot, err := r.Source.Collect(ctx, time.Now())
	if err != nil {
		out.Execution, out.Errors = "incomplete", []string{err.Error()}
		out.Analysis = report.AnalysisState("skipped", "collection incomplete")
	} else {
		snapshot.At = time.Now()
		out.At = snapshot.At.UTC()
		out.CollectionComplete = true
		out.Findings = health.Evaluate(snapshot, r.Config.Policy)
		out.Health = health.Status(out.Findings)
		out.Changes = report.Diff(nil, out.Findings)
		if r.Config.Deterministic {
			out.Analysis = report.AnalysisState("skipped", "deterministic-only")
		} else if out.Health == "unhealthy" {
			out.Analysis = r.Analyzer.Analyze(ctx, out.Findings)
			if out.Analysis.Status != "completed" {
				out.Execution = "incomplete"
				out.Errors = append(out.Errors, out.Analysis.Reason)
			}
		}
	}
	if r.Sink.Write(out) != nil || out.Execution == "incomplete" {
		return 1
	}
	return 0
}

type completion struct {
	report   report.Report
	findings []health.Finding
	analysis report.Analysis
}

type state struct {
	last                        []health.Finding
	current                     report.Report
	complete, initialised, busy bool
	lastError                   string
	attempted                   map[string]string
	calls                       []time.Time
}

func (s *state) needed() []health.Finding {
	out := []health.Finding{}
	for _, f := range s.last {
		if f.Actionable() && s.attempted[f.ID] != health.Fingerprint(f) {
			out = append(out, f)
		}
	}
	return out
}

func (s *state) allow(now time.Time, limit int) bool {
	s.calls = slices.DeleteFunc(s.calls, func(t time.Time) bool { return !t.After(now.Add(-time.Hour)) })
	return len(s.calls) < limit
}

func (r *Runner) Watch(ctx context.Context) int {
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	queue := workqueue.NewTypedDelayingQueue[string]()
	defer func() { cancel(); queue.ShutDown(); workers.Wait() }()
	wake := make(chan struct{}, 1)
	results := make(chan completion, 1)
	workers.Go(func() { r.Source.Run(ctx) })
	workers.Go(func() {
		for {
			key, shutdown := queue.Get()
			if shutdown {
				return
			}
			select {
			case wake <- struct{}{}:
			case <-ctx.Done():
				queue.Done(key)
				return
			}
			queue.Done(key)
		}
	})
	ticker := time.NewTicker(r.Config.CheckInterval)
	defer ticker.Stop()
	s := state{attempted: map[string]string{}}
	queue.Add("evaluate")
	for {
		select {
		case <-ctx.Done():
			return 0
		case <-r.Source.Changes():
			// AddAfter retains the earliest deadline, unlike a trailing-edge
			// debounce that can starve forever during a busy rollout.
			if r.Source.Err() != nil {
				queue.Add("evaluate")
			} else {
				queue.AddAfter("evaluate", r.Config.BatchWindow)
			}
		case <-ticker.C:
			queue.Add("evaluate")
		case <-wake:
			if err := r.evaluate(&s); err != nil {
				return 1
			}
			if err := r.startAnalysis(ctx, &s, results, &workers); err != nil {
				return 1
			}
		case result := <-results:
			s.busy = false
			out := result.report
			out.ID, out.Type, out.Changes = report.ID(), "analysis", report.Changes{New: []string{}, Updated: []string{}, Resolved: []string{}}
			out.Analysis = result.analysis
			for _, f := range result.findings {
				out.Analysis.FindingIDs = append(out.Analysis.FindingIDs, f.ID)
			}
			// Reconcile before publishing, not merely against the last scheduled
			// evaluation, so an already-resolved problem cannot get fresh advice.
			if err := r.evaluate(&s); err != nil {
				return 1
			}
			if !s.complete || !stillCurrent(result.findings, s.last) {
				out.Analysis = report.AnalysisState("superseded", "evidence changed or collection is incomplete")
			}
			if result.analysis.Status == "failed" {
				out.Execution = "incomplete"
				out.Errors = append(out.Errors, result.analysis.Reason)
			}
			if err := r.Sink.Write(out); err != nil {
				return 1
			}
			if err := r.startAnalysis(ctx, &s, results, &workers); err != nil {
				return 1
			}
		}
	}
}

func stillCurrent(requested, current []health.Finding) bool {
	now := map[string]string{}
	for _, f := range current {
		now[f.ID] = health.Fingerprint(f)
	}
	for _, f := range requested {
		if now[f.ID] != health.Fingerprint(f) {
			return false
		}
	}
	return true
}

func (r *Runner) evaluate(s *state) error {
	now := time.Now()
	snapshot, err := r.Source.Snapshot(now)
	out := report.New(r.Cluster, r.Config.Namespaces, now)
	if err != nil {
		if s.initialised && !s.complete && s.lastError == err.Error() {
			return nil
		}
		s.initialised, s.complete, s.lastError = true, false, err.Error()
		out.Execution, out.Errors = "incomplete", []string{err.Error()}
		out.Findings = s.last
		if out.Findings == nil {
			out.Findings = []health.Finding{}
		}
		out.Analysis = report.AnalysisState("skipped", "collection incomplete; retained findings are last-known evidence")
		return r.Sink.Write(out)
	}
	findings := health.Evaluate(snapshot, r.Config.Policy)
	out.Changes = report.Diff(s.last, findings)
	changed := !s.initialised || !s.complete || !out.Changes.Empty()
	s.initialised, s.complete, s.lastError = true, true, ""
	s.last = findings
	live := map[string]bool{}
	for _, f := range findings {
		if f.Actionable() {
			live[f.ID] = true
		}
	}
	for id := range s.attempted {
		if !live[id] {
			delete(s.attempted, id)
		}
	}
	if !changed {
		return nil
	}
	out.CollectionComplete, out.Findings, out.Health = true, findings, health.Status(findings)
	switch {
	case r.Config.Deterministic:
		out.Analysis = report.AnalysisState("skipped", "deterministic-only")
	case len(s.needed()) == 0:
		out.Analysis = report.AnalysisState("skipped", "no new or changed actionable findings")
	case s.busy:
		out.Analysis = report.AnalysisState("deferred", "another analysis is running")
	case !s.allow(now, r.Config.CallsPerHour):
		out.Analysis = report.AnalysisState("rate_limited", "rolling hourly model call limit reached")
	default:
		out.Analysis = report.AnalysisState("pending", "analysis scheduled separately from detection")
	}
	s.current = out
	return r.Sink.Write(out)
}

func (r *Runner) startAnalysis(ctx context.Context, s *state, results chan<- completion, workers *sync.WaitGroup) error {
	if !s.complete || s.busy || r.Config.Deterministic {
		return nil
	}
	findings := s.needed()
	if len(findings) == 0 || !s.allow(time.Now(), r.Config.CallsPerHour) {
		return nil
	}
	out := s.current
	if out.Analysis.Status != "pending" {
		out.ID, out.Type = report.ID(), "analysis_started"
		out.Analysis = report.AnalysisState("pending", "deferred analysis is starting")
		out.Changes = report.Changes{New: []string{}, Updated: []string{}, Resolved: []string{}}
		if err := r.Sink.Write(out); err != nil {
			return err
		}
	}
	s.current.Analysis = report.AnalysisState("submitted", "")
	s.busy = true
	s.calls = append(s.calls, time.Now())
	for _, f := range findings {
		s.attempted[f.ID] = health.Fingerprint(f)
	}
	workers.Go(func() {
		analysis := r.Analyzer.Analyze(ctx, findings)
		select {
		case results <- completion{out, findings, analysis}:
		case <-ctx.Done():
		}
	})
	return nil
}
