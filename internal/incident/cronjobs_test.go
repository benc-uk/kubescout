/*
Purpose: Test CronJob ownership, legacy identity and scheduling freshness.
Flow: Offline CronJob/Job/Pod snapshots coalesce scheduled runs under their
verified controller, preserve unresolved Job anchors and retain healthy
recovery candidates. These tests never publish incidents or call Kubernetes/AI.
*/
package incident

import (
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
)

// cronJobSnapshot supplies a CronJob with a failed owned Job and Pod for correlation fixtures.
func cronJobSnapshot() health.Snapshot {
	cron := health.Object{Resource: health.Resource{Kind: "CronJob", Namespace: "apps", Name: "nightly", UID: "cron"},
		CronJob: health.CronJobState{Schedule: "@daily"}}
	job := health.Object{Resource: health.Resource{Kind: "Job", Namespace: "apps", Name: "nightly-one", UID: "job"}, Owner: cron.Resource,
		Conditions: []health.Condition{{Type: "Failed", Status: "True", Reason: "BackoffLimitExceeded"}}}
	pod := health.Object{Resource: health.Resource{Kind: "Pod", Namespace: "apps", Name: "run", UID: "pod"}, Owner: job.Resource, Phase: "Failed"}

	return health.Snapshot{At: time.Now(), Objects: []health.Object{cron, job, pod}}
}

// TestCronJobCorrelationCoalescesRuns checks stable CronJob anchors across scheduled Job and Pod replacement.
func TestCronJobCorrelationCoalescesRuns(t *testing.T) {
	snapshot := cronJobSnapshot()
	key := Ref(snapshot.Objects[0].Resource)

	first, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 10)
	if err != nil || len(first) != 1 || first[key].Anchor != key || len(first[key].Signals) != 2 || len(first[key].Resources) != 3 {
		t.Fatalf("scheduled failures did not coalesce: %+v %v", first, err)
	}

	snapshot.Objects[1].Name, snapshot.Objects[1].UID = "nightly-two", "next-job"
	snapshot.Objects[2].Owner = snapshot.Objects[1].Resource
	snapshot.Objects[2].Name, snapshot.Objects[2].UID = "next-pod", "next-pod"

	next, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 10)
	if err != nil || len(next) != 1 || next[key].Anchor != first[key].Anchor {
		t.Fatal("scheduled run replacement changed CronJob identity")
	}

	snapshot.Objects[1].Owner.UID = "old-cron"

	isolated, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 10)
	if err != nil || len(isolated) != 1 {
		t.Fatal("unexpected stale-controller candidates")
	}

	for _, in := range isolated {
		if in.Anchor == key {
			t.Fatal("stale CronJob owner attached failures to a replacement controller")
		}
	}
}

// TestCronJobLegacyJobAnchorRetainsActiveEvidence prevents newly recognized ownership from faking legacy Job recovery.
func TestCronJobLegacyJobAnchorRetainsActiveEvidence(t *testing.T) {
	snapshot := cronJobSnapshot()
	job := snapshot.Objects[1]
	key := Ref(job.Resource)
	open := []Record{{Value: Incident{Identity: Identity{Anchor: job.Resource, Family: "job-failure"},
		Resources: []health.Resource{job.Resource}, Lifecycle: Lifecycle{State: "open"}}}}

	contexts, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), open, 10)
	if err != nil || len(contexts) != 1 || contexts[key].Anchor != key || !Active(contexts[key]) || len(contexts[key].Signals) != 2 {
		t.Fatalf("legacy Job evidence moved or falsely recovered: %+v %v", contexts, err)
	}

	if _, ok := contexts[key].Identities[Ref(snapshot.Objects[0].Resource)]; !ok {
		t.Fatal("legacy Job lost verified CronJob inspection context")
	}

	snapshot.Objects[1].Conditions = nil
	snapshot.Objects[2].Phase = "Succeeded"

	recovery, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), open, 10)
	if err != nil || len(recovery) != 1 || recovery[key].Anchor != key || Active(recovery[key]) {
		t.Fatal("legacy Job recovery candidate was stranded or retained stale failure")
	}
}

// TestCronJobFingerprintTracksSchedulingFacts invalidates stale reviews when approved controller state changes.
func TestCronJobFingerprintTracksSchedulingFacts(t *testing.T) {
	snapshot := cronJobSnapshot()
	key := Ref(snapshot.Objects[0].Resource)

	contexts, err := BuildInvestigationContexts(snapshot, health.DefaultPolicy(), nil, 10)
	if err != nil {
		t.Fatal(err)
	}

	in := contexts[key]
	baseline := Fingerprint(in)

	for _, edit := range []func(*health.Object){
		func(o *health.Object) { o.CronJob.Schedule = "@hourly" },
		func(o *health.Object) { o.CronJob.TimeZone = "Europe/London" },
		func(o *health.Object) { o.CronJob.Suspended = true },
		func(o *health.Object) { o.CronJob.ConcurrencyPolicy = "Forbid" },
		func(o *health.Object) { o.CronJob.StartingDeadline = new(int64(0)) },
		func(o *health.Object) { o.CronJob.LastSuccess = new(time.Now()) },
		func(o *health.Object) { o.CronJob.Active = 1 },
	} {
		o := snapshot.Objects[0]
		edit(&o)
		in.Objects[key] = o

		if Fingerprint(in) == baseline {
			t.Fatal("changed CronJob facts accepted as the same investigation revision")
		}
	}
}
