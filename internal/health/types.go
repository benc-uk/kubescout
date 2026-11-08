/*
Purpose: Define projected resource snapshots, evidence policies and findings.
Flow: Collection and pure checks share these internal types; identity hashes,
fingerprints, severity helpers and sorting support deterministic evidence
comparison. Storage facts exclude backend configuration; private claim links
support correlation, not ownership or authority to change incident lifecycle.
CronJob scheduling/status facts support interpretation of owned Job evidence.
HPA facts exclude metric selectors and expose only approved scaling context.
Ingress facts describe bounded routes without annotations, TLS Secrets or raw load-balancer addresses.
VolumeAttachment facts retain status/error metadata, never attachment metadata or inline volume specifications.
Host-only continuity and bounded change markers prevent batched transitions from faking stable recovery.
Configured extra resources retain API-group identity and selected status facts, never raw objects.
*/
package health

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"time"

	"github.com/benc-uk/kubescout/internal/resourcepolicy"
)

type Resource struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
	APIGroup  string `json:"api_group,omitempty"`
}

type Condition struct {
	Type, Status, Reason string
	Transition, Updated  time.Time
}

type Container struct {
	Name, Image, Waiting, Termination, PreviousReason string
	Init                                              bool
	Restarts                                          int32
	ExitCode, PreviousExit                            *int32
}

type Port struct {
	Name     string `json:"name"`
	Protocol string `json:"protocol"`
	Target   string `json:"target_port,omitempty"`
	Number   int32  `json:"port,omitempty"`
}

type Endpoint struct {
	Target                      Resource
	Ready, Serving, Terminating bool
}

type EndpointCounts struct {
	Total       int `json:"total"`
	Ready       int `json:"ready"`
	Serving     int `json:"serving"`
	Terminating int `json:"terminating"`
}

type Storage struct {
	Class         string
	Requested     string
	Capacity      string
	Mode          string
	AccessModes   []string
	ReclaimPolicy string
	VolumeName    string   `json:"-"`
	Claim         Resource `json:"-"`
}

type CronJobState struct {
	Schedule, TimeZone, ConcurrencyPolicy string
	Suspended                             bool
	StartingDeadline                      *int64
	SuccessfulHistory, FailedHistory      int32
	Active                                int
	LastSchedule, LastSuccess             *time.Time
}

type Metric struct {
	Type, Name, Container                 string
	ObjectVersion, ObjectKind, ObjectName string
	TargetType, Value, AverageValue       string
	Utilization                           *int32
}

type ScalingPolicy struct {
	Type                 string
	Value, PeriodSeconds int32
}

type ScalingRules struct {
	Stabilization *int32
	SelectPolicy  string
	Tolerance     string
	Policies      []ScalingPolicy
}

type HPAState struct {
	TargetVersion, TargetKind, TargetName string
	Min, Max, Current, Desired            int32
	ObservedGeneration                    *int64
	LastScale                             *time.Time
	Metrics, CurrentMetrics               []Metric
	ScaleUp, ScaleDown                    *ScalingRules
}

type IngressRoute struct {
	Default, ResourceBackend bool
	Host, Path, PathType     string
	Service, PortName        string
	PortNumber               int32
}

type IngressState struct {
	Class                  string
	Routes                 []IngressRoute
	TLSCount, AddressCount int
}

type AttachmentError struct {
	Present, HasCode bool
	Code             int32
	At               time.Time
}

type AttachmentState struct {
	Attacher, NodeName, VolumeName string
	Inline, Attached               bool
	DeletingSince                  time.Time
	AttachError, DetachError       AttachmentError
}

// Object is the allow-list shared by collection and pure rules, not a raw API object.
type Object struct {
	Resource
	Version                      string `json:"-"`
	Created                      time.Time
	Deleting, Paused             bool
	Phase                        string
	Conditions                   []Condition
	Containers                   []Container
	Generation, Observed         int64
	Desired, Available, Updated  int32
	Target                       Resource
	EventTime                    time.Time
	EventReason                  string
	EventCount                   int32
	Owner                        Resource
	Ready, Current, Misscheduled int32
	UpdateStrategy               string
	Partition                    int32
	Labels, Selector             map[string]string `json:"-"`
	ServiceType, ServiceName     string
	PublishNotReady              bool
	Ports                        []Port
	AddressType                  string
	Endpoints                    []Endpoint
	Storage                      Storage
	Claims                       []string `json:"-"`
	CronJob                      CronJobState
	HPA                          HPAState
	Ingress                      IngressState
	Attachment                   AttachmentState
	Extra                        map[string]any `json:"extra,omitempty"`
}

type Snapshot struct {
	At              time.Time
	Objects         []Object
	Continuity      uint64              `json:"-"`
	RecoveryChanges map[Resource]uint64 `json:"-"`
}

type Policy struct {
	StartupGrace, RolloutGrace, EventLookback time.Duration
	RestartThreshold                          int32
	ExtraResources                            []resourcepolicy.Resource
}

type Finding struct {
	ID       string   `json:"id"`
	Rule     string   `json:"rule_id"`
	Severity string   `json:"severity"`
	Resource Resource `json:"resource"`
	Summary  string   `json:"summary"`
	Evidence []string `json:"evidence"`
}

// ClusterScoped identifies supported globally observed kinds, independent of workload namespace selection.
func ClusterScoped(kind string) bool {
	return kind == "Node" || kind == "PersistentVolume" || kind == "VolumeAttachment"
}

// DefaultPolicy supplies grace periods, event lookback and a cumulative restart threshold for evidence checks.
func DefaultPolicy() Policy {
	return Policy{StartupGrace: 120 * time.Second, RolloutGrace: 300 * time.Second, EventLookback: time.Hour, RestartThreshold: 3}
}

// NewFinding derives stable evidence identity from the rule, resource incarnation and subject,
// independent of severity or wording, and normalizes absent evidence to an empty slice.
func NewFinding(rule, severity string, r Resource, subject, summary string, evidence ...string) Finding {
	parts := []string{rule, r.Kind, r.Namespace, r.Name, r.UID, subject}
	if r.APIGroup != "" {
		parts = append(parts, r.APIGroup)
	}

	key, _ := json.Marshal(parts)
	sum := sha256.Sum256(key)

	if evidence == nil {
		evidence = []string{}
	}

	return Finding{hex.EncodeToString(sum[:12]), rule, severity, r, summary, evidence}
}

// Actionable identifies warning or critical evidence, without authorizing an incident change.
func (f Finding) Actionable() bool { return f.Severity == "warning" || f.Severity == "critical" }

// Fingerprint hashes the full serialized finding to detect changes beyond its stable identity.
func Fingerprint(f Finding) string {
	data, _ := json.Marshal(f)
	sum := sha256.Sum256(data)

	return hex.EncodeToString(sum[:])
}

// RecoveryFingerprint compares projected recovery-sensitive state, including restarts and condition transitions.
// Heartbeats, Warning counts and current metric samples do not reset continuous recovery.
func RecoveryFingerprint(o Object) string {
	o.Conditions = slices.Clone(o.Conditions)
	for i := range o.Conditions {
		o.Conditions[i].Updated = time.Time{}
	}

	slices.SortFunc(o.Conditions, func(a, b Condition) int {
		return slices.Compare([]byte(a.Type), []byte(b.Type))
	})

	o.EventTime, o.EventCount = time.Time{}, 0
	o.HPA = o.HPA.Clone()
	o.HPA.CurrentMetrics, o.HPA.LastScale = nil, nil
	o.Attachment.AttachError.At, o.Attachment.DetachError.At = time.Time{}, time.Time{}

	data, _ := json.Marshal(struct {
		Object
		Labels, Selector map[string]string
	}{o, o.Labels, o.Selector})
	sum := sha256.Sum256(data)

	return hex.EncodeToString(sum[:])
}

// Status summarizes whether findings contain actionable evidence; it does not establish collection freshness.
func Status(findings []Finding) string {
	for _, f := range findings {
		if f.Actionable() {
			return "unhealthy"
		}
	}

	return "healthy"
}

// Sort orders findings in place by stable ID for deterministic comparisons.
func Sort(findings []Finding) {
	slices.SortFunc(findings, func(a, b Finding) int {
		if a.ID < b.ID {
			return -1
		}

		if a.ID > b.ID {
			return 1
		}

		return 0
	})
}

// condition returns the first condition of the requested type, distinguishing absence from an empty status.
func condition(o Object, name string) (Condition, bool) {
	for _, c := range o.Conditions {
		if c.Type == name {
			return c, true
		}
	}

	return Condition{}, false
}

// expired includes the grace boundary and treats a missing timestamp as already expired.
func expired(now, since time.Time, grace time.Duration) bool {
	return since.IsZero() || now.Sub(since) >= grace
}
