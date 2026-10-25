package health

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"time"
)

type Resource struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
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

// Object is the allow-list shared by collection and pure rules, not a raw API object.
type Object struct {
	Resource
	Created                     time.Time
	Deleting, Paused            bool
	Phase                       string
	Conditions                  []Condition
	Containers                  []Container
	Generation, Observed        int64
	Desired, Available, Updated int32
	Target                      Resource
	EventTime                   time.Time
	EventReason                 string
	EventCount                  int32
}

type Snapshot struct {
	At      time.Time
	Objects []Object
}

type Policy struct {
	StartupGrace, RolloutGrace, EventLookback time.Duration
	RestartThreshold                          int32
}

func DefaultPolicy() Policy {
	return Policy{120 * time.Second, 300 * time.Second, time.Hour, 3}
}

type Finding struct {
	ID       string   `json:"id"`
	Rule     string   `json:"rule_id"`
	Severity string   `json:"severity"`
	Resource Resource `json:"resource"`
	Summary  string   `json:"summary"`
	Evidence []string `json:"evidence"`
}

func NewFinding(rule, severity string, r Resource, subject, summary string, evidence ...string) Finding {
	key, _ := json.Marshal([]string{rule, r.Kind, r.Namespace, r.Name, r.UID, subject})
	sum := sha256.Sum256(key)
	if evidence == nil {
		evidence = []string{}
	}
	return Finding{hex.EncodeToString(sum[:12]), rule, severity, r, summary, evidence}
}

func (f Finding) Actionable() bool { return f.Severity == "warning" || f.Severity == "critical" }

func Fingerprint(f Finding) string {
	data, _ := json.Marshal(f)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func Status(findings []Finding) string {
	for _, f := range findings {
		if f.Actionable() {
			return "unhealthy"
		}
	}
	return "healthy"
}

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

func condition(o Object, name string) (Condition, bool) {
	for _, c := range o.Conditions {
		if c.Type == name {
			return c, true
		}
	}
	return Condition{}, false
}

func expired(now, since time.Time, grace time.Duration) bool {
	return since.IsZero() || now.Sub(since) >= grace
}
