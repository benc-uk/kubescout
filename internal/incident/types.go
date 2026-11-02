/*
Purpose: Define incident identity, lifecycle and AI investigation contracts.
Flow: Model-visible contexts keep host identities separate; decision validation
checks actions, resource handles, citations and recovery eligibility before
reconciliation, while allowing explicit AI no-action.
Host-owned descriptions accompany decisions outside the model's JSON contract.
*/
package incident

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
)

type Identity struct {
	Observer string          `json:"observer"`
	Anchor   health.Resource `json:"anchor"`
	Family   string          `json:"family"`
}

type Analysis struct {
	Title           string   `json:"title"`
	Severity        string   `json:"severity"`
	Impact          string   `json:"impact"`
	Diagnosis       string   `json:"diagnosis"`
	Explanation     string   `json:"explanation"`
	EvidenceIDs     []string `json:"evidence_ids"`
	Recommendations []string `json:"recommendations"`
	Uncertainty     string   `json:"uncertainty"`
	Model           string   `json:"model"`
	PromptVersion   string   `json:"prompt_version"`
	AnalysisID      string   `json:"analysis_id"`
}

type Lifecycle struct {
	State       string     `json:"state"`
	Revision    int64      `json:"revision"`
	Recurrences int64      `json:"recurrences"`
	RaisedAt    time.Time  `json:"raised_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	ResolvedAt  *time.Time `json:"resolved_at,omitempty"`
}

type Incident struct {
	Identity  Identity          `json:"identity"`
	Resources []health.Resource `json:"resources"`
	Analysis  Analysis          `json:"analysis"`
	Lifecycle Lifecycle         `json:"lifecycle"`
	Evidence  *Evidence         `json:"evidence,omitempty"`
}

type Resource struct {
	Ref       string `json:"ref"`
	Kind      string `json:"kind"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}

type Signal struct {
	ID       string   `json:"id"`
	Rule     string   `json:"rule"`
	Hint     string   `json:"triage_hint"`
	Resource string   `json:"resource_ref"`
	Summary  string   `json:"summary"`
	Evidence []string `json:"evidence"`
}

type Existing struct {
	ID        string    `json:"id"`
	Family    string    `json:"family"`
	Analysis  Analysis  `json:"analysis"`
	Lifecycle Lifecycle `json:"lifecycle"`
}

type InvestigationContext struct {
	Anchor           string     `json:"anchor_ref"`
	Reason           string     `json:"reason"`
	RecoveryEligible bool       `json:"recovery_eligible"`
	ObservedAt       time.Time  `json:"observed_at"`
	Resources        []Resource `json:"resources"`
	Signals          []Signal   `json:"signals"`
	Incidents        []Existing `json:"incidents"`
	Evidence         *Evidence  `json:"evidence,omitempty"`
	// Identities and objects are host-side only. UIDs are never model evidence.
	Identities   map[string]health.Resource `json:"-"`
	Objects      map[string]health.Object   `json:"-"`
	Revision     string                     `json:"-"`
	ToolVersions map[string]string          `json:"-"`
}

type Action struct {
	Operation       string   `json:"operation"`
	IncidentID      string   `json:"incident_id"`
	Family          string   `json:"family"`
	ResourceRefs    []string `json:"resource_refs"`
	Title           string   `json:"title"`
	Severity        string   `json:"severity"`
	Impact          string   `json:"impact"`
	Diagnosis       string   `json:"diagnosis"`
	Explanation     string   `json:"explanation"`
	EvidenceIDs     []string `json:"evidence_ids"`
	Recommendations []string `json:"recommendations"`
	Uncertainty     string   `json:"uncertainty"`
	MaterialChange  bool     `json:"material_change"`
}

type Decision struct {
	Outcome  string    `json:"outcome"`
	Reason   string    `json:"reason"`
	Actions  []Action  `json:"actions"`
	Evidence *Evidence `json:"-"`
}

type Change struct {
	Operation string   `json:"operation"`
	Name      string   `json:"name"`
	Incident  Incident `json:"incident"`
}

const APIVersion = "kubescout.benc-uk.github.com/v1alpha1"
const PromptVersion = "incident-v4"

// Name derives a stable Incident name from observer, anchor identity and incident family.
func Name(identity Identity) string {
	data, _ := json.Marshal(identity)
	sum := sha256.Sum256(data)

	return "inc-" + hex.EncodeToString(sum[:8])
}

var Families = []string{"scheduling", "container-failure", "rollout", "job-failure", "node-health", "service-availability", "storage-availability", "autoscaling"}
var diagnosisKey = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

// Validate permits explicit no-action or checks action bounds, known identities and citations.
// Resolution additionally requires an open incident and host-confirmed recovery eligibility.
func Validate(d Decision, in InvestigationContext, citations map[string]bool) error {
	invalid := errors.New("invalid AI incident decision")
	if strings.TrimSpace(d.Reason) == "" || len(d.Reason) > 4000 || d.Actions == nil || len(d.Actions) > 5 {
		return invalid
	}

	if d.Outcome == "no_action" {
		if len(d.Actions) != 0 {
			return invalid
		}

		return nil
	}

	if d.Outcome != "decisions" || len(d.Actions) == 0 {
		return invalid
	}

	existing := map[string]Existing{}
	for _, i := range in.Incidents {
		existing[i.ID] = i
	}

	families := map[string]bool{}
	for _, a := range d.Actions {
		if !slices.Contains(Families, a.Family) || families[a.Family] {
			return invalid
		}

		families[a.Family] = true
		if !slices.Contains([]string{"raise", "update", "resolve"}, a.Operation) {
			return invalid
		}

		if a.Operation == "raise" {
			if a.IncidentID != "" {
				return invalid
			}

			for _, i := range in.Incidents {
				if i.Family == a.Family {
					return errors.New("AI attempted to duplicate a known incident")
				}
			}
		} else {
			i, ok := existing[a.IncidentID]
			if !ok || i.Family != a.Family {
				return errors.New("AI referenced an unknown incident")
			}

			if a.Operation == "resolve" && (!in.RecoveryEligible || i.Lifecycle.State != "open") {
				return errors.New("AI resolution lacks stable fresh recovery")
			}
		}

		if !slices.Contains([]string{"low", "medium", "high", "critical"}, a.Severity) ||
			!slices.Contains([]string{"limited", "degraded", "unavailable"}, a.Impact) {
			return invalid
		}

		for _, text := range []string{a.Title, a.Diagnosis, a.Explanation, a.Uncertainty} {
			if strings.TrimSpace(text) == "" || len(text) > 4000 {
				return invalid
			}

			if !diagnosisKey.MatchString(a.Diagnosis) {
				return errors.New("AI diagnosis must use a stable semantic key")
			}
		}

		if !validStrings(a.ResourceRefs, 50) || !validStrings(a.EvidenceIDs, 50) || !validStrings(a.Recommendations, 10) {
			return invalid
		}

		for _, ref := range a.ResourceRefs {
			if _, ok := in.Identities[ref]; !ok {
				return errors.New("AI referenced an unauthorised resource")
			}
		}

		for _, id := range a.EvidenceIDs {
			if !citations[id] {
				return errors.New("AI referenced unknown evidence")
			}
		}
	}

	return nil
}

// validStrings requires a bounded, nonempty list of unique, nonblank strings of bounded length.
func validStrings(values []string, max int) bool {
	if len(values) == 0 || len(values) > max {
		return false
	}

	seen := map[string]bool{}
	for _, v := range values {
		if strings.TrimSpace(v) == "" || len(v) > 4000 || seen[v] {
			return false
		}

		seen[v] = true
	}

	return true
}
