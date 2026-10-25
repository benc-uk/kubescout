package report

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"k8s-ops-agent/internal/health"
)

type Item struct {
	FindingIDs      []string `json:"finding_ids"`
	Explanation     string   `json:"explanation"`
	SuggestedChecks []string `json:"suggested_checks"`
}

type Analysis struct {
	Status     string   `json:"status"`
	Reason     string   `json:"reason,omitempty"`
	Items      []Item   `json:"items"`
	FindingIDs []string `json:"finding_ids,omitempty"`
}

func AnalysisState(status, reason string) Analysis {
	return Analysis{Status: status, Reason: reason, Items: []Item{}}
}

type Changes struct {
	New      []string `json:"new"`
	Updated  []string `json:"updated"`
	Resolved []string `json:"resolved"`
}

type Report struct {
	SchemaVersion      string           `json:"schema_version"`
	ID                 string           `json:"report_id"`
	AssessmentID       string           `json:"assessment_id"`
	Type               string           `json:"type"`
	At                 time.Time        `json:"observed_at"`
	EmittedAt          time.Time        `json:"emitted_at"`
	Cluster            string           `json:"cluster_identifier"`
	Namespaces         []string         `json:"namespaces"`
	CollectionComplete bool             `json:"collection_complete"`
	Execution          string           `json:"execution_status"`
	Health             string           `json:"health_status"`
	Findings           []health.Finding `json:"findings"`
	Changes            Changes          `json:"changes"`
	Analysis           Analysis         `json:"analysis"`
	Errors             []string         `json:"errors"`
}

func ID() string {
	var id [16]byte
	_, _ = rand.Read(id[:])
	return hex.EncodeToString(id[:])
}

func New(cluster string, namespaces []string, at time.Time) Report {
	id := ID()
	return Report{
		SchemaVersion: "2.0", ID: id, AssessmentID: id, Type: "assessment", At: at.UTC(),
		Cluster: cluster, Namespaces: namespaces, Execution: "completed", Health: "unknown",
		Findings: []health.Finding{}, Changes: Changes{[]string{}, []string{}, []string{}},
		Analysis: AnalysisState("skipped", "no actionable findings"), Errors: []string{},
	}
}

func Diff(before, after []health.Finding) Changes {
	changes := Changes{[]string{}, []string{}, []string{}}
	prior := map[string]string{}
	for _, f := range before {
		prior[f.ID] = health.Fingerprint(f)
	}
	for _, f := range after {
		value, found := prior[f.ID]
		if !found {
			changes.New = append(changes.New, f.ID)
		} else if value != health.Fingerprint(f) {
			changes.Updated = append(changes.Updated, f.ID)
		}
		delete(prior, f.ID)
	}
	for _, f := range before {
		if _, found := prior[f.ID]; found {
			changes.Resolved = append(changes.Resolved, f.ID)
		}
	}
	return changes
}

func (c Changes) Empty() bool { return len(c.New)+len(c.Updated)+len(c.Resolved) == 0 }

type Writer struct {
	Out  io.Writer
	File string
}

func (w Writer) Write(r Report) error {
	r.EmittedAt = time.Now().UTC()
	var fileErr error
	if w.File != "" {
		fileErr = atomicWrite(w.File, r)
		if fileErr != nil {
			r.Execution = "incomplete"
			r.Errors = append(r.Errors, "report file write failed")
		}
	}
	if err := json.NewEncoder(w.Out).Encode(r); err != nil {
		if w.File != "" && fileErr == nil {
			r.Execution = "incomplete"
			r.Errors = append(r.Errors, "stdout write failed")
			if e := atomicWrite(w.File, r); e != nil {
				return errors.New("stdout and report file write failed")
			}
		}
		return errors.New("stdout write failed")
	}
	if fileErr != nil {
		return errors.New("report file write failed")
	}
	return nil
}

func atomicWrite(path string, r Report) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".k8s-health-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := json.NewEncoder(f).Encode(r); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
