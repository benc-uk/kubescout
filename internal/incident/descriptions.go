/*
Purpose: Define native kubectl describe evidence retained with Incident revisions.
Flow: Go captures bounded native text before AI review; the same envelope is
stored with material conclusions and validated again for dashboard display.
These descriptions contain neither raw Kubernetes objects nor model-authored text.
Each resource has one describe output, not duplicate typed state or event sections.
Description resource identities preserve optional API groups, including unavailable custom-resource descriptions.
*/
package incident

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/util/validation"
)

// Evidence holds the latest investigation's native descriptions, not an evidence history.
type Evidence struct {
	Format           string                `json:"format"`
	Descriptions     []ResourceDescription `json:"descriptions"`
	OmittedResources int                   `json:"omitted_resources"`
}

// ResourceDescription identifies one native description actually supplied to AI.
type ResourceDescription struct {
	Resource   Resource          `json:"resource"`
	Source     string            `json:"source"`
	EvidenceID string            `json:"evidence_id"`
	ObservedAt time.Time         `json:"observed_at"`
	Output     DescriptionOutput `json:"output"`
}

// DescriptionOutput distinguishes observations from omissions and unavailable reads.
type DescriptionOutput struct {
	Status string `json:"status"`
	Text   string `json:"text"`
}

const DescriptionFormat = "kubectl-describe/v1"
const MaxDescriptionBytes = 16 << 10
const MaxDescriptions = 4
const DescribeSource = "kubectl/v0.35.9"
const MaxDescribeBytes = 8 << 10

// ValidateEvidence permits absent evidence but rejects obsolete, malformed or oversized descriptions.
func ValidateEvidence(e *Evidence) error {
	if e == nil {
		return nil
	}

	invalid := errors.New("invalid resource description evidence")
	if e.Format != DescriptionFormat || len(e.Descriptions) == 0 || len(e.Descriptions) > MaxDescriptions ||
		e.OmittedResources < 0 || e.OmittedResources+len(e.Descriptions) > 50 {
		return invalid
	}

	refs, ids := map[string]bool{}, map[string]bool{}

	for _, s := range e.Descriptions {
		r := s.Resource
		if len(r.Ref) == 0 || len(r.Ref) > 128 || refs[r.Ref] ||
			len(s.EvidenceID) == 0 || len(s.EvidenceID) > 128 || ids[s.EvidenceID] ||
			s.Source != DescribeSource || s.ObservedAt.IsZero() || len(r.Kind) == 0 || len(r.Kind) > 63 ||
			len(validation.IsDNS1123Subdomain(r.Name)) != 0 ||
			(r.APIGroup != "" && len(validation.IsDNS1123Subdomain(r.APIGroup)) != 0) ||
			(r.Namespace != "" && len(validation.IsDNS1123Label(r.Namespace)) != 0) {
			return invalid
		}

		refs[r.Ref], ids[s.EvidenceID] = true, true

		switch s.Output.Status {
		case "captured", "partial", "unavailable", "absent":
		default:
			return invalid
		}

		if !utf8.ValidString(s.Output.Text) || strings.TrimSpace(s.Output.Text) == "" || len(s.Output.Text) > MaxDescribeBytes {
			return invalid
		}
	}

	data, err := json.Marshal(e)
	if err != nil || len(data) > MaxDescriptionBytes {
		return invalid
	}

	return nil
}
