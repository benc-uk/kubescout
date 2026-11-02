/*
Purpose: Capture native kubectl describe as the sole stored resource-detail evidence.
Flow: The pinned library performs guarded reads for the anchor and related resources. Its text is
credential-protected and byte-bounded before AI input, transcripts and Incident storage.
Operational sections are retained without prefixed notices; annotations, environment and UIDs stay private.
Unavailable reads remain explicit; on-demand typed tools are unchanged.
*/
package investigation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/output"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/describe"
)

var describeField = regexp.MustCompile(`^([ \t]*)([^:]+):(?:[ \t]*(.*))$`)

// CaptureDescriptions prepares at most four native descriptions within the shared serialized evidence budget.
// Scope, identity and protocol failures abort; unavailable reads never imply health or recovery.
func (s *Session) CaptureDescriptions(ctx context.Context, budget int) (*incident.Evidence, error) {
	budget = min(budget, incident.MaxDescriptionBytes, 32768-s.Bytes)
	refs := s.descriptionRefs()
	evidence := &incident.Evidence{Format: incident.DescriptionFormat, Descriptions: []incident.ResourceDescription{},
		OmittedResources: len(refs) - min(len(refs), incident.MaxDescriptions)}

	for i, ref := range refs[:min(len(refs), incident.MaxDescriptions)] {
		r, err := s.authorize(ref)
		if err != nil {
			return nil, err
		}

		evidence.Descriptions = append(evidence.Descriptions, incident.ResourceDescription{
			Resource: incident.Resource{Ref: ref, Kind: r.Kind, Namespace: r.Namespace, Name: r.Name},
			Source:   incident.DescribeSource, EvidenceID: fmt.Sprintf("kubectl-describe-%d", i+1), ObservedAt: time.Now().UTC(),
			Output: incident.DescriptionOutput{Status: "unavailable", Text: "Describe unavailable."},
		})
	}

	if err := fitDescriptions(evidence, budget); err != nil {
		return nil, err
	}

	limits := &describeBudget{}

	for i := range evidence.Descriptions {
		item := &evidence.Descriptions[i]
		item.ObservedAt = time.Now().UTC()

		section, err := s.describeResource(ctx, item.Resource.Ref, limits)
		if err != nil {
			return nil, err
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		item.Output = section
	}

	if err := fitDescriptions(evidence, budget); err != nil {
		return nil, err
	}

	if err := incident.ValidateEvidence(evidence); err != nil {
		return nil, err
	}

	data, err := json.Marshal(evidence)
	if err != nil {
		return nil, errors.New("describe serialization failed")
	}

	s.Bytes += len(data)

	for _, item := range evidence.Descriptions {
		s.Citations[item.EvidenceID] = true

		level := "info"
		if item.Output.Status != "captured" {
			level = "warn"
		}

		_ = s.Log.Log(level, "GO", "DESCRIBE", fmt.Sprintf("%s %s %s/%s status=%s",
			output.Scope(ctx), item.Resource.Kind, item.Resource.Namespace, item.Resource.Name, item.Output.Status))
	}

	s.Transcript.Record(output.Scope(ctx), "GO", "DESCRIBE", string(data))

	return evidence, nil
}

// descriptionRefs prioritizes the anchor, then signal urgency and stable handles.
func (s *Session) descriptionRefs() []string {
	priority := map[string]int{s.Investigation.Anchor: 100}
	for _, signal := range s.Investigation.Signals {
		rank := map[string]int{"critical": 4, "warning": 3, "info": 2}[signal.Hint]
		priority[signal.Resource] = max(priority[signal.Resource], rank)
	}

	refs := slices.Sorted(maps.Keys(s.Investigation.Identities))
	slices.SortStableFunc(refs, func(a, b string) int { return priority[b] - priority[a] })

	return refs
}

// describeResource invokes the actual kubectl resource describer behind a deadline-aware transport.
func (s *Session) describeResource(ctx context.Context, ref string, budget *describeBudget) (incident.DescriptionOutput, error) {
	r, err := s.authorize(ref)
	if err != nil {
		return incident.DescriptionOutput{}, err
	}

	unavailable := incident.DescriptionOutput{Status: "unavailable", Text: "kubectl describe unavailable. Use the scoped inspection tools for additional evidence."}
	if s.DescribeConfig == nil {
		unavailable.Text = "kubectl describe unavailable: no authenticated describe client configured."
		return unavailable, nil
	}

	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	cfg := rest.CopyConfig(s.DescribeConfig)

	base, err := url.Parse(cfg.Host)
	if err != nil || base.Host == "" {
		return incident.DescriptionOutput{}, errors.New("invalid describe API endpoint configuration")
	}

	cfg.Timeout = s.Timeout
	cfg.QPS = -1
	cfg.RateLimiter = nil
	cfg.ContentType, cfg.AcceptContentTypes = "application/json", "application/json"
	guard := &describeTransport{ctx: ctx, session: s, root: r, budget: budget,
		host: base.Host, basePath: strings.TrimSuffix(base.Path, "/"), privateIDs: map[string]bool{}}

	cfg.Wrap(func(next http.RoundTripper) http.RoundTripper { guard.next = next; return guard })

	describer, supported := describe.DescriberFor(schema.GroupKind{Group: resources[r.Kind].Group, Kind: r.Kind}, cfg)
	if !supported {
		unavailable.Text = "The pinned kubectl library has no resource describer for this kind; no generic raw-object fallback was used."
		return unavailable, nil
	}

	text, describeErr := describer.Describe(r.Namespace, r.Name, describe.DescriberSettings{ShowEvents: true, ChunkSize: 20})

	if guard.failure != nil {
		return incident.DescriptionOutput{}, guard.failure
	}

	if guard.rootAbsent {
		return incident.DescriptionOutput{Status: "absent", Text: "Resource not found at describe time. Absence alone does not establish recovery."}, nil
	}

	if describeErr != nil || ctx.Err() != nil {
		return unavailable, nil
	}

	text = describeText(text, guard.privateIDs, s.SecretValues, guard.eventUnavailable, guard.relatedIncomplete)

	status := "captured"
	if guard.partial {
		status = "partial"
	}

	if len(text) > incident.MaxDescribeBytes {
		text = describeExcerpt(text, incident.MaxDescribeBytes/2)
		status = "partial"
	}

	return incident.DescriptionOutput{Status: status, Text: text}, nil
}

// protectDescribeValues excludes annotations/environment and escapes controls before native rendering.
// UIDs remain available for kubectl correlation but are removed from the final text.
func protectDescribeValues(value any, ids map[string]bool) {
	escape := strings.NewReplacer("\\", "\\\\", "\r", "\\r", "\n", "\\n", "\t", "\\t", "\x1b", "\\u001b")

	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			switch key {
			case "metadata":
				if metadata, ok := child.(map[string]any); ok {
					delete(metadata, "annotations")
				}
			case "containers", "initContainers", "ephemeralContainers":
				if containers, ok := child.([]any); ok {
					for _, container := range containers {
						if fields, ok := container.(map[string]any); ok {
							delete(fields, "env")
							delete(fields, "envFrom")
						}
					}
				}
			}

			if text, ok := child.(string); ok {
				if strings.EqualFold(key, "uid") && text != "" {
					ids[text] = true
				}

				v[key] = escape.Replace(text)
			} else {
				protectDescribeValues(child, ids)
			}
		}
	case []any:
		for i, child := range v {
			if text, ok := child.(string); ok {
				v[i] = escape.Replace(text)
			} else {
				protectDescribeValues(child, ids)
			}
		}
	}
}

// describeText preserves native operational sections without a preamble while protecting credentials and private metadata.
// Failed related reads remain explicit rather than kubectl's misleading empty sections.
func describeText(text string, privateIDs map[string]bool, secrets []string, eventsUnavailable, relatedIncomplete bool) string {
	lines := []string{}
	skipIndent := -1

	for _, line := range strings.Split(text, "\n") {
		indent := len(line) - len(strings.TrimLeft(line, " \t"))
		if skipIndent >= 0 && indent > skipIndent {
			continue
		}

		if strings.TrimSpace(line) == "" {
			lines = append(lines, "")
			continue
		}

		skipIndent = -1

		field := describeField.FindStringSubmatch(line)
		if field != nil {
			key := strings.TrimSpace(field[2])
			if key == "Events" && eventsUnavailable ||
				relatedIncomplete && slices.Contains([]string{"Pods Statuses", "OldReplicaSets", "NewReplicaSet", "Default backend", "Rules"}, key) {
				lines = append(lines, field[1]+key+": [unavailable: related read incomplete or outside scope]")
				skipIndent = indent

				continue
			}
		}

		lines = append(lines, line)
	}

	result := strings.Join(lines, "\n")
	for id := range privateIDs {
		result = strings.ReplaceAll(result, id, "[UID omitted]")
	}

	return output.Redact(result, secrets...)
}

// describeExcerpt truncates native text at a valid UTF-8 boundary and marks omitted content.
func describeExcerpt(text string, end int) string {
	end = min(end, len(text))
	for end > 0 && end < len(text) && !utf8.RuneStart(text[end]) {
		end--
	}

	return text[:end] + "\n[Additional kubectl describe output omitted by size limit]"
}

// fitDescriptions shrinks native output at UTF-8 boundaries without dropping resource coverage metadata.
func fitDescriptions(evidence *incident.Evidence, budget int) error {
	for {
		data, err := json.Marshal(evidence)
		if err != nil {
			return errors.New("describe serialization failed")
		}

		if len(data) <= budget {
			return nil
		}

		var largest *incident.DescriptionOutput

		for i := range evidence.Descriptions {
			section := &evidence.Descriptions[i].Output
			if len(section.Text) > 64 && (largest == nil || len(section.Text) > len(largest.Text)) {
				largest = section
			}
		}

		if largest == nil {
			return &LimitError{Kind: "local_describe_metadata_bytes", Observed: len(data), Limit: budget}
		}

		if len(largest.Text) < 256 {
			*largest = incident.DescriptionOutput{Status: "unavailable", Text: "Describe omitted by input budget."}
		} else {
			largest.Text = describeExcerpt(largest.Text, len(largest.Text)/2)
			largest.Status = "partial"
		}
	}
}
