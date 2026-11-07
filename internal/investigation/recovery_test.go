/*
Purpose: Test replacement-aware native descriptions and typed inspection offline.
Flow: Local HTTP fixtures distinguish original-incarnation absence from a
supplied replacement, preserve exact version checks and reject arbitrary
recreation. No live Kubernetes resources, AI or Incident writes are used.
*/
package investigation

import (
	"strings"
	"testing"

	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
)

// replacementSession adds an old anchor and the fixture's actual current incarnation as distinct authorized handles.
func replacementSession(t *testing.T, mode string) *Session {
	t.Helper()

	s, _ := descriptionSession(t, mode)
	current := s.Investigation.Identities["pod-ref"]
	original := current
	original.UID = "old-incarnation"
	s.Investigation.Anchor = "original"
	s.Investigation.Identities = map[string]health.Resource{"original": original, "replacement": current}
	s.Investigation.Objects = map[string]health.Object{"replacement": {Resource: current, Version: "7"}}
	s.Investigation.Recovery = &incident.Recovery{AnchorState: "replaced", ReplacementRef: "replacement"}

	return s
}

// TestReplacementDescriptionsAndInspection avoids describing a superseded anchor under the current incarnation's name.
func TestReplacementDescriptionsAndInspection(t *testing.T) {
	s := replacementSession(t, "ok")

	evidence, err := s.CaptureDescriptions(t.Context(), incident.MaxDescriptionBytes)
	if err != nil || len(evidence.Descriptions) != 2 {
		t.Fatalf("replacement descriptions: evidence=%+v err=%v", evidence, err)
	}

	original, current := evidence.Descriptions[0], evidence.Descriptions[1]
	if original.Resource.Ref != "original" || original.Output.Status != "unavailable" ||
		!strings.Contains(original.Output.Text, "superseded") || current.Resource.Ref != "replacement" ||
		!strings.Contains(current.Output.Text, "Name:") || s.Investigation.ToolVersions["replacement"] != "7" {
		t.Fatal("native description confused the original and current incarnations")
	}

	result, err := (InspectResource{Session: s}).Call(t.Context(), `{"resource_ref":"original"}`)
	if err != nil {
		t.Fatal(err)
	}

	fields := result.(map[string]any)
	if fields["exists"] != false || fields["replacement_ref"] != "replacement" ||
		s.Investigation.ToolVersions["original"] != "" || s.Investigation.ToolVersions["replacement"] != "7" {
		t.Fatal("typed original-incarnation absence was not tied to the reviewed replacement")
	}

	result, err = (InspectResource{Session: s}).Call(t.Context(), `{"resource_ref":"replacement"}`)
	if err != nil || result.(map[string]any)["exists"] != true {
		t.Fatal("replacement itself could not be inspected")
	}
}

// TestReplacementInspectionRejectsChangedIncarnations keeps intervening writes and unrecognized replacements as failures.
func TestReplacementInspectionRejectsChangedIncarnations(t *testing.T) {
	for _, mode := range []string{"version", "incarnation", "original-read"} {
		t.Run(mode, func(t *testing.T) {
			serverMode := "ok"
			if mode == "incarnation" {
				serverMode = "recreated"
			}

			s := replacementSession(t, serverMode)
			if mode == "version" {
				s.Investigation.ToolVersions["replacement"] = "6"
			} else if mode == "original-read" {
				s.Investigation.ToolVersions["original"] = "5"
			}

			if _, err := (InspectResource{Session: s}).Call(t.Context(), `{"resource_ref":"original"}`); err == nil {
				t.Fatal("changed replacement was accepted as original-incarnation absence")
			}
		})
	}
}

// TestHistoricalChildReplacementUsesCurrentHandle avoids native UID failures for stored children of a live controller.
func TestHistoricalChildReplacementUsesCurrentHandle(t *testing.T) {
	s := replacementSession(t, "ok")
	s.Investigation.Anchor = "controller"
	s.Investigation.Identities["controller"] = health.Resource{Kind: "Deployment", Namespace: "apps", Name: "app", UID: "controller"}
	s.Investigation.Recovery = &incident.Recovery{AnchorState: "present"}

	current, ref, known := s.knownReplacement("original")
	if !known || ref != "replacement" || current.UID != "private-uid" {
		t.Fatal("historical child was not tied to its supplied current incarnation")
	}

	result, err := (InspectResource{Session: s}).Call(t.Context(), `{"resource_ref":"original"}`)
	if err != nil || result.(map[string]any)["replacement_ref"] != "replacement" {
		t.Fatal("historical child inspection confused old and current incarnations")
	}
}
