/*
Purpose: Distinguish superseded resource incarnations from authorized current replacements.
Flow: Host context supplies exact same-name current handles for anchors or historical children.
Typed reads may confirm original-incarnation absence only against that known
replacement; both handles' observations remain freshness-checked. Identity
merging, arbitrary replacement discovery and Incident writes are not allowed.
*/
package investigation

import (
	"errors"

	"github.com/benc-uk/kubescout/internal/health"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// knownReplacement permits one supplied current same-name incarnation of a missing authorized resource.
// Historical child resources and the original anchor retain their separate handles.
func (s *Session) knownReplacement(ref string) (health.Resource, string, bool) {
	in := s.Investigation

	original, known := in.Identities[ref]
	if !known {
		return health.Resource{}, "", false
	}

	if _, present := in.Objects[ref]; present {
		return health.Resource{}, "", false
	}

	var replacement health.Resource

	currentRef := ""

	for candidateRef, object := range in.Objects {
		current, authorized := in.Identities[candidateRef]
		if !authorized || current != object.Resource || current.UID == "" || current.UID == original.UID ||
			current.Kind != original.Kind || current.Namespace != original.Namespace || current.Name != original.Name {
			continue
		}

		if currentRef != "" {
			return health.Resource{}, "", false
		}

		replacement, currentRef = current, candidateRef
	}

	return replacement, currentRef, currentRef != ""
}

// replacementAbsence confirms the original incarnation is absent only when the named read matches its supplied replacement.
// Missing versions, intervening changes and unrecognized incarnations remain explicit failures.
func (s *Session) replacementAbsence(ref string, obj *unstructured.Unstructured) (string, bool, error) {
	replacement, replacementRef, known := s.knownReplacement(ref)
	if !known || obj.GetKind() != replacement.Kind || obj.GetName() != replacement.Name ||
		obj.GetNamespace() != replacement.Namespace || string(obj.GetUID()) != replacement.UID {
		return "", false, nil
	}

	if obj.GetResourceVersion() == "" {
		return "", false, errors.New("replacement inspection response lacks a resource version")
	}

	if version, read := s.Investigation.ToolVersions[ref]; read && version != "" {
		return "", false, errors.New("resource was replaced between investigation reads")
	}

	if version, read := s.Investigation.ToolVersions[replacementRef]; read && version != obj.GetResourceVersion() {
		return "", false, errors.New("replacement changed between investigation reads")
	}

	if s.Investigation.ToolVersions != nil {
		s.Investigation.ToolVersions[ref] = ""
		s.Investigation.ToolVersions[replacementRef] = obj.GetResourceVersion()
	}

	return replacementRef, true, nil
}
