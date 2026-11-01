/*
Purpose: Return bounded approved VolumeAttachment state through handle-scoped inspection.
Flow: An authorized storage API GET exposes attachment state and redacted driver
errors with supplied PV/Node handles. Attachment metadata and inline volume specs
are excluded; text redaction cannot guarantee secret-free messages. No CSI, node
or storage writes are permitted.
*/
package investigation

import (
	"errors"

	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/kube"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// inspectAttachment returns approved operation/error fields and only already-authorized current PV/Node handles.
func inspectAttachment(obj *unstructured.Unstructured, r health.Resource, s *Session, result map[string]any) error {
	o, err := kube.ProjectResource(obj, r.Kind, r.Namespace)
	if err != nil {
		return err
	}

	fields := map[string]any{"attacher": redactValue(o.Attachment.Attacher), "node_name": redactValue(o.Attachment.NodeName),
		"attached": o.Attachment.Attached, "deleting": o.Deleting, "inline_volume": o.Attachment.Inline}

	if o.Attachment.VolumeName != "" {
		fields["volume_name"] = redactValue(o.Attachment.VolumeName)
	}

	if !o.Attachment.DeletingSince.IsZero() {
		fields["deleting_since"] = o.Attachment.DeletingSince
	}

	for key, projected := range map[string]health.AttachmentError{"attachError": o.Attachment.AttachError, "detachError": o.Attachment.DetachError} {
		if !projected.Present {
			continue
		}

		value := map[string]any{}
		if !projected.At.IsZero() {
			value["time"] = projected.At
		}

		if projected.HasCode {
			value["error_code"] = projected.Code
		}

		message, found, err := unstructured.NestedString(obj.Object, "status", key, "message")
		if err != nil {
			return errors.New("invalid attachment error schema")
		}

		if found {
			value["message"] = redactValue(message)
		}

		fields[key] = value
	}

	for ref, target := range s.Investigation.Objects {
		if identity, authorized := s.Investigation.Identities[ref]; !authorized || identity != target.Resource {
			continue
		}

		if health.MatchesAttachmentVolume(o, target) {
			fields["volume_ref"] = ref
		}

		if health.MatchesAttachmentNode(o, target) {
			fields["node_ref"] = ref
		}
	}

	result["state"] = fields

	return nil
}
