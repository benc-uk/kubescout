/*
Purpose: Assess reported VolumeAttachment errors and identify related storage targets.
Flow: Cluster-scoped projections yield grace-aware attach/detach evidence for AI
review. Named current PV/Node relationships are context, not ownership; pending
attachment alone, stale errors and successful attachment do not establish impact.
*/
package health

// MatchesAttachmentVolume finds the current PV named by an attachment, without implying UID-based ownership or mounting.
func MatchesAttachmentVolume(attachment, volume Object) bool {
	return attachment.Kind == "VolumeAttachment" && volume.Kind == "PersistentVolume" && attachment.Namespace == "" && volume.Namespace == "" &&
		!attachment.Attachment.Inline && volume.UID != "" && attachment.Attachment.VolumeName != "" && attachment.Attachment.VolumeName == volume.Name
}

// MatchesAttachmentNode finds the current cluster Node named by an attachment, not a workload namespace relationship.
func MatchesAttachmentNode(attachment, node Object) bool {
	return attachment.Kind == "VolumeAttachment" && node.Kind == "Node" && attachment.Namespace == "" && node.Namespace == "" &&
		node.UID != "" && attachment.Attachment.NodeName != "" && attachment.Attachment.NodeName == node.Name
}

// Attachments emits private errors only for the current attachment operation; old error records alone do not prove failure.
func Attachments(s Snapshot, p Policy) []Finding {
	var out []Finding

	for _, o := range s.Objects {
		if o.Kind != "VolumeAttachment" {
			continue
		}

		rule, summary, operation, since := "", "", "", o.Created

		if !o.Deleting && !o.Attachment.Attached && o.Attachment.AttachError.Present {
			rule, operation = "volumeattachment.attach_error", "attach"
			summary = "VolumeAttachment reports an attach error while unattached; application impact and mount state are unknown"
		} else if o.Deleting && o.Attachment.Attached && o.Attachment.DetachError.Present {
			rule, operation, since = "volumeattachment.detach_error", "detach", o.Attachment.DeletingSince
			summary = "Deleting VolumeAttachment reports a detach error while still attached; this does not establish an application outage"
		}

		if rule == "" {
			continue
		}

		severity := "info"
		if expired(s.At, since, p.StartupGrace) {
			severity = "warning"
		}

		out = append(out, NewFinding(rule, severity, o.Resource, "", summary, "operation="+operation))
	}

	return out
}
