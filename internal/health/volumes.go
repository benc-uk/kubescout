/*
Purpose: Assess PVC/PV state and verify private storage relationships.
Flow: Scoped projected objects yield grace-aware pending claims and explicit
loss/failure evidence for AI review. Claim use is context, not controller ownership;
Available/Released PVs and normal resizing do not independently imply an outage.
*/
package health

import (
	"fmt"
	"slices"
)

// MatchesVolumeClaim requires mutual binding names and the current PVC UID, not a stale claimRef.
func MatchesVolumeClaim(volume, claim Object) bool {
	return volume.Kind == "PersistentVolume" && claim.Kind == "PersistentVolumeClaim" &&
		volume.Namespace == "" && claim.UID != "" && volume.Storage.Claim == claim.Resource &&
		claim.Storage.VolumeName == volume.Name
}

// MatchesClaimPod checks same-namespace explicit PVC use, excluding deleting Pods.
func MatchesClaimPod(claim, pod Object) bool {
	return claim.Kind == "PersistentVolumeClaim" && pod.Kind == "Pod" && !pod.Deleting &&
		claim.Namespace == pod.Namespace && slices.Contains(pod.Claims, claim.Name)
}

// Volumes returns private pending, loss and resize-failure evidence; it never decides incident lifecycle.
func Volumes(s Snapshot, p Policy) []Finding {
	var out []Finding

	for _, o := range s.Objects {
		if o.Deleting {
			continue
		}

		switch o.Kind {
		case "PersistentVolumeClaim":
			switch o.Phase {
			case "Pending":
				severity := "info"
				if expired(s.At, o.Created, p.StartupGrace) {
					severity = "warning"
				}

				out = append(out, NewFinding("pvc.pending", severity, o.Resource, "",
					"PVC is pending binding; delayed binding can be intentional and does not prove a storage failure",
					"phase=Pending", fmt.Sprintf("requested_storage=%s", o.Storage.Requested)))
			case "Lost":
				out = append(out, NewFinding("pvc.lost", "critical", o.Resource, "",
					"PVC reports that its bound persistent volume is lost", "phase=Lost"))
			}

			for _, c := range o.Conditions {
				if c.Status == "True" && (c.Type == "ControllerResizeError" || c.Type == "NodeResizeError") {
					out = append(out, NewFinding("pvc.resize_error", "warning", o.Resource, c.Type,
						"PVC reports a volume resize error; existing capacity may still be usable", "condition="+c.Type))
				}
			}
		case "PersistentVolume":
			if o.Phase == "Failed" {
				out = append(out, NewFinding("pv.failed", "critical", o.Resource, "",
					"PV reports failure; this does not by itself establish application impact or data loss", "phase=Failed"))
			}
		}
	}

	return out
}
