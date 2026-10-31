package health

import "fmt"

func Controllers(s Snapshot, p Policy) []Finding {
	var out []Finding
	for _, o := range s.Objects {
		if o.Deleting || (o.Kind != "StatefulSet" && o.Kind != "DaemonSet") {
			continue
		}
		prefix := "statefulset"
		if o.Kind == "DaemonSet" {
			prefix = "daemonset"
		}
		severity := "info"
		if expired(s.At, o.Created, p.RolloutGrace) {
			severity = "warning"
		}
		evidence := []string{fmt.Sprintf("desired=%d", o.Desired), fmt.Sprintf("ready=%d", o.Ready),
			fmt.Sprintf("available=%d", o.Available), fmt.Sprintf("updated=%d", o.Updated),
			fmt.Sprintf("generation=%d", o.Generation), fmt.Sprintf("observed_generation=%d", o.Observed),
			"update_strategy=" + o.UpdateStrategy}
		if o.Desired > 0 && o.Observed < o.Generation {
			out = append(out, NewFinding(prefix+".unobserved", severity, o.Resource, "",
				"Controller has not observed the desired generation", evidence...))
			continue
		}
		updated := o.Desired
		if o.Kind == "StatefulSet" {
			updated = max(int32(0), o.Desired-o.Partition)
			evidence = append(evidence, fmt.Sprintf("partition=%d", o.Partition))
		}
		incomplete := o.Ready < o.Desired || o.Available < o.Desired
		if o.Kind == "DaemonSet" {
			incomplete = incomplete || o.Current < o.Desired
		}
		if o.UpdateStrategy != "OnDelete" {
			incomplete = incomplete || o.Updated < updated
		}
		if o.Desired > 0 && incomplete {
			out = append(out, NewFinding(prefix+".replicas", severity, o.Resource, "",
				"Controller reports a replica or update shortfall", evidence...))
		}
		if o.Kind == "DaemonSet" && o.Misscheduled > 0 {
			out = append(out, NewFinding("daemonset.misscheduled", severity, o.Resource, "",
				"DaemonSet reports Pods on nodes that should not run them",
				fmt.Sprintf("misscheduled=%d", o.Misscheduled)))
		}
	}
	return out
}
