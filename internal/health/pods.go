/*
Purpose: Derive Pod scheduling, readiness and container failure evidence.
Flow: Projected Pod state and startup grace produce deterministic findings,
excluding deleting or succeeded Pods. Cumulative restarts stay informational
because recency is unknown; incident decisions remain with AI assessment.
*/
package health

import (
	"fmt"
	"slices"
)

// Pods derives scheduling, readiness and container evidence, skipping deleting or succeeded Pods.
// Startup failures bypass grace, while cumulative restart counts remain informational.
func Pods(s Snapshot, p Policy) []Finding {
	var out []Finding

	for _, o := range s.Objects {
		if o.Kind != "Pod" || o.Deleting || o.Phase == "Succeeded" {
			continue
		}

		add := func(rule, severity, summary string, evidence ...string) {
			out = append(out, NewFinding(rule, severity, o.Resource, "", summary, evidence...))
		}

		old := expired(s.At, o.Created, p.StartupGrace)
		switch o.Phase {
		case "Failed":
			add("pod.failed", "critical", "Pod is in Failed phase")
		case "Unknown":
			add("pod.unknown", "warning", "Kubernetes reports the pod phase as Unknown")
		}

		scheduled, _ := condition(o, "PodScheduled")

		unscheduled := scheduled.Status == "False"
		if old && unscheduled && expired(s.At, scheduled.Transition, p.StartupGrace) {
			add("pod.unschedulable", "warning", "Pod remains unscheduled beyond the startup grace period", "reason="+scheduled.Reason)
		} else if old && !unscheduled && o.Phase == "Pending" {
			add("pod.pending", "warning", "Pod remains Pending beyond the startup grace period")
		}

		ready, _ := condition(o, "Ready")
		if old && !slices.Contains([]string{"Pending", "Failed", "Unknown"}, o.Phase) && ready.Status != "True" && expired(s.At, ready.Transition, p.StartupGrace) {
			add("pod.ready", "warning", "Pod is not Ready beyond the readiness grace period", "phase="+o.Phase)
		}

		for _, c := range o.Containers {
			label := "container="
			if c.Init {
				label = "init container="
			}

			identity := []string{label + c.Name}
			if c.Image != "" {
				identity = append(identity, "image="+c.Image)
			}

			addContainer := func(rule, severity, summary string, evidence ...string) {
				all := append(slices.Clone(identity), evidence...)
				out = append(out, NewFinding(rule, severity, o.Resource, c.Name, summary, all...))
			}
			if slices.Contains([]string{"CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "InvalidImageName", "CreateContainerConfigError", "CreateContainerError", "RunContainerError"}, c.Waiting) {
				addContainer("pod.container.waiting", "warning", "Container is waiting because of a startup or runtime failure", "reason="+c.Waiting)
			}

			if c.ExitCode != nil && *c.ExitCode != 0 {
				addContainer("pod.container.terminated", "warning", "Container is currently terminated with a non-zero exit code", fmt.Sprintf("exit_code=%d", *c.ExitCode), "reason="+c.Termination)
			}

			if c.Restarts >= p.RestartThreshold {
				evidence := []string{fmt.Sprintf("cumulative_restarts=%d", c.Restarts)}
				if c.PreviousExit != nil {
					evidence = append(evidence, fmt.Sprintf("previous_exit_code=%d", *c.PreviousExit), "previous_reason="+c.PreviousReason)
				}

				addContainer("pod.container.restarts", "info", "Container restart count reached the cumulative threshold; recency is unknown", evidence...)
			}
		}
	}

	return out
}
