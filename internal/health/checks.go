package health

import "fmt"

type Check func(Snapshot, Policy) []Finding

var Checks = []Check{Nodes, Pods, Deployments, Jobs, Events}

func Evaluate(s Snapshot, p Policy) []Finding {
	findings := []Finding{}
	for _, check := range Checks {
		findings = append(findings, check(s, p)...)
	}
	Sort(findings)
	return findings
}

func Nodes(s Snapshot, _ Policy) []Finding {
	var out []Finding
	for _, o := range s.Objects {
		if o.Kind != "Node" {
			continue
		}
		ready, found := condition(o, "Ready")
		if !found || ready.Status != "True" {
			out = append(out, NewFinding("node.ready", "critical", o.Resource, "", "Node is not reporting Ready=True", "Ready="+ready.Status))
		}
		for _, name := range []string{"MemoryPressure", "DiskPressure", "PIDPressure"} {
			c, found := condition(o, name)
			if found && c.Status != "False" {
				out = append(out, NewFinding("node."+name, "warning", o.Resource, "", "Node reports "+name+"="+c.Status))
			}
		}
	}
	return out
}

func Jobs(s Snapshot, _ Policy) []Finding {
	var out []Finding
	for _, o := range s.Objects {
		if o.Kind != "Job" {
			continue
		}
		for _, name := range []string{"Failed", "FailureTarget"} {
			c, found := condition(o, name)
			if found && c.Status == "True" {
				out = append(out, NewFinding("job.failed", "critical", o.Resource, "", "Job reports a terminal failure", "condition="+name, "reason="+c.Reason))
				break
			}
		}
	}
	return out
}

func Events(s Snapshot, p Policy) []Finding {
	var out []Finding
	for _, o := range s.Objects {
		age := s.At.Sub(o.EventTime)
		if o.Kind != "Event" || o.EventTime.IsZero() || age < 0 || age > p.EventLookback {
			continue
		}
		out = append(out, NewFinding("event.warning", "info", o.Target, o.UID,
			"Recent warning event; supporting evidence, not proof of an active failure",
			"event="+o.Name, "reason="+o.EventReason, fmt.Sprintf("count=%d", o.EventCount)))
	}
	return out
}
