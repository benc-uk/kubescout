/*
Purpose: Derive Node readiness and resource-pressure evidence.
Flow: Projected conditions yield critical findings when Ready is absent or not
True, and warnings for reported pressure conditions that are not False. Findings
are private inputs to AI assessment rather than user-facing incidents.
*/
package health

// Nodes flags absent or non-true readiness and reported pressure conditions that are not false.
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
