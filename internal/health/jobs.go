/*
Purpose: Identify terminal Job failure conditions in projected snapshots.
Flow: Failed or FailureTarget conditions set to True yield one critical finding
per Job. These deterministic signals inform AI assessment but do not themselves
authorize an incident lifecycle change.
*/
package health

// Jobs emits one critical finding per Job with a true Failed or FailureTarget condition.
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
