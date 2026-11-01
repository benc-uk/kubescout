/*
Purpose: Turn recent projected Warning events into supporting evidence.
Flow: Snapshot time and event lookback exclude missing, future and stale
occurrences; remaining events yield informational findings tied to their targets,
not proof of an active failure or an instruction to publish an incident.
*/
package health

import "fmt"

// Events returns informational evidence for projected Warning events within the lookback,
// excluding missing or future timestamps rather than treating events as active failures.
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
