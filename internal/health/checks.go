/*
Purpose: Register and evaluate the deterministic resource evidence checks.
Flow: A projected snapshot and policy feed each pure check; sorted findings
support investigation scheduling and AI assessment, not incident publication.
*/
package health

type Check func(Snapshot, Policy) []Finding

var Checks = []Check{Nodes, Pods, Deployments, Controllers, Services, Jobs, Events}

// Evaluate runs every registered pure check and returns findings in stable ID order.
func Evaluate(s Snapshot, p Policy) []Finding {
	findings := []Finding{}
	for _, check := range Checks {
		findings = append(findings, check(s, p)...)
	}

	Sort(findings)

	return findings
}
