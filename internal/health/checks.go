/*
Purpose: Register named deterministic resource checks and evaluate their evidence.
Flow: A projected snapshot and policy feed each pure check; sorted findings
support investigation scheduling and AI assessment, not incident publication.
*/
package health

type Check struct {
	Name string
	Run  func(Snapshot, Policy) []Finding
}

var Checks = []Check{{"nodes", Nodes}, {"pods", Pods}, {"deployments", Deployments},
	{"controllers", Controllers}, {"services", Services}, {"ingresses", Ingresses}, {"storage", Volumes}, {"attachments", Attachments}, {"autoscaling", HPAs}, {"jobs", Jobs}, {"events", Events}}

// Evaluate runs every registered pure check and returns findings in stable ID order.
func Evaluate(s Snapshot, p Policy) []Finding {
	findings := []Finding{}
	for _, check := range Checks {
		findings = append(findings, check.Run(s, p)...)
	}

	Sort(findings)

	return findings
}
