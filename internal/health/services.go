/*
Purpose: Correlate Services with Pods and EndpointSlices to assess backend data.
Flow: Selectors, ownership, endpoint conditions and port definitions produce
grace-aware findings for selector-managed Services. This is configuration
evidence for AI assessment, not a traffic probe or an incident decision.
*/
package health

import "fmt"

// MatchesServicePod requires a nonempty matching selector and namespace, excluding deleting Pods.
func MatchesServicePod(service, pod Object) bool {
	if service.Kind != "Service" || pod.Kind != "Pod" || service.Namespace != pod.Namespace || len(service.Selector) == 0 || pod.Deleting {
		return false
	}

	for key, value := range service.Selector {
		if actual, ok := pod.Labels[key]; !ok || actual != value {
			return false
		}
	}

	return true
}

// MatchesServiceSlice checks namespace and Service linkage, rejecting deleting slices or conflicting owners.
func MatchesServiceSlice(service, slice Object) bool {
	return service.Kind == "Service" && slice.Kind == "EndpointSlice" && !slice.Deleting &&
		service.Namespace == slice.Namespace && service.Name == slice.ServiceName &&
		(slice.Owner.UID == "" || slice.Owner == service.Resource)
}

// CountEndpoints tallies total endpoints and their independent ready, serving and terminating conditions.
func CountEndpoints(endpoints []Endpoint) EndpointCounts {
	var counts EndpointCounts
	for _, endpoint := range endpoints {
		counts.Total++
		if endpoint.Ready {
			counts.Ready++
		}

		if endpoint.Serving {
			counts.Serving++
		}

		if endpoint.Terminating {
			counts.Terminating++
		}
	}

	return counts
}

// Services derives grace-aware backend and port evidence for selector-managed Services
// from matching Pods and EndpointSlices; it does not probe traffic.
func Services(s Snapshot, p Policy) []Finding {
	var out []Finding

	for _, service := range s.Objects {
		if service.Kind != "Service" || service.Deleting || service.ServiceType == "ExternalName" || len(service.Selector) == 0 {
			continue
		}

		slices, pods := 0, 0
		matching := []Object{}

		var endpoints EndpointCounts

		for _, o := range s.Objects {
			if MatchesServicePod(service, o) {
				pods++
			}

			if MatchesServiceSlice(service, o) {
				slices++

				matching = append(matching, o)
				counts := CountEndpoints(o.Endpoints)
				endpoints.Total += counts.Total
				endpoints.Ready += counts.Ready
				endpoints.Serving += counts.Serving
				endpoints.Terminating += counts.Terminating
			}
		}

		severity := "info"
		if expired(s.At, service.Created, p.StartupGrace) {
			severity = "warning"
		}

		if endpoints.Ready == 0 {
			out = append(out, NewFinding("service.backends", severity, service.Resource, "",
				"No ready EndpointSlice backends observed for a selector-managed Service; this is not a traffic probe",
				fmt.Sprintf("matched_pods=%d", pods), fmt.Sprintf("endpoint_slices=%d", slices),
				fmt.Sprintf("endpoints=%d", endpoints.Total), fmt.Sprintf("ready_endpoints=%d", endpoints.Ready),
				fmt.Sprintf("serving_endpoints=%d", endpoints.Serving), fmt.Sprintf("terminating_endpoints=%d", endpoints.Terminating)))

			continue
		}

		for _, port := range service.Ports {
			protocol := port.Protocol
			if protocol == "" {
				protocol = "TCP"
			}

			found := false

			for _, slice := range matching {
				if CountEndpoints(slice.Endpoints).Ready == 0 {
					continue
				}

				for _, backend := range slice.Ports {
					backendProtocol := backend.Protocol
					if backendProtocol == "" {
						backendProtocol = "TCP"
					}

					if port.Name == backend.Name && protocol == backendProtocol {
						found = true
						break
					}
				}

				if found {
					break
				}
			}

			if !found {
				out = append(out, NewFinding("service.ports", severity, service.Resource, port.Name+"/"+protocol,
					"Ready EndpointSlice backends lack a matching Service port definition; this is configuration evidence, not a traffic probe",
					fmt.Sprintf("service_port=%d", port.Number), fmt.Sprintf("ready_endpoints=%d", endpoints.Ready)))
			}
		}
	}

	return out
}
