/*
Purpose: Identify missing Ingress Service backends and Service ports.
Flow: Complete scoped snapshots support grace-aware configuration evidence;
related Services supply context, not ownership. Rules never probe traffic,
inspect TLS Secrets/classes or duplicate existing endpoint-readiness evidence.
*/
package health

import "strconv"

// MatchesIngressService identifies a current same-namespace Service referenced by an approved route.
func MatchesIngressService(ingress, service Object) bool {
	if ingress.Kind != "Ingress" || service.Kind != "Service" || ingress.Namespace != service.Namespace || service.UID == "" {
		return false
	}

	for _, route := range ingress.Ingress.Routes {
		if !route.ResourceBackend && route.Service == service.Name {
			return true
		}
	}

	return false
}

// Ingresses emits one private finding per distinct missing Service or Service port after complete collection.
func Ingresses(s Snapshot, p Policy) []Finding {
	services := map[string]Object{}

	for _, o := range s.Objects {
		if o.Kind == "Service" {
			services[o.Namespace+"/"+o.Name] = o
		}
	}

	var out []Finding

	for _, o := range s.Objects {
		if o.Kind != "Ingress" || o.Deleting {
			continue
		}

		severity := "info"
		if expired(s.At, o.Created, p.StartupGrace) {
			severity = "warning"
		}

		seen := map[string]bool{}

		for _, route := range o.Ingress.Routes {
			if route.ResourceBackend {
				continue
			}

			port := "number:" + strconv.Itoa(int(route.PortNumber))
			if route.PortName != "" {
				port = "name:" + route.PortName
			}

			rule, subject, summary := "", route.Service, ""
			service, exists := services[o.Namespace+"/"+route.Service]

			if !exists {
				rule, summary = "ingress.service_missing", "Ingress references a Service absent from the complete scoped snapshot; traffic has not been tested"
			} else {
				found := false

				for _, candidate := range service.Ports {
					if route.PortName != "" && candidate.Name == route.PortName ||
						route.PortName == "" && candidate.Number == route.PortNumber {
						found = true
						break
					}
				}

				if !found {
					rule, subject = "ingress.port_missing", route.Service+"/"+port
					summary = "Ingress references a port absent from its backend Service definition; this is not a reachability probe"
				}
			}

			if rule != "" && !seen[rule+"/"+subject] {
				out = append(out, NewFinding(rule, severity, o.Resource, subject, summary, "backend_type=Service"))
				seen[rule+"/"+subject] = true
			}
		}
	}

	return out
}
