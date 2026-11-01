/*
Purpose: Return bounded approved Ingress configuration through handle-scoped inspection.
Flow: An authorized GET becomes redacted routing facts, counts and supplied
same-namespace Service handles. No DNS/HTTP probe, Secret, IngressClass or
custom backend read is made, and no controller address is exposed.
*/
package investigation

import (
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/kube"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// inspectIngress returns approved routing descriptions and only already-authorized backend Service handles.
func inspectIngress(obj *unstructured.Unstructured, r health.Resource, s *Session, result map[string]any) error {
	o, err := kube.ProjectResource(obj, r.Kind, r.Namespace)
	if err != nil {
		return err
	}

	routes := []map[string]any{}

	for _, route := range o.Ingress.Routes {
		value := map[string]any{"default_backend": route.Default, "host": redactValue(route.Host),
			"path": redactValue(route.Path), "path_type": redactValue(route.PathType)}

		if route.ResourceBackend {
			value["backend_type"] = "Resource"
		} else {
			value["backend_type"], value["service"] = "Service", redactValue(route.Service)

			if route.PortName != "" {
				value["port_name"] = redactValue(route.PortName)
			} else {
				value["port_number"] = route.PortNumber
			}

			for ref, service := range s.Investigation.Objects {
				if identity, authorized := s.Investigation.Identities[ref]; authorized && identity == service.Resource &&
					service.Name == route.Service && health.MatchesIngressService(o, service) {
					value["service_ref"] = ref
				}
			}
		}

		routes = append(routes, value)
	}

	result["state"] = map[string]any{"generation": o.Generation, "class_name": redactValue(o.Ingress.Class),
		"routes": routes, "tls_entry_count": o.Ingress.TLSCount, "load_balancer_address_count": o.Ingress.AddressCount}

	return nil
}
