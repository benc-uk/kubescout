/*
Purpose: Project networking.k8s.io/v1 Ingress routing facts within explicit bounds.
Flow: Typed routes retain host/path and Service port references for local checks
and redacted inspection. TLS entries and load-balancer addresses become counts;
annotations, Secret names, raw addresses and custom backend objects are excluded.
*/
package kube

import (
	"errors"

	"github.com/benc-uk/kubescout/internal/health"
	networkingv1 "k8s.io/api/networking/v1"
)

// projectIngress validates bounded route structures and retains approved facts without external or Secret reads.
func projectIngress(ingress networkingv1.Ingress, o *health.Object) error {
	if len(ingress.Spec.Rules) > 128 || len(ingress.Spec.TLS) > 128 || len(ingress.Status.LoadBalancer.Ingress) > 128 {
		return errors.New("Ingress projection exceeds limit")
	}

	o.Generation = ingress.Generation
	o.Ingress.TLSCount, o.Ingress.AddressCount = len(ingress.Spec.TLS), len(ingress.Status.LoadBalancer.Ingress)

	if ingress.Spec.IngressClassName != nil {
		if len(*ingress.Spec.IngressClassName) > 253 {
			return errors.New("Ingress class projection exceeds limit")
		}

		o.Ingress.Class = *ingress.Spec.IngressClassName
	}

	add := func(backend networkingv1.IngressBackend, route health.IngressRoute) error {
		if len(o.Ingress.Routes) >= 128 {
			return errors.New("Ingress route projection exceeds limit")
		}

		if (backend.Service == nil) == (backend.Resource == nil) {
			return errors.New("Ingress backend must specify exactly one backend type")
		}

		if backend.Service != nil {
			route.Service, route.PortName, route.PortNumber = backend.Service.Name, backend.Service.Port.Name, backend.Service.Port.Number
			if route.Service == "" || len(route.Service) > 253 || len(route.PortName) > 63 ||
				(route.PortName == "") == (route.PortNumber == 0) || route.PortNumber < 0 || route.PortNumber > 65535 {
				return errors.New("invalid Ingress Service backend")
			}
		} else {
			route.ResourceBackend = true
		}

		o.Ingress.Routes = append(o.Ingress.Routes, route)

		return nil
	}

	if ingress.Spec.DefaultBackend != nil {
		if err := add(*ingress.Spec.DefaultBackend, health.IngressRoute{Default: true}); err != nil {
			return err
		}
	}

	for _, rule := range ingress.Spec.Rules {
		if len(rule.Host) > 253 {
			return errors.New("Ingress host projection exceeds limit")
		}

		if rule.HTTP == nil {
			continue
		}

		if len(rule.HTTP.Paths) > 128 {
			return errors.New("Ingress route projection exceeds limit")
		}

		for _, path := range rule.HTTP.Paths {
			if len(path.Path) > 2048 || path.PathType == nil {
				return errors.New("invalid or oversized Ingress path")
			}

			switch *path.PathType {
			case networkingv1.PathTypeExact, networkingv1.PathTypePrefix, networkingv1.PathTypeImplementationSpecific:
			default:
				return errors.New("unknown Ingress path type")
			}

			if err := add(path.Backend, health.IngressRoute{Host: rule.Host, Path: path.Path, PathType: string(*path.PathType)}); err != nil {
				return err
			}
		}
	}

	return nil
}
