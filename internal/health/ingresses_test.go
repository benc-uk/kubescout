/*
Purpose: Test Ingress backend configuration evidence and namespace relationships.
Flow: Offline route/Service snapshots exercise missing names/ports, grace
boundaries and exclusions without treating endpoint readiness, TLS, classes or
load-balancer status as availability probes or incident decisions.
*/
package health

import (
	"testing"
	"time"
)

// TestIngressEvidence checks exact missing-Service/port rules and avoids duplicating Service readiness evidence.
func TestIngressEvidence(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	p := DefaultPolicy()
	ingress := Object{Resource: Resource{Kind: "Ingress", Namespace: "apps", Name: "web", UID: "ingress"},
		Created: at.Add(-time.Hour), Ingress: IngressState{Routes: []IngressRoute{{Host: "web.example", Path: "/", Service: "web", PortNumber: 80}}}}
	service := Object{Resource: Resource{Kind: "Service", Namespace: "apps", Name: "web", UID: "service"},
		Ports: []Port{{Name: "http", Number: 80, Target: "8080"}}}

	for _, tt := range []struct {
		name, rule, severity string
		edit                 func(*Object, *Object)
		omitService          bool
	}{
		{name: "healthy config without endpoints", edit: func(i, s *Object) {}},
		{name: "missing service", omitService: true, rule: "ingress.service_missing", severity: "warning", edit: func(i, s *Object) {}},
		{name: "within grace", omitService: true, rule: "ingress.service_missing", severity: "info", edit: func(i, s *Object) { i.Created = at.Add(-p.StartupGrace + time.Nanosecond) }},
		{name: "grace boundary", omitService: true, rule: "ingress.service_missing", severity: "warning", edit: func(i, s *Object) { i.Created = at.Add(-p.StartupGrace) }},
		{name: "other namespace service", rule: "ingress.service_missing", severity: "warning", edit: func(i, s *Object) { s.Namespace = "other" }},
		{name: "missing numeric port", rule: "ingress.port_missing", severity: "warning", edit: func(i, s *Object) { i.Ingress.Routes[0].PortNumber = 81 }},
		{name: "targetPort is not service port", rule: "ingress.port_missing", severity: "warning", edit: func(i, s *Object) { i.Ingress.Routes[0].PortNumber = 8080 }},
		{name: "named port", edit: func(i, s *Object) { i.Ingress.Routes[0].PortNumber, i.Ingress.Routes[0].PortName = 0, "http" }},
		{name: "missing named port", rule: "ingress.port_missing", severity: "warning", edit: func(i, s *Object) { i.Ingress.Routes[0].PortNumber, i.Ingress.Routes[0].PortName = 0, "https" }},
		{name: "default backend", omitService: true, rule: "ingress.service_missing", severity: "warning", edit: func(i, s *Object) { i.Ingress.Routes[0].Default = true }},
		{name: "custom backend", omitService: true, edit: func(i, s *Object) { i.Ingress.Routes = []IngressRoute{{ResourceBackend: true}} }},
		{name: "external name service", edit: func(i, s *Object) { s.ServiceType = "ExternalName" }},
		{name: "omitted class empty status TLS configured", edit: func(i, s *Object) { i.Ingress.TLSCount = 1 }},
		{name: "deleting", omitService: true, edit: func(i, s *Object) { i.Deleting = true }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			i, s := ingress, service
			i.Ingress.Routes = append([]IngressRoute{}, ingress.Ingress.Routes...)
			tt.edit(&i, &s)

			objects := []Object{i}
			if !tt.omitService {
				objects = append(objects, s)
			}

			findings := Ingresses(Snapshot{At: at, Objects: objects}, p)
			if tt.rule == "" {
				if len(findings) != 0 {
					t.Fatalf("configuration context fabricated a failure: %+v", findings)
				}
			} else if len(findings) != 1 || findings[0].Rule != tt.rule || findings[0].Severity != tt.severity || findings[0].Resource != i.Resource {
				t.Fatalf("unexpected Ingress evidence: %+v", findings)
			}
		})
	}
}

// TestIngressBackendDeduplication emits one finding per missing Service or port rather than per host/path.
func TestIngressBackendDeduplication(t *testing.T) {
	i := Object{Resource: Resource{Kind: "Ingress", Namespace: "apps", Name: "web", UID: "uid"}, Ingress: IngressState{Routes: []IngressRoute{
		{Default: true, Service: "missing", PortNumber: 80},
		{Host: "one.example", Path: "/", Service: "missing", PortNumber: 81},
		{Host: "two.example", Path: "/api", Service: "web", PortName: "missing"},
		{Host: "two.example", Path: "/other", Service: "web", PortName: "missing"},
	}}}
	s := Object{Resource: Resource{Kind: "Service", Namespace: "apps", Name: "web", UID: "service"}, Ports: []Port{{Number: 80}}}

	findings := Evaluate(Snapshot{At: time.Now(), Objects: []Object{i, s}}, DefaultPolicy())
	if len(findings) != 2 || findings[0].Rule == findings[1].Rule || !MatchesIngressService(i, s) {
		t.Fatalf("backend findings were duplicated or missing: %+v", findings)
	}

	s.Namespace = "other"
	if MatchesIngressService(i, s) {
		t.Fatal("Ingress related Service escaped namespace scope")
	}
}
