package health

import (
	"testing"
	"time"
)

func TestControllerEvidence(t *testing.T) {
	base := Object{Resource: Resource{Kind: "StatefulSet"}, Created: time.Now().Add(-time.Hour),
		Desired: 3, Ready: 3, Available: 3, Current: 3, Updated: 3, UpdateStrategy: "RollingUpdate"}
	for _, tt := range []struct {
		name string
		edit func(*Object)
		want bool
	}{
		{"healthy", func(o *Object) {}, false},
		{"unavailable", func(o *Object) { o.Available = 2 }, true},
		{"OnDelete", func(o *Object) { o.UpdateStrategy = "OnDelete"; o.Updated = 0 }, false},
		{"partition", func(o *Object) { o.Partition = 2; o.Updated = 1 }, false},
		{"daemon shortfall", func(o *Object) { o.Kind = "DaemonSet"; o.Ready = 2; o.Current = 2 }, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o := base
			tt.edit(&o)
			findings := Controllers(Snapshot{At: time.Now(), Objects: []Object{o}}, DefaultPolicy())
			if (len(findings) != 0) != tt.want {
				t.Fatalf("unexpected controller evidence: %+v", findings)
			}
		})
	}
}

func TestServiceEvidence(t *testing.T) {
	service := Object{Resource: Resource{Kind: "Service", Namespace: "apps", Name: "web", UID: "service"}, Created: time.Now().Add(-time.Hour),
		Selector: map[string]string{"app": "web"}, Ports: []Port{{Name: "http", Protocol: "TCP", Number: 80, Target: "http"}}}
	base := Object{Resource: Resource{Kind: "EndpointSlice", Namespace: "apps", Name: "web-a", UID: "slice"},
		ServiceName: "web", Endpoints: []Endpoint{{Ready: true}}, Ports: []Port{{Name: "http", Protocol: "TCP", Number: 8080}}}
	for _, tt := range []struct {
		name string
		edit func(*Object, *Object)
		rule string
	}{
		{"healthy", func(s, e *Object) {}, ""},
		{"no ready backends", func(s, e *Object) { e.Endpoints = []Endpoint{{Ready: false}} }, "service.backends"},
		{"missing named port", func(s, e *Object) { e.Ports = nil }, "service.ports"},
		{"selectorless", func(s, e *Object) { s.Selector = nil; e.Endpoints = nil }, ""},
		{"ExternalName", func(s, e *Object) { s.ServiceType = "ExternalName"; e.Endpoints = nil }, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, e := service, base
			tt.edit(&s, &e)
			findings := Services(Snapshot{At: time.Now(), Objects: []Object{s, e}}, DefaultPolicy())
			if tt.rule == "" {
				if len(findings) != 0 {
					t.Fatalf("unexpected service evidence: %+v", findings)
				}
			} else if len(findings) != 1 || findings[0].Rule != tt.rule {
				t.Fatalf("unexpected service evidence: %+v", findings)
			}
		})
	}
}
