package incident

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"k8s-ops-agent/internal/health"
)

func Ref(r health.Resource) string {
	data, _ := json.Marshal(r)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:12])
}

func anchor(o health.Object, objects map[string]health.Object) health.Resource {
	if o.Kind == "EndpointSlice" {
		for _, service := range objects {
			if health.MatchesServiceSlice(service, o) {
				return service.Resource
			}
		}
		return o.Resource
	}
	for depth := 0; depth < 3 && o.Owner.UID != ""; depth++ {
		parent, ok := objects[Ref(o.Owner)]
		if !ok {
			return o.Owner
		}
		o = parent
	}
	return o.Resource
}

// Inputs groups internal signals by verified controller identity. It does not
// decide whether any group merits a user-facing incident.
func Inputs(snapshot health.Snapshot, policy health.Policy, open []Record, limit int) (map[string]Input, error) {
	objects := map[string]health.Object{}
	for _, o := range snapshot.Objects {
		objects[Ref(o.Resource)] = o
	}
	out := map[string]Input{}
	ensure := func(a health.Resource) Input {
		key := Ref(a)
		in, ok := out[key]
		if !ok {
			in = Input{Anchor: key, ObservedAt: snapshot.At, Resources: []Resource{}, Signals: []Signal{}, Incidents: []Existing{},
				Identities: map[string]health.Resource{}, Objects: map[string]health.Object{}, ToolVersions: map[string]string{}}
		}
		return in
	}
	add := func(in *Input, r health.Resource) {
		key := Ref(r)
		if _, ok := in.Identities[key]; ok {
			return
		}
		in.Identities[key] = r
		in.Resources = append(in.Resources, Resource{key, r.Kind, r.Namespace, r.Name})
		if o, ok := objects[key]; ok {
			in.Objects[key] = o
		}
	}
	for _, f := range health.Evaluate(snapshot, policy) {
		o, ok := objects[Ref(f.Resource)]
		if !ok && f.Resource.UID == "" {
			// An Event target without UID cannot establish a resource incarnation.
			for _, candidate := range snapshot.Objects {
				if candidate.Kind == f.Resource.Kind && candidate.Namespace == f.Resource.Namespace && candidate.Name == f.Resource.Name {
					o, ok = candidate, true
					break
				}
			}
		}
		if !ok {
			continue
		}
		a := anchor(o, objects)
		in := ensure(a)
		add(&in, a)
		add(&in, o.Resource)
		in.Signals = append(in.Signals, Signal{f.ID, f.Rule, f.Severity, Ref(o.Resource), f.Summary, f.Evidence})
		out[in.Anchor] = in
	}
	for _, existing := range open {
		in := ensure(existing.Value.Identity.Anchor)
		add(&in, existing.Value.Identity.Anchor)
		for _, r := range existing.Value.Resources {
			add(&in, r)
		}
		out[in.Anchor] = in
	}
	if len(out) > limit {
		return nil, fmt.Errorf("investigation candidate limit exceeded")
	}
	for key, in := range out {
		// Include bounded known owner/replica context without adding unrelated scopes.
		for _, o := range snapshot.Objects {
			if anchor(o, objects) == in.Identities[in.Anchor] {
				add(&in, o.Resource)
			}
		}
		if service, ok := objects[in.Anchor]; ok && service.Kind == "Service" {
			addPod := func(pod health.Object) {
				add(&in, pod.Resource)
				for depth := 0; depth < 3 && pod.Owner.UID != ""; depth++ {
					parent, ok := objects[Ref(pod.Owner)]
					if !ok {
						break
					}
					add(&in, parent.Resource)
					pod = parent
				}
			}
			for _, o := range snapshot.Objects {
				if health.MatchesServicePod(service, o) {
					addPod(o)
				}
				if health.MatchesServiceSlice(service, o) {
					add(&in, o.Resource)
					for _, endpoint := range o.Endpoints {
						if pod, ok := objects[Ref(endpoint.Target)]; ok && pod.Kind == "Pod" && pod.Namespace == service.Namespace {
							addPod(pod)
						}
					}
				}
			}
		}
		nodes := 0
		for _, o := range snapshot.Objects {
			if o.Kind == "Node" && nodes < 10 {
				add(&in, o.Resource)
				nodes++
			}
		}
		if len(in.Resources) > 50 {
			return nil, fmt.Errorf("related resource context exceeds investigation limit")
		}
		slices.SortFunc(in.Resources, func(a, b Resource) int { return strings.Compare(a.Ref, b.Ref) })
		in.Revision = Fingerprint(in)
		out[key] = in
	}
	return out, nil
}

func Fingerprint(in Input) string {
	type fact struct {
		Rule, Resource, Summary string
		Evidence                []string
	}
	facts := []fact{}
	seen := map[string]bool{}
	for _, s := range in.Signals {
		if s.Rule == "event.warning" || s.Rule == "pod.container.restarts" {
			evidence := []string{}
			for _, v := range s.Evidence {
				if strings.HasPrefix(v, "reason=") || strings.HasPrefix(v, "previous_reason=") {
					evidence = append(evidence, v)
				}
			}
			if len(evidence) > 0 {
				value := fact{s.Rule, s.Resource, s.Summary, evidence}
				key, _ := json.Marshal(value)
				if !seen[string(key)] {
					facts = append(facts, value)
					seen[string(key)] = true
				}
			}
			continue
		}
		facts = append(facts, fact{s.Rule, s.Resource, s.Summary, s.Evidence})
	}
	// Informational-only cases still get an initial review, but counter churn
	// does not invalidate an investigation or the no-action cache.
	slices.SortFunc(facts, func(a, b fact) int {
		aa, _ := json.Marshal(a)
		bb, _ := json.Marshal(b)
		return slices.Compare(aa, bb)
	})
	type state struct {
		Ref, Phase                                        string
		Generation, Observed, Desired, Available, Updated int64
		Containers                                        []health.Container
		Ready, Current, Misscheduled, Partition           int32
		UpdateStrategy, ServiceType, AddressType          string
		Selector                                          map[string]string
		PublishNotReady                                   bool
		Ports                                             []health.Port
		Endpoints                                         []string
	}
	states := []state{}
	for _, resource := range in.Resources {
		o, ok := in.Objects[resource.Ref]
		if !ok || o.Kind == "Node" && resource.Ref != in.Anchor {
			continue
		}
		value := state{Ref: resource.Ref, Phase: o.Phase, Generation: o.Generation, Observed: o.Observed, Desired: int64(o.Desired), Available: int64(o.Available), Updated: int64(o.Updated)}
		value.Ready, value.Current, value.Misscheduled, value.Partition = o.Ready, o.Current, o.Misscheduled, o.Partition
		value.UpdateStrategy, value.ServiceType, value.AddressType = o.UpdateStrategy, o.ServiceType, o.AddressType
		value.Selector, value.PublishNotReady = o.Selector, o.PublishNotReady
		value.Ports = slices.Clone(o.Ports)
		slices.SortFunc(value.Ports, func(a, b health.Port) int {
			aa, _ := json.Marshal(a)
			bb, _ := json.Marshal(b)
			return slices.Compare(aa, bb)
		})
		for _, endpoint := range o.Endpoints {
			value.Endpoints = append(value.Endpoints, fmt.Sprintf("%s:%t:%t:%t", Ref(endpoint.Target), endpoint.Ready, endpoint.Serving, endpoint.Terminating))
		}
		slices.Sort(value.Endpoints)
		for _, c := range o.Containers {
			c.Restarts = 0
			value.Containers = append(value.Containers, c)
		}
		states = append(states, value)
	}
	data, _ := json.Marshal(struct {
		Facts  []fact
		States []state
	}{facts, states})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func Active(in Input) bool {
	for _, s := range in.Signals {
		if s.Hint == "warning" || s.Hint == "critical" {
			return true
		}
	}
	return false
}
