/*
Purpose: Build correlated investigation contexts and evidence fingerprints.
Flow: Projected snapshots, deterministic signals and unresolved records become
bounded resource groups for AI review, including recovery candidates; identity
and activity hints schedule investigation rather than decide incident changes.
Verified PVC/PV bindings and Pod claim use add context without changing anchors.
CronJob ownership groups scheduled runs while preserving unresolved legacy Job anchors.
HPA scale targets add workload context without becoming ownership or changing anchors.
Ingress routes share Service/backend context without duplicating endpoint evidence or changing anchors.
VolumeAttachments share named current PV/Node and scoped claim context without changing incident ownership.
*/
package incident

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
)

// Ref hashes a resource identity into a stable handle without exposing its raw UID.
func Ref(r health.Resource) string {
	data, _ := json.Marshal(r)
	sum := sha256.Sum256(data)

	return hex.EncodeToString(sum[:12])
}

// anchor correlates EndpointSlices with verified Services or follows up to three owner links.
// Unresolved legacy Job anchors remain stable; absent owners do not revert to their children.
func anchor(o health.Object, objects map[string]health.Object, jobAnchors map[string]bool) health.Resource {
	if o.Kind == "EndpointSlice" {
		for _, service := range objects {
			if health.MatchesServiceSlice(service, o) {
				return service.Resource
			}
		}

		return o.Resource
	}

	for depth := 0; depth < 3 && o.Owner.UID != ""; depth++ {
		if o.Kind == "Job" && jobAnchors[Ref(o.Resource)] {
			break
		}

		parent, ok := objects[Ref(o.Owner)]
		if !ok {
			return o.Owner
		}

		o = parent
	}

	return o.Resource
}

// BuildInvestigationContexts groups signals by verified controller identity and retains
// unresolved records for recovery review, enforcing candidate and related-resource limits.
// It does not decide whether any group merits a user-facing incident.
func BuildInvestigationContexts(snapshot health.Snapshot, policy health.Policy, open []Record, limit int) (map[string]InvestigationContext, error) {
	objects := map[string]health.Object{}
	for _, o := range snapshot.Objects {
		objects[Ref(o.Resource)] = o
	}

	jobAnchors := map[string]bool{}

	for _, existing := range open {
		if a := existing.Value.Identity.Anchor; a.Kind == "Job" {
			jobAnchors[Ref(a)] = true
		}
	}

	out := map[string]InvestigationContext{}
	ensure := func(a health.Resource) InvestigationContext {
		key := Ref(a)

		in, ok := out[key]
		if !ok {
			in = InvestigationContext{Anchor: key, ObservedAt: snapshot.At, Resources: []Resource{}, Signals: []Signal{}, Incidents: []Existing{},
				Identities: map[string]health.Resource{}, Objects: map[string]health.Object{}, ToolVersions: map[string]string{}}
		}

		return in
	}
	add := func(in *InvestigationContext, r health.Resource) {
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

		a := anchor(o, objects, jobAnchors)
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
			if anchor(o, objects, jobAnchors) == in.Identities[in.Anchor] {
				add(&in, o.Resource)
			}
		}

		if job, ok := objects[in.Anchor]; ok && job.Kind == "Job" && job.Owner.Kind == "CronJob" {
			if cron, ok := objects[Ref(job.Owner)]; ok {
				add(&in, cron.Resource)
			}
		}

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

		services := map[string]health.Object{}

		if service, ok := objects[in.Anchor]; ok && service.Kind == "Service" {
			services[in.Anchor] = service
		}

		for _, related := range slices.Clone(in.Resources) {
			o, ok := objects[related.Ref]
			if !ok {
				continue
			}

			for _, candidate := range snapshot.Objects {
				if health.MatchesIngressService(candidate, o) {
					add(&in, candidate.Resource)
				}

				if health.MatchesIngressService(o, candidate) {
					add(&in, candidate.Resource)
					services[Ref(candidate.Resource)] = candidate
				}
			}
		}

		for _, service := range services {
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

		for _, related := range slices.Clone(in.Resources) {
			o, ok := objects[related.Ref]
			if !ok {
				continue
			}

			for _, candidate := range snapshot.Objects {
				if health.MatchesHPATarget(candidate, o) {
					add(&in, candidate.Resource)
				}

				if health.MatchesHPATarget(o, candidate) {
					add(&in, candidate.Resource)
					targetAnchor := anchor(candidate, objects, jobAnchors)

					for _, child := range snapshot.Objects {
						if anchor(child, objects, jobAnchors) == targetAnchor {
							add(&in, child.Resource)
						}
					}
				}
			}
		}

		addAttachment := func(attachment health.Object) {
			add(&in, attachment.Resource)

			for _, target := range snapshot.Objects {
				if health.MatchesAttachmentVolume(attachment, target) || health.MatchesAttachmentNode(attachment, target) {
					add(&in, target.Resource)
				}
			}
		}
		addAttachments := func() {
			for _, related := range slices.Clone(in.Resources) {
				o, ok := objects[related.Ref]
				if !ok {
					continue
				}

				if o.Kind == "VolumeAttachment" {
					addAttachment(o)
				}

				for _, attachment := range snapshot.Objects {
					if health.MatchesAttachmentVolume(attachment, o) ||
						related.Ref == in.Anchor && health.MatchesAttachmentNode(attachment, o) {
						addAttachment(attachment)
					}
				}
			}
		}
		addAttachments()

		claims := map[string]health.Object{}

		for _, related := range in.Resources {
			o, ok := objects[related.Ref]
			if !ok {
				continue
			}

			for _, claim := range snapshot.Objects {
				if claim.Kind == "PersistentVolumeClaim" && (o.Resource == claim.Resource ||
					health.MatchesClaimPod(claim, o) || health.MatchesVolumeClaim(o, claim)) {
					claims[Ref(claim.Resource)] = claim
				}
			}
		}

		a := in.Identities[in.Anchor]
		storageAnchor := a.Kind == "PersistentVolumeClaim" || a.Kind == "PersistentVolume" || a.Kind == "VolumeAttachment"

		for _, claim := range claims {
			add(&in, claim.Resource)

			for _, o := range snapshot.Objects {
				if health.MatchesVolumeClaim(o, claim) {
					add(&in, o.Resource)
				}

				if storageAnchor && health.MatchesClaimPod(claim, o) {
					addPod(o)
				}
			}
		}

		addAttachments()

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

// Fingerprint hashes semantic signals and projected state for stale-result and no-action checks.
// Restart counters, event count churn and non-anchor Node state do not affect the hash.
func Fingerprint(in InvestigationContext) string {
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
		Storage                                           health.Storage
		VolumeName, ClaimRef                              string
		Claims                                            []string
		StorageConditions                                 []health.Condition
		Deleting                                          bool
		CronJob                                           health.CronJobState
		HPA                                               health.HPAState
		HPAConditions                                     []health.Condition
		Ingress                                           health.IngressState
		Attachment                                        health.AttachmentState
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
		value.CronJob = o.CronJob
		value.Attachment = o.Attachment
		value.Attachment.AttachError.At = time.Time{}
		value.Attachment.DetachError.At = time.Time{}
		value.Attachment.DeletingSince = time.Time{}

		if o.Kind == "VolumeAttachment" {
			value.Deleting = o.Deleting
		}

		value.Ingress = o.Ingress
		value.Ingress.Routes = slices.Clone(o.Ingress.Routes)
		slices.SortFunc(value.Ingress.Routes, func(a, b health.IngressRoute) int {
			aa, _ := json.Marshal(a)
			bb, _ := json.Marshal(b)

			return slices.Compare(aa, bb)
		})

		if o.Kind == "Ingress" {
			value.Deleting = o.Deleting
		}

		value.HPA = o.HPA.Clone()
		value.HPA.CurrentMetrics, value.HPA.LastScale = nil, nil
		slices.SortFunc(value.HPA.Metrics, func(a, b health.Metric) int {
			aa, _ := json.Marshal(a)
			bb, _ := json.Marshal(b)

			return slices.Compare(aa, bb)
		})

		for _, rules := range []*health.ScalingRules{value.HPA.ScaleUp, value.HPA.ScaleDown} {
			if rules != nil {
				slices.SortFunc(rules.Policies, func(a, b health.ScalingPolicy) int {
					aa, _ := json.Marshal(a)
					bb, _ := json.Marshal(b)

					return slices.Compare(aa, bb)
				})
			}
		}

		if o.Kind == "HorizontalPodAutoscaler" {
			value.Deleting = o.Deleting
			for _, c := range o.Conditions {
				value.HPAConditions = append(value.HPAConditions, health.Condition{Type: c.Type, Status: c.Status, Reason: c.Reason})
			}

			slices.SortFunc(value.HPAConditions, func(a, b health.Condition) int {
				aa, _ := json.Marshal(a)
				bb, _ := json.Marshal(b)

				return slices.Compare(aa, bb)
			})
		}

		value.Storage, value.VolumeName, value.ClaimRef = o.Storage, o.Storage.VolumeName, Ref(o.Storage.Claim)
		value.Storage.AccessModes = slices.Clone(o.Storage.AccessModes)
		slices.Sort(value.Storage.AccessModes)
		value.Claims = slices.Clone(o.Claims)
		slices.Sort(value.Claims)

		if o.Kind == "PersistentVolumeClaim" || o.Kind == "PersistentVolume" {
			value.Deleting = o.Deleting
			for _, c := range o.Conditions {
				value.StorageConditions = append(value.StorageConditions, health.Condition{Type: c.Type, Status: c.Status, Reason: c.Reason})
			}

			slices.SortFunc(value.StorageConditions, func(a, b health.Condition) int {
				aa, _ := json.Marshal(a)
				bb, _ := json.Marshal(b)

				return slices.Compare(aa, bb)
			})
		}

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

// Active reports warning or critical scheduling hints, not an AI incident decision.
func Active(in InvestigationContext) bool {
	for _, s := range in.Signals {
		if s.Hint == "warning" || s.Hint == "critical" {
			return true
		}
	}

	return false
}
