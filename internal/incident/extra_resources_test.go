/*
Purpose: Test extra-resource correlation, private evidence and stable API-group identity.
Flow: Offline projected fixtures coalesce CEL rules, distinguish API groups, retain
recovery anchors and invalidate changed facts without creating deterministic incidents.
*/
package incident

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/resourcepolicy"
)

// TestExtraResourceContexts verifies coalescing, group-separated identity and explicit invalid-policy failures.
func TestExtraResourceContexts(t *testing.T) {
	policy := health.DefaultPolicy()

	var err error

	policy.ExtraResources, err = resourcepolicy.Load("../../examples/cert-manager.yaml")
	if err != nil {
		t.Fatal(err)
	}

	at := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	r := health.Resource{Kind: "Certificate", APIGroup: "cert-manager.io", Namespace: "apps", Name: "tls", UID: "private-uid"}
	facts := map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "False"}},
		"notAfter": at.Add(-time.Hour).Format(time.RFC3339)}
	o := health.Object{Resource: r, Created: at.Add(-time.Hour), Extra: facts}
	snapshot := health.Snapshot{At: at, Objects: []health.Object{o}}

	contexts, err := BuildInvestigationContexts(snapshot, policy, nil, 100)
	if err != nil || len(contexts) != 1 || len(contexts[Ref(r)].Signals) != 2 {
		t.Fatalf("rules were not coalesced under the certificate: %v", err)
	}

	in := contexts[Ref(r)]

	data, _ := json.Marshal(in)
	if strings.Contains(string(data), "private-uid") || !strings.Contains(string(data), "cert-manager.io") ||
		!strings.Contains(string(data), "notAfter") {
		t.Fatal("model context leaked identity or omitted approved status evidence")
	}

	other := o
	other.APIGroup = "other.example.io"

	policy.ExtraResources = append(policy.ExtraResources, policy.ExtraResources[0])
	policy.ExtraResources[1].Group = other.APIGroup
	snapshot.Objects = append(snapshot.Objects, other)

	contexts, err = BuildInvestigationContexts(snapshot, policy, nil, 100)
	if err != nil || len(contexts) != 2 || Ref(r) == Ref(other.Resource) ||
		Name(Identity{Observer: "fixture", Anchor: r, Family: "custom-resource-health"}) ==
			Name(Identity{Observer: "fixture", Anchor: other.Resource, Family: "custom-resource-health"}) {
		t.Fatal("same-named resources in different APIs shared an incident identity")
	}

	changed := o
	changed.Extra = map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}
	next := in

	next.Objects = map[string]health.Object{Ref(r): changed}
	if Fingerprint(next) == Fingerprint(in) || health.RecoveryFingerprint(changed) == health.RecoveryFingerprint(o) {
		t.Fatal("changed custom status did not invalidate AI/recovery observations")
	}

	record := Record{Value: Incident{Identity: Identity{Observer: "fixture", Anchor: r, Family: "custom-resource-health"}}}
	snapshot.Objects = []health.Object{changed}

	contexts, err = BuildInvestigationContexts(snapshot, policy, []Record{record}, 100)
	if err != nil || contexts[Ref(r)].Recovery.AnchorState != "present" || Active(contexts[Ref(r)]) {
		t.Fatal("fresh cleared custom signals did not retain the original recovery anchor")
	}

	policy.ExtraResources = nil
	if _, err := BuildInvestigationContexts(snapshot, policy, []Record{record}, 100); err == nil {
		t.Fatal("missing custom policy became a healthy recovery observation")
	}
}
