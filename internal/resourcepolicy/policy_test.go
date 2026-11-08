/*
Purpose: Verify strict extra-resource policies, bounded CEL and approved status projection.
Flow: Offline YAML and synthetic status fixtures exercise certificate readiness/expiry,
missing facts, malformed data, credential exclusion and execution limits without API or model calls.
*/
package resourcepolicy

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// certificatePolicy loads the shipped opt-in example so fixture tests validate the actual operator configuration.
func certificatePolicy(t *testing.T) Resource {
	t.Helper()

	resources, err := Load("../../examples/extra-resources.yaml")
	if err != nil {
		t.Fatal(err)
	}

	return resources[0]
}

// TestCertificateRules covers exact startup/expiry boundaries, missing status and deletion exclusions.
func TestCertificateRules(t *testing.T) {
	r := certificatePolicy(t)
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)

	for _, tt := range []struct {
		name, readiness, expiry string
		age                     time.Duration
		deleting                bool
		want                    []string
	}{
		{name: "healthy", readiness: "True", expiry: "169h", age: time.Hour},
		{name: "seven day boundary", readiness: "True", expiry: "168h", age: time.Hour, want: []string{"expires-soon"}},
		{name: "expiry boundary", readiness: "True", expiry: "0s", age: time.Hour, want: []string{"expired"}},
		{name: "expired not ready", readiness: "False", expiry: "-1h", age: time.Hour, want: []string{"not-ready", "expired"}},
		{name: "startup", readiness: "False", age: 299 * time.Second},
		{name: "startup boundary", readiness: "False", age: 300 * time.Second, want: []string{"not-ready"}},
		{name: "unknown", readiness: "Unknown", age: time.Hour, want: []string{"not-ready"}},
		{name: "missing status", age: time.Hour, want: []string{"not-ready"}},
		{name: "deleting", readiness: "False", expiry: "-1h", age: time.Hour, deleting: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status := map[string]any{}
			if tt.readiness != "" {
				status["conditions"] = []any{map[string]any{"type": "Ready", "status": tt.readiness}}
			}

			if tt.expiry != "" {
				duration, err := time.ParseDuration(tt.expiry)
				if err != nil {
					t.Fatal(err)
				}

				status["notAfter"] = now.Add(duration).Format(time.RFC3339)
			}

			facts, err := r.Project(map[string]any{"status": status})
			if err != nil {
				t.Fatal(err)
			}

			rules, cost, err := r.Evaluate(t.Context(), facts, now, now.Add(-tt.age), tt.deleting)
			if err != nil || cost == 0 || cost > MaxRuleCost*3 {
				t.Fatalf("evaluation cost=%d err=%v", cost, err)
			}

			got := []string{}
			for _, rule := range rules {
				got = append(got, rule.ID)
			}

			if !slices.Equal(got, tt.want) {
				t.Fatalf("rules=%v want=%v", got, tt.want)
			}
		})
	}
}

// TestStrictPolicyValidation rejects unsafe API/field selection, malformed metadata and nonboolean or unbounded expressions.
func TestStrictPolicyValidation(t *testing.T) {
	data, err := os.ReadFile("../../examples/extra-resources.yaml")
	if err != nil {
		t.Fatal(err)
	}

	base := string(data)

	for _, tt := range []struct {
		name, from, to string
	}{
		{"version", "version: 1", "version: 2"},
		{"unknown key", "scope: Namespaced", "scope: Namespaced\n    unexpected: true"},
		{"duplicate key", "scope: Namespaced", "scope: Namespaced\n    scope: Cluster"},
		{"scope", "scope: Namespaced", "scope: Auto"},
		{"secret kind", "kind: Certificate", "kind: Secret"},
		{"configmap", "resource: certificates", "resource: configmaps"},
		{"builtin", "kind: Certificate", "kind: Pod"},
		{"incident", "group: cert-manager.io", "group: kubescout.benc-uk.github.com"},
		{"authorization", "group: cert-manager.io", "group: authorization.k8s.io"},
		{"wildcard", "resource: certificates", "resource: '*'"},
		{"subresource", "resource: certificates", "resource: certificates/status"},
		{"raw spec", "[status, conditions]", "[spec, conditions]"},
		{"whole status", "[status, conditions]", "[status]"},
		{"credential path", "[status, notAfter]", "[status, privateKey]"},
		{"unknown type", "type: timestamp", "type: object"},
		{"duplicate field", "name: notAfter", "name: conditions"},
		{"duplicate rule", "id: expired", "id: not-ready"},
		{"severity", "severity: critical", "severity: high"},
		{"not boolean", `!deleting && "notAfter" in fields && timestamp(fields.notAfter) <= now`, `"not boolean"`},
		{"raw object variable", `!deleting && "notAfter" in fields && timestamp(fields.notAfter) <= now`, `object.status.ready == true`},
		{"large expression", `!deleting && "notAfter" in fields && timestamp(fields.notAfter) <= now`, strings.Repeat("true && ", 700) + "true"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Parse(strings.Replace(base, tt.from, tt.to, 1)); err == nil {
				t.Fatal("invalid policy accepted")
			}
		})
	}

	for _, data := range []string{"", base + "\n---\nversion: 1", strings.Repeat(" ", MaxFileBytes+1)} {
		if _, err := Parse(data); err == nil {
			t.Fatal("invalid document accepted")
		}
	}

	if _, err := Load("missing-policy.yaml"); err == nil {
		t.Fatal("missing policy accepted")
	}
}

// TestStatusProjection protects excluded data and rejects missing required fields, malformed types and exact size overruns.
func TestStatusProjection(t *testing.T) {
	r := certificatePolicy(t)
	raw := map[string]any{
		"metadata": map[string]any{"uid": "private-uid", "annotations": map[string]any{"key": "private-annotation"}},
		"spec":     map[string]any{"secretName": "private-secret", "data": "private-data"},
		"status": map[string]any{"unused": "private-unused", "conditions": []any{
			map[string]any{"type": "Ready", "status": "False", "reason": "token=private-token",
				"message": "private-message", "credentials": "private-credentials"},
		}},
	}

	facts, err := r.Project(raw)
	if err != nil {
		t.Fatal(err)
	}

	data, _ := json.Marshal(facts)
	if strings.Contains(string(data), "private-") || !strings.Contains(string(data), "[redacted credential]") {
		t.Fatalf("private data reached facts: %s", data)
	}

	for _, status := range []map[string]any{
		{"notAfter": "invalid-time"},
		{"notAfter": true},
		{"conditions": []any{map[string]any{"type": "Ready"}}},
		{"conditions": []any{map[string]any{"type": "Ready", "status": strings.Repeat("a", 513)}}},
		{"conditions": make([]any, 33)},
	} {
		if _, err := r.Project(map[string]any{"status": status}); err == nil {
			t.Fatal("malformed or oversized status accepted")
		}
	}

	r.Fields = []Field{{Name: "phase", Path: []string{"status", "phase"}, Type: "string"}}
	if _, err := r.Project(map[string]any{}); err == nil {
		t.Fatal("missing required field became empty evidence")
	}

	r.Fields[0].Type = "strings"
	if _, err := r.Project(map[string]any{"status": map[string]any{"phase": []any{
		strings.Repeat("x", 512), strings.Repeat("x", 512), strings.Repeat("x", 512), strings.Repeat("x", 512),
		strings.Repeat("x", 512), strings.Repeat("x", 512), strings.Repeat("x", 512), strings.Repeat("x", 512),
	}}}); err == nil {
		t.Fatal("facts exceeding 4 KiB accepted")
	}
}

// TestCELEvaluationFailures verifies runtime errors, cancellation and expensive comprehensions cannot report no findings.
func TestCELEvaluationFailures(t *testing.T) {
	base := `version: 1
resources:
  - id: tests
    group: example.io
    version: v1
    resource: widgets
    kind: Widget
    scope: Cluster
    fields:
      - name: values
        path: [status, values]
        type: strings
    rules:
      - id: test
        severity: warning
        summary: Test check.
        expression: `

	for _, expression := range []string{
		"fields.missing == true",
		"fields.values.all(a, fields.values.all(b, fields.values.all(c, fields.values.all(d, a == b && c == d))))",
	} {
		rules, err := Parse(base + expression)
		if err != nil {
			t.Fatal(err)
		}

		values := make([]any, 32)
		for i := range values {
			values[i] = "same"
		}

		_, _, err = rules[0].Evaluate(t.Context(), map[string]any{"values": values}, time.Now(), time.Now(), false)
		if err == nil {
			t.Fatal("missing facts or excessive evaluation cost became a healthy result")
		}
	}

	rules, err := Parse(base + "fields.values.all(a, a == 'same')")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, _, err = rules[0].Evaluate(ctx, map[string]any{"values": []any{"same"}}, time.Now(), time.Now(), false)
	if err == nil {
		t.Fatal("canceled evaluation succeeded")
	}
}

// TestScalarProjectionTypes verifies typed numeric/boolean facts and known credential exclusion without defaults.
func TestScalarProjectionTypes(t *testing.T) {
	for _, tt := range []struct {
		kind  string
		value any
		valid bool
	}{
		{"boolean", true, true}, {"boolean", "true", false},
		{"integer", int64(7), true}, {"integer", float64(7), false},
		{"number", int64(7), true}, {"number", float64(7.5), true}, {"number", "7.5", false},
		{"string", strings.Repeat("x", 512), true}, {"string", strings.Repeat("x", 513), false},
		{"string", map[string]any{"raw": true}, false},
	} {
		t.Run(tt.kind+":"+fmt.Sprint(tt.valid)+":"+fmt.Sprintf("%T", tt.value), func(t *testing.T) {
			_, err := projectValue(tt.value, tt.kind, nil)
			if (err == nil) != tt.valid {
				t.Fatalf("declared type=%s input=%T error=%v", tt.kind, tt.value, err)
			}
		})
	}

	value, err := projectValue("controller said fixture-key", "string", []string{"fixture-key"})
	if err != nil || strings.Contains(value.(string), "fixture-key") {
		t.Fatal("known credential reached the projected scalar")
	}
}
