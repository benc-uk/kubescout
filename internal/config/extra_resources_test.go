/*
Purpose: Test opt-in startup policy loading and configured resource scope.
Flow: Offline YAML files and synthetic model settings verify flag/environment
precedence, invalid configuration and scope removal without cluster or AI access.
*/
package config

import (
	"io"
	"slices"
	"testing"

	"github.com/benc-uk/kubescout/internal/health"
)

// TestExtraResourceConfiguration checks startup compilation, environment defaults and explicit disable/override.
func TestExtraResourceConfiguration(t *testing.T) {
	base := []string{"--auth-mode=kubeconfig", "--observer-id=test"}
	path := "../../examples/extra-resources.yaml"
	env := func(key string) string {
		if key == "EXTRA_RESOURCES_FILE" {
			return path
		}

		return testEnv(key)
	}

	for _, tt := range []struct {
		name string
		args []string
		want int
		fail bool
	}{
		{name: "environment", want: 1},
		{name: "flag", args: []string{"--extra-resources=" + path}, want: 1},
		{name: "disable environment", args: []string{"--extra-resources="}},
		{name: "missing file", args: []string{"--extra-resources=missing.yaml"}, fail: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, err := Parse(append(slices.Clone(base), tt.args...), env, io.Discard)
			if (err != nil) != tt.fail || err == nil && len(c.Policy.ExtraResources) != tt.want {
				t.Fatalf("resources=%d err=%v", len(c.Policy.ExtraResources), err)
			}
		})
	}

	c, err := Parse(base, env, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	c.NamespaceWatch = []string{"apps"}

	r := health.Resource{Kind: "Certificate", APIGroup: "cert-manager.io", Namespace: "apps"}
	if !c.ResourceInScope(r) {
		t.Fatal("configured namespaced resource rejected")
	}

	r.Namespace = "other"
	if c.ResourceInScope(r) {
		t.Fatal("extra resource escaped namespace scope")
	}

	c.Policy.ExtraResources[0].Scope = "Cluster"

	r.Namespace = ""
	if !c.ResourceInScope(r) {
		t.Fatal("configured cluster resource rejected")
	}

	c.Policy.ExtraResources = nil
	if c.ResourceInScope(r) {
		t.Fatal("removed policy remained in observation scope")
	}

	r.APIGroup = ""

	r.Namespace = "apps"
	if c.ResourceInScope(r) {
		t.Fatal("unknown core resource remained in observation scope")
	}
}
