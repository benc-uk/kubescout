/*
Purpose: Load explicit extra-resource policies and compile bounded local CEL checks.
Flow: Strict startup YAML selects API resources and approved status facts; boolean
expressions produce private evidence hints only, never API calls or Incident actions.
*/
package resourcepolicy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"go.yaml.in/yaml/v3"
	"k8s.io/apimachinery/pkg/util/validation"
)

type Document struct {
	Version   int        `yaml:"version"`
	Resources []Resource `yaml:"resources"`
}

type Resource struct {
	ID       string  `yaml:"id"`
	Group    string  `yaml:"group"`
	Version  string  `yaml:"version"`
	Resource string  `yaml:"resource"`
	Kind     string  `yaml:"kind"`
	Scope    string  `yaml:"scope"`
	Fields   []Field `yaml:"fields"`
	Rules    []Rule  `yaml:"rules"`
}

type Field struct {
	Name     string   `yaml:"name"`
	Path     []string `yaml:"path"`
	Type     string   `yaml:"type"`
	Optional bool     `yaml:"optional"`
}

type Rule struct {
	ID         string `yaml:"id"`
	Expression string `yaml:"expression"`
	Severity   string `yaml:"severity"`
	Summary    string `yaml:"summary"`
	program    cel.Program
}

type EvaluationError struct {
	ResourceID, RuleID, Reason string
}

const MaxFileBytes = 64 << 10
const MaxFactsBytes = 4 << 10
const MaxRuleCost = 10000
const MaxSnapshotCost = 1000000

var identifier = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,62}$`)

// Error exposes trusted rule identifiers and a fixed local failure category, never raw status or CEL error text.
func (e *EvaluationError) Error() string {
	return fmt.Sprintf("extra-resource evaluation unavailable: resource=%s rule=%s reason=%s", e.ResourceID, e.RuleID, e.Reason)
}

// Load reads one bounded local YAML document and validates every resource before any API or AI work.
func Load(path string) ([]Resource, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot open extra-resource policy file")
	}
	defer f.Close()

	data, err := io.ReadAll(io.LimitReader(f, MaxFileBytes+1))
	if err != nil || len(data) > MaxFileBytes {
		return nil, errors.New("extra-resource policy file is unreadable or exceeds 64 KiB")
	}

	return Parse(string(data))
}

// Parse rejects unknown/duplicate YAML fields, extra documents and invalid policies, then compiles CEL once.
func Parse(data string) ([]Resource, error) {
	if len(data) > MaxFileBytes {
		return nil, errors.New("extra-resource policy exceeds 64 KiB")
	}

	decoder := yaml.NewDecoder(strings.NewReader(data))
	decoder.KnownFields(true)

	var doc Document
	if err := decoder.Decode(&doc); err != nil {
		return nil, errors.New("invalid extra-resource YAML (unknown, duplicate or malformed fields)")
	}

	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("extra-resource policy must contain exactly one YAML document")
	}

	if doc.Version != 1 || len(doc.Resources) == 0 || len(doc.Resources) > 16 {
		return nil, errors.New("extra-resource policy requires version 1 and between 1 and 16 resources")
	}

	env, err := cel.NewEnv(cel.Variable("fields", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("now", cel.TimestampType), cel.Variable("created", cel.TimestampType),
		cel.Variable("deleting", cel.BoolType), cel.ParserExpressionSizeLimit(4096),
		cel.ParserRecursionLimit(64))
	if err != nil {
		return nil, errors.New("cannot initialize extra-resource CEL environment")
	}

	ids, apis, kinds := map[string]bool{}, map[string]bool{}, map[string]bool{}

	for i := range doc.Resources {
		r := &doc.Resources[i]
		if err := r.validate(); err != nil {
			return nil, fmt.Errorf("extra resource %d: %w", i+1, err)
		}

		api, kind := r.Group+"/"+r.Resource, r.Group+"/"+r.Kind
		if ids[r.ID] || apis[api] || kinds[kind] {
			return nil, errors.New("extra-resource IDs, group/resources and group/kinds must be unique")
		}

		ids[r.ID], apis[api], kinds[kind] = true, true, true
		ruleIDs := map[string]bool{}

		for j := range r.Rules {
			rule := &r.Rules[j]
			if len(validation.IsDNS1123Label(rule.ID)) != 0 || ruleIDs[rule.ID] ||
				!slices.Contains([]string{"info", "warning", "critical"}, rule.Severity) ||
				strings.TrimSpace(rule.Summary) == "" || len(rule.Summary) > 512 {
				return nil, fmt.Errorf("extra resource %s has invalid or duplicate rule metadata", r.ID)
			}

			ruleIDs[rule.ID] = true

			ast, issues := env.Compile(rule.Expression)
			if issues.Err() != nil || ast.OutputType() != cel.BoolType {
				return nil, fmt.Errorf("extra resource %s rule %s must be a valid boolean CEL expression", r.ID, rule.ID)
			}

			rule.program, err = env.Program(ast, cel.CostLimit(MaxRuleCost), cel.InterruptCheckFrequency(100))
			if err != nil {
				return nil, fmt.Errorf("cannot compile extra resource %s rule %s", r.ID, rule.ID)
			}
		}
	}

	return doc.Resources, nil
}

// validate restricts API identities, reserved/sensitive kinds and explicitly selected bounded status fields.
func (r Resource) validate() error {
	if len(validation.IsDNS1123Label(r.ID)) != 0 || r.Group != "" && len(validation.IsDNS1123Subdomain(r.Group)) != 0 ||
		len(validation.IsDNS1035Label(r.Version)) != 0 || len(validation.IsDNS1035Label(r.Resource)) != 0 ||
		len(validation.IsDNS1035Label(strings.ToLower(r.Kind))) != 0 ||
		!slices.Contains([]string{"Namespaced", "Cluster"}, r.Scope) {
		return errors.New("invalid id, group, version, resource, kind or scope")
	}

	// Reserved kinds cannot enter built-in kind-based relationship or tool dispatch.
	if BuiltinKind(r.Kind) || slices.Contains([]string{"Secret", "ConfigMap", "ServiceAccount"}, r.Kind) ||
		slices.Contains([]string{"secrets", "configmaps", "serviceaccounts"}, r.Resource) ||
		slices.Contains([]string{"kubescout.benc-uk.github.com", "authentication.k8s.io", "authorization.k8s.io", "rbac.authorization.k8s.io"}, r.Group) {
		return errors.New("built-in, credential, authorization and Incident resources cannot be extra resources")
	}

	if len(r.Fields) == 0 || len(r.Fields) > 32 || len(r.Rules) == 0 || len(r.Rules) > 16 {
		return errors.New("each resource requires 1-32 fields and 1-16 rules")
	}

	names := map[string]bool{}

	for _, field := range r.Fields {
		if !identifier.MatchString(field.Name) || names[field.Name] || len(field.Path) < 2 ||
			len(field.Path) > 8 || field.Path[0] != "status" ||
			!slices.Contains([]string{"string", "boolean", "integer", "number", "timestamp", "strings", "conditions"}, field.Type) {
			return errors.New("fields must have unique names, a supported type and an explicit status path")
		}

		for _, segment := range field.Path {
			if !identifier.MatchString(segment) ||
				slices.Contains([]string{"uid", "annotations", "env", "data", "token", "password", "credentials", "secret", "secretKey", "privateKey"}, segment) {
				return errors.New("invalid or prohibited status path")
			}
		}

		names[field.Name] = true
	}

	return nil
}

// BuiltinKind identifies kinds whose existing typed projection and relationship semantics cannot be overridden.
func BuiltinKind(kind string) bool {
	return slices.Contains([]string{"Node", "Pod", "Deployment", "ReplicaSet", "StatefulSet", "DaemonSet",
		"Service", "EndpointSlice", "PersistentVolumeClaim", "PersistentVolume", "VolumeAttachment",
		"Job", "CronJob", "HorizontalPodAutoscaler", "Ingress", "Event"}, kind)
}

// Evaluate returns matching private rule hints and measured cost; errors never become a healthy result.
func (r Resource) Evaluate(ctx context.Context, fields map[string]any, now, created time.Time, deleting bool) ([]Rule, uint64, error) {
	matches := []Rule{}
	cost := uint64(0)

	for _, rule := range r.Rules {
		if ctx.Err() != nil {
			return nil, cost, &EvaluationError{ResourceID: r.ID, RuleID: rule.ID, Reason: "deadline_or_cancellation"}
		}

		if rule.program == nil {
			return nil, cost, &EvaluationError{ResourceID: r.ID, RuleID: rule.ID, Reason: "uncompiled_policy"}
		}

		value, details, err := rule.program.ContextEval(ctx, map[string]any{
			"fields": fields, "now": now, "created": created, "deleting": deleting,
		})
		if details != nil && details.ActualCost() != nil {
			cost += *details.ActualCost()
		}

		if err != nil || ctx.Err() != nil || value != types.True && value != types.False {
			return nil, cost, &EvaluationError{ResourceID: r.ID, RuleID: rule.ID, Reason: "invalid_facts_or_execution_limit"}
		}

		if value == types.True {
			matches = append(matches, rule)
		}
	}

	return matches, cost, nil
}
