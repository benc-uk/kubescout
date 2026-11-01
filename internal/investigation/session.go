/*
Purpose: Share investigation tool validation, budgets, redaction and diagnostics.
Flow: Both local tools resolve model-supplied handles through private identities
before scoped reads, including cluster-scoped PVs and attachments; one session enforces call, byte and timeout limits and
registers operation-named citations only for accepted evidence. Logs distinguish
AI requests from Go execution without publishing incidents or raw objects.
*/
package investigation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/output"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// Session holds the shared scope, budgets and evidence registry for one investigation.
type Session struct {
	Client        dynamic.Interface
	Investigation incident.InvestigationContext
	Timeout       time.Duration
	Calls, Bytes  int
	MaxCalls      int
	Citations     map[string]bool
	Scope         config.Config
	Log           *output.Writer
	Transcript    *output.Transcript
}

// LimitError describes a local tool budget rejection without arguments or evidence.
type LimitError struct {
	Kind            string
	Observed, Limit int
}

// Error reports only a fixed budget category and numeric usage.
func (e *LimitError) Error() string {
	return fmt.Sprintf("investigation limit exceeded: %s (observed=%d, limit=%d)", e.Kind, e.Observed, e.Limit)
}

// resourceRefSchema permits exactly one resource handle and excludes arbitrary API parameters.
func resourceRefSchema() any {
	return map[string]any{"type": "object", "properties": map[string]any{"resource_ref": map[string]any{"type": "string"}},
		"required": []string{"resource_ref"}, "additionalProperties": false}
}

var resources = map[string]schema.GroupVersionResource{
	"Node": {Version: "v1", Resource: "nodes"}, "Pod": {Version: "v1", Resource: "pods"},
	"Deployment":              {Group: "apps", Version: "v1", Resource: "deployments"},
	"ReplicaSet":              {Group: "apps", Version: "v1", Resource: "replicasets"},
	"Job":                     {Group: "batch", Version: "v1", Resource: "jobs"},
	"CronJob":                 {Group: "batch", Version: "v1", Resource: "cronjobs"},
	"HorizontalPodAutoscaler": {Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"},
	"Ingress":                 {Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"},
	"StatefulSet":             {Group: "apps", Version: "v1", Resource: "statefulsets"},
	"DaemonSet":               {Group: "apps", Version: "v1", Resource: "daemonsets"},
	"Service":                 {Version: "v1", Resource: "services"},
	"EndpointSlice":           {Group: "discovery.k8s.io", Version: "v1", Resource: "endpointslices"},
	"PersistentVolumeClaim":   {Version: "v1", Resource: "persistentvolumeclaims"},
	"PersistentVolume":        {Version: "v1", Resource: "persistentvolumes"},
	"VolumeAttachment":        {Group: "storage.k8s.io", Version: "v1", Resource: "volumeattachments"},
}

// call validates a scoped handle before invoking a tool's read under shared budgets.
// Only bounded successful results receive citations and approved transcript entries.
func (s *Session) call(ctx context.Context, name, args string, read func(context.Context, string, health.Resource, map[string]any) error) (value any, callErr error) {
	scope := output.Scope(ctx)

	start := time.Now()
	defer func() {
		message := fmt.Sprintf("%s %s duration=%s", scope, name, time.Since(start).Round(time.Millisecond))
		if callErr != nil {
			message += " rejected/failed: " + callErr.Error()
			_ = s.Log.Log("warn", "GO", "TOOL.EXECUTE", message)
			s.Transcript.Record(scope, "GO", "TOOL.FAILED", message)
		} else {
			_ = s.Log.Log("info", "GO", "TOOL.EXECUTE", message+" completed; approved evidence returned")

			data, err := json.MarshalIndent(value, "", "  ")
			if err != nil {
				s.Transcript.Record(scope, "GO", "TOOL.RESULT", "[approved result serialization failed]")
			} else {
				s.Transcript.Record(scope, "GO", "TOOL.RESULT", string(data))
			}
		}
	}()

	if s.Client == nil {
		return nil, errors.New("investigation Kubernetes client unavailable")
	}

	if len(args) > 1024 {
		return nil, &LimitError{Kind: "local_tool_arguments_limit", Observed: len(args), Limit: 1024}
	}

	if s.MaxCalls <= 0 {
		return nil, errors.New("investigation tool-call limit must be positive")
	}

	if s.Calls >= s.MaxCalls {
		return nil, &LimitError{Kind: "local_tool_call_limit", Observed: s.Calls + 1, Limit: s.MaxCalls}
	}

	d := json.NewDecoder(strings.NewReader(args))

	open, err := d.Token()
	if err != nil || open != json.Delim('{') {
		return nil, errors.New("invalid tool arguments")
	}

	key, err := d.Token()
	if err != nil || key != "resource_ref" {
		return nil, errors.New("invalid tool argument name")
	}

	var ref string
	if d.Decode(&ref) != nil {
		return nil, errors.New("invalid resource handle")
	}

	close, err := d.Token()
	if err != nil || close != json.Delim('}') {
		return nil, errors.New("extra or duplicate tool arguments")
	}

	if _, err := d.Token(); err != io.EOF {
		return nil, errors.New("trailing tool arguments")
	}

	r, ok := s.Investigation.Identities[ref]

	_, known := resources[r.Kind]
	if !ok || !known {
		return nil, errors.New("unauthorised tool resource")
	}

	if health.ClusterScoped(r.Kind) {
		if r.Namespace != "" {
			return nil, errors.New("unauthorised cluster resource namespace")
		}
	} else if !s.Scope.WatchesNamespace(r.Namespace) {
		return nil, errors.New("tool resource outside namespace scope")
	}

	s.Calls++
	_ = s.Log.Log("info", "AI", "TOOL.REQUEST", fmt.Sprintf("%s tool=%d %s for %s %s/%s", scope, s.Calls, name, r.Kind, r.Namespace, r.Name))
	s.Transcript.Record(scope, "AI", "TOOL.REQUEST", fmt.Sprintf("tool=%d name=%s\nValidated resource_ref: %s\nResource: %s %s/%s",
		s.Calls, name, ref, r.Kind, r.Namespace, r.Name))

	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	result := map[string]any{"resource_ref": ref, "observed_at": time.Now().UTC(), "complete": true}
	if err := read(ctx, ref, r, result); err != nil {
		return nil, err
	}

	id := fmt.Sprintf("%s-%d", strings.ReplaceAll(name, "_", "-"), s.Calls)
	result["evidence_id"] = id

	data, err := json.Marshal(result)
	if err != nil {
		return nil, errors.New("investigation evidence serialization failed")
	}

	if len(data) > 8192 {
		return nil, &LimitError{Kind: "local_tool_result_bytes", Observed: len(data), Limit: 8192}
	}

	if s.Bytes+len(data) > 32768 {
		return nil, &LimitError{Kind: "local_cumulative_tool_bytes", Observed: s.Bytes + len(data), Limit: 32768}
	}

	s.Bytes += len(data)
	s.Citations[id] = true

	return result, nil
}

// A regular expression to match sensitive information patterns in strings.
var sensitive = regexp.MustCompile(`(?i)(authorization|bearer|password|passwd|api[_-]?key|secret|token|://[^/\s]+:[^@\s]+@)`)

// redactValue recursively replaces strings matching sensitive patterns and truncates long strings.
// Other scalar values pass through; arbitrary text is not guaranteed secret-free.
func redactValue(v any) any {
	switch value := v.(type) {
	case string:
		if sensitive.MatchString(value) {
			return "[redacted sensitive text]"
		}

		if len(value) > 1024 {
			return value[:1024] + " [truncated]"
		}

		return value
	case map[string]any:
		out := map[string]any{}
		for k, v := range value {
			out[k] = redactValue(v)
		}

		return out
	case []any:
		out := make([]any, len(value))
		for i, v := range value {
			out[i] = redactValue(v)
		}

		return out
	default:
		return v
	}
}
