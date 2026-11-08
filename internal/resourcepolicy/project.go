/*
Purpose: Reduce configured resources to typed, bounded and credential-protected status facts.
Flow: Explicit paths select scalars or approved condition records; missing required
fields, malformed types and exceeded bounds remain errors, not empty healthy evidence.
Neither raw maps, specifications nor arbitrary condition payloads enter CEL or AI.
*/
package resourcepolicy

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
	"unicode/utf8"

	"github.com/benc-uk/kubescout/internal/output"
)

// Project reads only configured status paths and rejects incomplete or excessive projected evidence.
func (r Resource) Project(object map[string]any, secrets ...string) (map[string]any, error) {
	facts := map[string]any{}

	for _, field := range r.Fields {
		var value any = object

		found := true

		for _, segment := range field.Path {
			parent, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("extra resource %s field %s has an invalid status path", r.ID, field.Name)
			}

			value, found = parent[segment]
			if !found {
				break
			}
		}

		if !found || value == nil {
			if field.Optional {
				continue
			}

			return nil, fmt.Errorf("extra resource %s field %s is required but absent", r.ID, field.Name)
		}

		projected, err := projectValue(value, field.Type, secrets)
		if err != nil {
			return nil, fmt.Errorf("extra resource %s field %s: %w", r.ID, field.Name, err)
		}

		facts[field.Name] = projected
	}

	data, err := json.Marshal(facts)
	if err != nil || len(data) > MaxFactsBytes {
		return nil, errors.New("extra-resource status facts exceed 4 KiB or cannot be encoded")
	}

	return facts, nil
}

// projectValue enforces declared scalar/list types without returning arbitrary nested objects.
func projectValue(value any, kind string, secrets []string) (any, error) {
	switch kind {
	case "string", "timestamp":
		text, ok := value.(string)
		if !ok || len(text) > 512 || !utf8.ValidString(text) {
			return nil, errors.New("expected a UTF-8 string of at most 512 bytes")
		}

		if kind == "timestamp" {
			if _, err := time.Parse(time.RFC3339Nano, text); err != nil {
				return nil, errors.New("expected an RFC3339 timestamp")
			}
		}

		return output.Redact(text, secrets...), nil
	case "boolean":
		if boolean, ok := value.(bool); ok {
			return boolean, nil
		}
	case "integer":
		if integer, ok := value.(int64); ok {
			return integer, nil
		}
	case "number":
		switch number := value.(type) {
		case int64:
			return float64(number), nil
		case float64:
			if !math.IsNaN(number) && !math.IsInf(number, 0) {
				return number, nil
			}
		}
	case "strings", "conditions":
		items, ok := value.([]any)
		if !ok || len(items) > 32 {
			return nil, errors.New("expected a list of at most 32 entries")
		}

		out := make([]any, 0, len(items))

		for _, item := range items {
			if kind == "strings" {
				projected, err := projectValue(item, "string", secrets)
				if err != nil {
					return nil, err
				}

				out = append(out, projected)

				continue
			}

			condition, ok := item.(map[string]any)
			if !ok {
				return nil, errors.New("expected condition records")
			}

			projected := map[string]any{}

			for _, key := range []string{"type", "status", "reason", "lastTransitionTime"} {
				value, found := condition[key]
				if !found {
					if key == "type" || key == "status" {
						return nil, errors.New("condition lacks type or status")
					}

					continue
				}

				fieldType := "string"
				if key == "lastTransitionTime" {
					fieldType = "timestamp"
				}

				safe, err := projectValue(value, fieldType, secrets)
				if err != nil {
					return nil, err
				}

				projected[key] = safe
			}

			out = append(out, projected)
		}

		return out, nil
	}

	return nil, errors.New("status value does not match its declared type")
}
