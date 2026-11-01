/*
Purpose: Project model exchanges into readable application-level transcripts.
Flow: Bounded SDK payloads are decoded, credential text is redacted and tool
arguments are normalized only for known handles. Provider error bodies and
authentication metadata never enter capture; this is not a raw HTTP dump.
*/
package analysis

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/output"
)

// transcriptText removes known credentials and recognizable credential assignments, not operational prose.
func transcriptText(text, apiKey string) string {
	return output.Redact(text, apiKey)
}

// transcriptPayload renders decoded SDK content without forwarding raw tool argument strings.
func transcriptPayload(data []byte, apiKey string, in incident.InvestigationContext) string {
	var value any

	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()

	if d.Decode(&value) != nil {
		return "[invalid transport JSON omitted]"
	}

	value = transcriptValue(value, apiKey, in)
	instructions := ""

	if fields, ok := value.(map[string]any); ok {
		if text, ok := fields["instructions"].(string); ok {
			instructions = "Instructions:\n" + text + "\n\nPayload:\n"
			fields["instructions"] = "[see Instructions section above]"
		}
	}

	pretty, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return "[transcript serialization failed]"
	}

	return instructions + string(pretty)
}

// transcriptValue preserves approved content and expands embedded JSON for readable tool/context sections.
func transcriptValue(value any, apiKey string, in incident.InvestigationContext) any {
	switch v := value.(type) {
	case map[string]any:
		if v["type"] == "function_call" {
			name, _ := v["name"].(string)
			args, _ := v["arguments"].(string)
			v["arguments"] = "[omitted: unvalidated tool arguments]"

			if name == "inspect_resource" || name == "related_warning_events" {
				var fields map[string]string

				d := json.NewDecoder(strings.NewReader(args))
				if len(args) <= 1024 && uniqueJSON(d, 0) == nil && json.Unmarshal([]byte(args), &fields) == nil && len(fields) == 1 {
					if _, ok := in.Identities[fields["resource_ref"]]; ok {
						v["arguments"] = fields
					}
				}
			} else {
				v["name"] = "[unknown tool]"
			}
		}

		for key, child := range v {
			switch strings.ToLower(key) {
			case "authorization", "api_key", "api-key", "apikey", "password", "passwd", "access_token", "refresh_token", "secret":
				v[key] = "[redacted credential]"
				continue
			}

			if text, ok := child.(string); ok && (key == "text" || key == "output" || key == "content") && len(text) > 0 && (text[0] == '{' || text[0] == '[') {
				var decoded any

				d := json.NewDecoder(strings.NewReader(text))
				d.UseNumber()

				if d.Decode(&decoded) == nil {
					child = decoded
				}
			}

			v[key] = transcriptValue(child, apiKey, in)
		}

		return v
	case []any:
		for i, child := range v {
			v[i] = transcriptValue(child, apiKey, in)
		}

		return v
	case string:
		return transcriptText(v, apiKey)
	default:
		return value
	}
}
