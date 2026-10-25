package analysis

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/microsoft/agent-framework-go/provider/openaiprovider"
	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/responses"
	"k8s-ops-agent/internal/config"
	"k8s-ops-agent/internal/health"
	"k8s-ops-agent/internal/report"
)

//go:embed instructions.txt
var instructions string

type Analyzer struct {
	Config     config.Config
	HTTPClient *http.Client
}

func (a *Analyzer) Analyze(ctx context.Context, findings []health.Finding) report.Analysis {
	fail := func(reason string) report.Analysis { return report.AnalysisState("failed", reason) }
	payload, err := Evidence(findings, a.Config.MaxInputChars)
	if err != nil {
		return fail(err.Error())
	}
	if strings.TrimSpace(instructions) == "" {
		return fail("analysis instructions are blank")
	}
	ctx, cancel := context.WithTimeout(ctx, a.Config.RequestTimeout)
	defer cancel()
	httpClient := a.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: a.Config.RequestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect refused") }}
	}
	bounded := &boundedClient{client: httpClient, limit: int64(a.Config.MaxOutputTokens)*64 + 65536}
	client := openai.NewClient(
		option.WithBaseURL(a.Config.Endpoint), option.WithAPIKey(a.Config.APIKey),
		option.WithMaxRetries(0), option.WithRequestTimeout(a.Config.RequestTimeout), option.WithHTTPClient(bounded),
	)
	agent := openaiprovider.NewResponsesAgent(client, openaiprovider.AgentConfig{
		Model: a.Config.Model, Instructions: instructions, DisableStoreOutput: true,
	})
	response, err := agent.RunText(ctx, payload,
		openaiprovider.ResponsesIncludeReasoningEncryptedContent(false),
		openaiprovider.ResponsesNewParams(responses.ResponseNewParams{
			MaxOutputTokens: openai.Int(int64(a.Config.MaxOutputTokens)),
			Store:           openai.Bool(false),
		}),
	).Collect()
	if err != nil {
		return fail("model request failed or timed out")
	}
	if response.FinishReason != "stop" || response.ContinuationToken != "" {
		return fail("model returned an incomplete or unsupported response")
	}
	text := response.String()
	if len(text) > a.Config.MaxOutputTokens*32 {
		return fail("model output exceeds the size limit")
	}
	items, err := Validate(text, findings)
	if err != nil {
		return fail(err.Error())
	}
	return report.Analysis{Status: "completed", Items: items}
}

type boundedClient struct {
	client *http.Client
	limit  int64
	called atomic.Bool
}

func (c *boundedClient) Do(req *http.Request) (*http.Response, error) {
	if c.called.Swap(true) {
		return nil, errors.New("only one model request is permitted per analysis")
	}
	response, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, c.limit+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || int64(len(body)) > c.limit {
		return nil, errors.New("model response body invalid or exceeds limit")
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	return response, nil
}

func Evidence(findings []health.Finding, maxChars int) (string, error) {
	type resource struct {
		Kind      string `json:"kind"`
		Namespace string `json:"namespace,omitempty"`
		Name      string `json:"name"`
	}
	type evidence struct {
		ID       string   `json:"id"`
		Rule     string   `json:"rule_id"`
		Severity string   `json:"severity"`
		Resource resource `json:"resource"`
		Summary  string   `json:"summary"`
		Evidence []string `json:"evidence"`
	}
	allowed := make([]evidence, 0, len(findings))
	for _, f := range findings {
		allowed = append(allowed, evidence{f.ID, f.Rule, f.Severity, resource{f.Resource.Kind, f.Resource.Namespace, f.Resource.Name}, f.Summary, f.Evidence})
	}
	data, err := json.Marshal(struct {
		Findings []evidence `json:"findings"`
	}{allowed})
	if err != nil {
		return "", errors.New("cannot encode analysis evidence")
	}
	if utf8.RuneCount(data) > maxChars {
		return "", errors.New("analysis evidence exceeds input limit; findings were not truncated")
	}
	return string(data), nil
}

func Validate(text string, findings []health.Finding) ([]report.Item, error) {
	invalid := errors.New("model returned invalid analysis JSON or schema")
	decoder := json.NewDecoder(strings.NewReader(text))
	if err := uniqueJSON(decoder, 0); err != nil {
		return nil, invalid
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, invalid
	}
	var root map[string]json.RawMessage
	if json.Unmarshal([]byte(text), &root) != nil || len(root) != 1 || root["items"] == nil {
		return nil, invalid
	}
	var entries []map[string]json.RawMessage
	if json.Unmarshal(root["items"], &entries) != nil || len(entries) == 0 || len(entries) > 100 {
		return nil, invalid
	}
	known, required, seen := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, f := range findings {
		known[f.ID] = true
		if f.Actionable() {
			required[f.ID] = true
		}
	}
	items := make([]report.Item, 0, len(entries))
	for _, entry := range entries {
		if len(entry) != 3 || entry["finding_ids"] == nil || entry["explanation"] == nil || entry["suggested_checks"] == nil {
			return nil, invalid
		}
		var item report.Item
		if json.Unmarshal(entry["finding_ids"], &item.FindingIDs) != nil ||
			json.Unmarshal(entry["explanation"], &item.Explanation) != nil ||
			json.Unmarshal(entry["suggested_checks"], &item.SuggestedChecks) != nil {
			return nil, invalid
		}
		if !validStrings(item.FindingIDs, 100) || !validStrings(item.SuggestedChecks, 10) || strings.TrimSpace(item.Explanation) == "" || utf8.RuneCountInString(item.Explanation) > 4000 {
			return nil, invalid
		}
		for _, id := range item.FindingIDs {
			if !known[id] {
				return nil, errors.New("model analysis referenced an unknown finding")
			}
			seen[id] = true
		}
		items = append(items, item)
	}
	for id := range required {
		if !seen[id] {
			return nil, errors.New("model analysis omitted actionable findings")
		}
	}
	return items, nil
}

func validStrings(values []string, max int) bool {
	if len(values) == 0 || len(values) > max {
		return false
	}
	seen := map[string]bool{}
	for _, value := range values {
		if strings.TrimSpace(value) == "" || utf8.RuneCountInString(value) > 2000 || seen[value] {
			return false
		}
		seen[value] = true
	}
	return true
}

// encoding/json accepts duplicate keys by default, which is not a strict contract.
func uniqueJSON(d *json.Decoder, depth int) error {
	if depth > 16 {
		return errors.New("JSON nesting limit exceeded")
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	keys := map[string]bool{}
	for d.More() {
		if delim == '{' {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || keys[name] {
				return errors.New("duplicate JSON key")
			}
			keys[name] = true
		}
		if err := uniqueJSON(d, depth+1); err != nil {
			return err
		}
	}
	_, err = d.Token()
	return err
}
