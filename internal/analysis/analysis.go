package analysis

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/microsoft/agent-framework-go/agent"
	"github.com/microsoft/agent-framework-go/agent/harness/toolautocall"
	"github.com/microsoft/agent-framework-go/provider/openaiprovider"
	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/responses"
	"k8s-ops-agent/internal/config"
	"k8s-ops-agent/internal/incident"
	"k8s-ops-agent/internal/investigation"
	"k8s.io/client-go/dynamic"
)

//go:embed instructions.txt
var instructions string

type Analyzer struct {
	Config     config.Config
	Kubernetes dynamic.Interface
	HTTPClient *http.Client
	mu         sync.Mutex
	calls      []time.Time
}

func (a *Analyzer) admit(now time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	first := 0
	for first < len(a.calls) && !a.calls[first].After(now.Add(-time.Hour)) {
		first++
	}
	a.calls = a.calls[first:]
	if len(a.calls) >= a.Config.CallsPerHour {
		return false
	}
	a.calls = append(a.calls, now)
	return true
}

func (a *Analyzer) Investigate(ctx context.Context, in incident.Input) (incident.Decision, error) {
	payload, err := json.Marshal(in)
	if err != nil || len(payload) > a.Config.MaxInputChars {
		return incident.Decision{}, errors.New("investigation input exceeds limit")
	}
	ctx, cancel := context.WithTimeout(ctx, a.Config.InvestigationTimeout)
	defer cancel()
	client := a.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: a.Config.RequestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect refused") }}
	}
	bounded := &boundedClient{client: client, admit: a.admit, limit: int64(a.Config.MaxOutputTokens)*64 + 65536, inputLimit: a.Config.MaxInputChars * 4}
	api := openai.NewClient(option.WithBaseURL(a.Config.Endpoint), option.WithAPIKey(a.Config.APIKey), option.WithMaxRetries(0),
		option.WithRequestTimeout(a.Config.RequestTimeout), option.WithHTTPClient(bounded))
	agentClient := openaiprovider.NewResponsesAgent(api, openaiprovider.AgentConfig{
		Model: a.Config.Model, Instructions: instructions, DisableStoreOutput: true,
		ToolAutoCall: &toolautocall.Config{MaximumIterationsPerRequest: new(4), MaximumConsecutiveErrorsPerRequest: new(0),
			TerminateOnUnknownCalls: true, AllowConcurrentInvocations: false},
	})
	citations := map[string]bool{"snapshot": true}
	for _, s := range in.Signals {
		citations[s.ID] = true
	}
	session := &investigation.Session{Client: a.Kubernetes, Input: in, Timeout: a.Config.RequestTimeout, Citations: citations,
		Scope: config.Config{NamespaceWatch: a.Config.NamespaceWatch, NamespaceIgnore: a.Config.NamespaceIgnore}}
	result, err := agentClient.RunText(ctx, string(payload),
		agent.WithTool(investigation.Tool{Session: session}),
		agent.WithTool(investigation.Tool{Session: session, RelatedEvents: true}),
		openaiprovider.ResponsesNewParams(responses.ResponseNewParams{Store: openai.Bool(false), MaxOutputTokens: openai.Int(int64(a.Config.MaxOutputTokens))}),
	).Collect()
	if err != nil {
		return incident.Decision{}, errors.New("AI investigation failed, was rate limited, or exceeded its budget")
	}
	if result.FinishReason != "stop" || result.ContinuationToken != "" {
		return incident.Decision{}, errors.New("AI returned incomplete or unsupported investigation output")
	}
	return Validate(result.String(), in, citations)
}

type boundedClient struct {
	client     *http.Client
	admit      func(time.Time) bool
	limit      int64
	inputLimit int
	inputBytes int
	requests   int
	bytes      int64
}

func (c *boundedClient) Do(req *http.Request) (*http.Response, error) {
	if c.requests >= 6 {
		return nil, errors.New("model request limit reached")
	}
	if req.Body != nil {
		body, err := io.ReadAll(io.LimitReader(req.Body, int64(c.inputLimit)+1))
		req.Body.Close()
		if err != nil || len(body) > c.inputLimit {
			return nil, errors.New("model transcript limit exceeded")
		}
		c.inputBytes += len(body)
		if c.inputBytes > 3*c.inputLimit {
			return nil, errors.New("cumulative model input budget exceeded")
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	if !c.admit(time.Now()) {
		return nil, errors.New("rolling model request budget exhausted")
	}
	c.requests++
	response, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, c.limit+1))
	closeErr := response.Body.Close()
	c.bytes += int64(len(data))
	if err != nil || closeErr != nil || int64(len(data)) > c.limit || c.bytes > 2*c.limit {
		return nil, errors.New("model output byte limit exceeded")
	}
	response.Body = io.NopCloser(bytes.NewReader(data))
	return response, nil
}

func Validate(text string, in incident.Input, citations map[string]bool) (incident.Decision, error) {
	var out incident.Decision
	if len(text) > 32000 {
		return out, errors.New("AI decision exceeds limit")
	}
	decoder := json.NewDecoder(bytes.NewBufferString(text))
	if err := uniqueJSON(decoder, 0); err != nil {
		return out, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return out, errors.New("trailing AI output")
	}
	var root map[string]json.RawMessage
	if json.Unmarshal([]byte(text), &root) != nil || len(root) != 3 || root["outcome"] == nil || root["reason"] == nil || root["actions"] == nil {
		return out, errors.New("invalid AI decision fields")
	}
	var actions []map[string]json.RawMessage
	if json.Unmarshal(root["actions"], &actions) != nil {
		return out, errors.New("invalid AI action list")
	}
	actionType := reflect.TypeFor[incident.Action]()
	for _, a := range actions {
		if len(a) != actionType.NumField() {
			return out, errors.New("missing or unknown AI action fields")
		}
		for i := 0; i < actionType.NumField(); i++ {
			raw := a[actionType.Field(i).Tag.Get("json")]
			if raw == nil || string(raw) == "null" {
				return out, errors.New("missing or null AI action field")
			}
		}
	}
	decoder = json.NewDecoder(bytes.NewBufferString(text))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&out); err != nil {
		return out, errors.New("invalid AI decision JSON")
	}
	if err := incident.Validate(out, in, citations); err != nil {
		return out, err
	}
	return out, nil
}

func uniqueJSON(d *json.Decoder, depth int) error {
	if depth > 16 {
		return errors.New("JSON nesting limit exceeded")
	}
	token, err := d.Token()
	if err != nil {
		return errors.New("invalid AI JSON")
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	keys := map[string]bool{}
	for d.More() {
		if delim == '{' {
			token, err := d.Token()
			key, ok := token.(string)
			if err != nil || !ok || keys[key] {
				return errors.New("duplicate JSON key")
			}
			keys[key] = true
		}
		if err := uniqueJSON(d, depth+1); err != nil {
			return err
		}
	}
	_, err = d.Token()
	return err
}
