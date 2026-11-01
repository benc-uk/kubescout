/*
Purpose: Run bounded Responses API investigations and validate AI decisions.
Flow: Investigation contexts enter the Agent Framework loop with separate
inspection and Warning event tools sharing a session invocation cap and HTTP budgets; strict JSON,
citation and lifecycle checks return a
decision, including no-action, or safe classified failure details without
publishing an incident or exposing raw provider errors. Each actual HTTP attempt
and validation outcome can be captured in a bounded, sanitized local transcript.
*/
package analysis

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/incident"
	"github.com/benc-uk/kubescout/internal/investigation"
	"github.com/benc-uk/kubescout/internal/output"
	"github.com/microsoft/agent-framework-go/agent"
	"github.com/microsoft/agent-framework-go/agent/harness/toolautocall"
	"github.com/microsoft/agent-framework-go/provider/openaiprovider"
	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/responses"
	"k8s.io/client-go/dynamic"
)

type Analyzer struct {
	Config     config.Config
	Kubernetes dynamic.Interface
	HTTPClient *http.Client
	Log        *output.Writer
	Transcript *output.Transcript
	mu         sync.Mutex
	calls      []time.Time
}

type boundedClient struct {
	client     *http.Client
	admit      func(time.Time) error
	limit      int64
	inputLimit int
	inputBytes int
	requests   int
	bytes      int64
	failure    *Failure
	log        *output.Writer
	transcript *output.Transcript
	apiKey     string
	in         incident.InvestigationContext
}

//go:embed instructions.txt
var instructions string

// admit reserves a rolling hourly request slot or reports the earliest slot expiry.
func (a *Analyzer) admit(now time.Time) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	first := 0
	for first < len(a.calls) && !a.calls[first].After(now.Add(-time.Hour)) {
		first++
	}

	a.calls = a.calls[first:]
	if len(a.calls) >= a.Config.CallsPerHour {
		failure := &Failure{Kind: "local_hourly_quota", CallsInHour: len(a.calls), CallsPerHourLimit: a.Config.CallsPerHour}
		if len(a.calls) > 0 {
			retryAt := a.calls[0].Add(time.Hour).UTC()
			failure.RetryNotBefore = &retryAt
			failure.RetryHintSource = "local_rolling_hour"
		}

		return failure
	}

	a.calls = append(a.calls, now)

	return nil
}

// callsInHour counts only unexpired HTTP attempts without reserving another request slot.
func (a *Analyzer) callsInHour(now time.Time) int {
	a.mu.Lock()
	defer a.mu.Unlock()

	count := 0

	for _, call := range a.calls {
		if call.After(now.Add(-time.Hour)) {
			count++
		}
	}

	return count
}

// Investigate runs the bounded Responses tool loop and validates its final decision.
// Failures report safe categories, counters and retry hints, never raw SDK or provider text.
func (a *Analyzer) Investigate(ctx context.Context, in incident.InvestigationContext) (decision incident.Decision, investigationErr error) {
	scope := output.Scope(ctx)
	a.Transcript.Record(scope, "GO", "INVESTIGATION.START", fmt.Sprintf("Model: %s\nPrompt: %s\nTrigger: %s\nRequest limit: 6\nTool limit: %d\nRolling-hour limit: %d\nInput limit: %d bytes\nOutput limit: %d tokens/request\nRequest timeout: %s\nInvestigation timeout: %s\nInstructions:\n%s",
		a.Config.Model, incident.PromptVersion, in.Reason, a.Config.MaxToolCalls, a.Config.CallsPerHour, a.Config.MaxInputChars,
		a.Config.MaxOutputTokens, a.Config.RequestTimeout, a.Config.InvestigationTimeout, instructions))

	defer func() {
		message := "Validated decision: " + decision.Outcome
		if investigationErr != nil {
			message = "Investigation rejected/failed: " + investigationErr.Error()
			_ = a.Log.Log("warn", "GO", "MODEL", scope+" "+message)
		}

		a.Transcript.Record(scope, "GO", "INVESTIGATION.END", message)
	}()

	if a.Config.MaxToolCalls <= 0 {
		return incident.Decision{}, errors.New("investigation tool-call limit must be positive")
	}

	payload, err := json.Marshal(in)
	if err != nil {
		return incident.Decision{}, errors.New("investigation input serialization failed")
	}

	if len(payload) > a.Config.MaxInputChars {
		a.Transcript.Record(scope, "GO", "INPUT.REJECTED", "Initial context exceeds the configured input limit; content omitted.")

		return incident.Decision{}, &Failure{
			Kind: "local_investigation_input_limit", Observed: int64(len(payload)), Limit: int64(a.Config.MaxInputChars),
			ModelRequestLimit: 6, ToolCallLimit: a.Config.MaxToolCalls, CallsPerHourLimit: a.Config.CallsPerHour,
			CallsInHour:     a.callsInHour(time.Now()),
			MaxOutputTokens: a.Config.MaxOutputTokens,
			RequestTimeout:  a.Config.RequestTimeout.String(), InvestigationTimeout: a.Config.InvestigationTimeout.String(),
		}
	}

	if a.Transcript != nil {
		a.Transcript.Record(scope, "GO", "CONTEXT", transcriptPayload(payload, a.Config.APIKey, in))
	}

	ctx, cancel := context.WithTimeout(ctx, a.Config.InvestigationTimeout)
	defer cancel()

	client := a.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: a.Config.RequestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect refused") }}
	}

	bounded := &boundedClient{client: client, admit: a.admit, limit: int64(a.Config.MaxOutputTokens)*64 + 65536, inputLimit: a.Config.MaxInputChars * 4,
		log: a.Log, transcript: a.Transcript, apiKey: a.Config.APIKey, in: in}
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

	session := &investigation.Session{Client: a.Kubernetes, Investigation: in, Timeout: a.Config.RequestTimeout, Citations: citations,
		MaxCalls: a.Config.MaxToolCalls, Log: a.Log, Transcript: a.Transcript,
		Scope: config.Config{NamespaceWatch: a.Config.NamespaceWatch, NamespaceIgnore: a.Config.NamespaceIgnore}}

	result, err := agentClient.RunText(ctx, string(payload),
		agent.WithTool(investigation.InspectResource{Session: session}),
		agent.WithTool(investigation.RelatedWarningEvents{Session: session}),
		openaiprovider.ResponsesNewParams(responses.ResponseNewParams{Store: openai.Bool(false), MaxOutputTokens: openai.Int(int64(a.Config.MaxOutputTokens))}),
	).Collect()
	if err == nil && (result.FinishReason != "stop" || result.ContinuationToken != "") {
		err = errors.New("AI returned incomplete or unsupported investigation output")
		bounded.failure = &Failure{Kind: "model_incomplete_output"}
	}

	if err != nil {
		failure := bounded.failure
		if failure == nil {
			failure = &Failure{Kind: "tool_or_model_protocol"}

			var toolLimit *investigation.LimitError
			if errors.As(err, &toolLimit) {
				failure.Kind, failure.Observed, failure.Limit = toolLimit.Kind, int64(toolLimit.Observed), int64(toolLimit.Limit)
			}
		}

		if ctx.Err() != nil {
			failure.Kind = "investigation_deadline"
			if errors.Is(ctx.Err(), context.Canceled) {
				failure.Kind = "investigation_canceled"
			}
		}

		failure.CallsInHour = a.callsInHour(time.Now())
		failure.ModelRequests, failure.ModelRequestLimit, failure.CallsPerHourLimit = bounded.requests, 6, a.Config.CallsPerHour
		failure.ToolCalls, failure.ToolCallLimit = session.Calls, a.Config.MaxToolCalls
		failure.InputBytes, failure.OutputBytes = bounded.inputBytes, bounded.bytes
		failure.MaxOutputTokens = a.Config.MaxOutputTokens
		failure.RequestTimeout, failure.InvestigationTimeout = a.Config.RequestTimeout.String(), a.Config.InvestigationTimeout.String()

		return incident.Decision{}, failure
	}

	a.Transcript.Record(scope, "AI", "FINAL.ANSWER", transcriptText(result.String(), a.Config.APIKey))

	decision, investigationErr = Validate(result.String(), in, citations)
	if investigationErr != nil {
		_ = a.Log.Log("warn", "GO", "VALIDATE", scope+" model decision rejected: "+investigationErr.Error())
	}

	return decision, investigationErr
}

// Do enforces request and transcript budgets plus shared quota before issuing model HTTP calls.
// Responses are buffered under byte limits; failures retain safe status and retry metadata.
func (c *boundedClient) Do(req *http.Request) (response *http.Response, requestErr error) {
	scope := output.Scope(req.Context())
	start := time.Now()
	sent := false
	attempt := c.requests + 1

	defer func() {
		if requestErr != nil {
			detail := "request failed"
			if c.failure != nil {
				detail = fmt.Sprintf("kind=%s http_status=%d observed=%d limit=%d", c.failure.Kind, c.failure.HTTPStatus, c.failure.Observed, c.failure.Limit)
				if c.failure.RetryNotBefore != nil {
					detail += " provider_or_quota_retry_not_before=" + c.failure.RetryNotBefore.Format(time.RFC3339)
				}
			}

			message := fmt.Sprintf("%s request=%d sent=%t duration=%s: %s", scope, attempt, sent, time.Since(start).Round(time.Millisecond), detail)
			_ = c.log.Log("warn", "GO", "MODEL.REQUEST", message)
			c.transcript.Record(scope, "GO", "REQUEST.FAILED", message)
		}
	}()

	if c.requests >= 6 {
		return nil, c.fail("local_model_request_limit", int64(c.requests+1), 6)
	}

	var body []byte

	if req.Body != nil {
		var err error

		body, err = io.ReadAll(io.LimitReader(req.Body, int64(c.inputLimit)+1))
		req.Body.Close()

		if err != nil {
			return nil, c.fail("model_input_read", 0, 0)
		}

		if len(body) > c.inputLimit {
			return nil, c.fail("local_model_input_limit", int64(len(body)), int64(c.inputLimit))
		}

		c.inputBytes += len(body)
		if c.inputBytes > 3*c.inputLimit {
			return nil, c.fail("local_cumulative_input_limit", int64(c.inputBytes), int64(3*c.inputLimit))
		}

		req.Body = io.NopCloser(bytes.NewReader(body))
	}

	if err := c.admit(time.Now()); err != nil {
		if !errors.As(err, &c.failure) {
			return nil, c.fail("local_request_admission", 0, 0)
		}

		return nil, c.failure
	}

	c.requests++

	_ = c.log.Log("info", "GO", "MODEL.REQUEST", fmt.Sprintf("%s request=%d/6 Sending %d bytes", scope, c.requests, len(body)))
	if c.transcript != nil {
		c.transcript.Record(scope, "GO", fmt.Sprintf("REQUEST.%d", c.requests), transcriptPayload(body, c.apiKey, c.in))
	}

	sent = true

	response, err := c.client.Do(req)
	if err != nil {
		kind := "model_transport"

		var timeout net.Error

		if errors.As(err, &timeout) && timeout.Timeout() {
			kind = "model_request_timeout"
		}

		return nil, c.fail(kind, 0, 0)
	}

	if response.StatusCode >= http.StatusBadRequest {
		_ = response.Body.Close()
		c.failure = providerFailure(response, time.Now())

		return nil, c.failure
	}

	data, err := io.ReadAll(io.LimitReader(response.Body, c.limit+1))
	closeErr := response.Body.Close()

	c.bytes += int64(len(data))

	if err != nil || closeErr != nil {
		var timeout net.Error
		if errors.As(err, &timeout) && timeout.Timeout() || errors.As(closeErr, &timeout) && timeout.Timeout() {
			return nil, c.fail("model_request_timeout", 0, 0)
		}

		return nil, c.fail("model_output_read", 0, 0)
	}

	if int64(len(data)) > c.limit {
		return nil, c.fail("local_model_output_limit", int64(len(data)), c.limit)
	}

	if c.bytes > 2*c.limit {
		return nil, c.fail("local_cumulative_output_limit", c.bytes, 2*c.limit)
	}

	_ = c.log.Log("info", "GO", "MODEL.RESPONSE", fmt.Sprintf("%s request=%d Received HTTP %d, %d bytes in %s (not yet validated)",
		scope, c.requests, response.StatusCode, len(data), time.Since(start).Round(time.Millisecond)))
	if c.transcript != nil {
		c.transcript.Record(scope, "AI", fmt.Sprintf("RESPONSE.%d", c.requests), transcriptPayload(data, c.apiKey, c.in))
	}

	response.Body = io.NopCloser(bytes.NewReader(data))

	return response, nil
}

// fail records an allow-listed transport or budget failure for SDK-independent diagnostics.
func (c *boundedClient) fail(kind string, observed, limit int64) error {
	c.failure = &Failure{Kind: kind, Observed: observed, Limit: limit}

	return c.failure
}

// Validate rejects malformed, duplicate-key or unexpected JSON fields before checking
// decision semantics against authorised resources, citations and lifecycle context.
func Validate(text string, in incident.InvestigationContext, citations map[string]bool) (incident.Decision, error) {
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

// uniqueJSON consumes one JSON value recursively, rejecting duplicate keys and excessive nesting.
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
