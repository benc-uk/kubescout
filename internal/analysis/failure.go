/*
Purpose: Describe investigation failures without exposing provider or tool text.
Flow: Transport status, local budgets and deadlines become safe typed diagnostics;
only validated numeric quota headers and retry times supplement failure counters.
These diagnostics do not decide incidents or change retry scheduling.
*/
package analysis

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// Failure contains allow-listed metadata only; it never wraps raw SDK or provider errors.
type Failure struct {
	Kind                 string     `json:"kind"`
	HTTPStatus           int        `json:"http_status,omitempty"`
	RetryNotBefore       *time.Time `json:"retry_not_before,omitempty"`
	RetryHintSource      string     `json:"retry_hint_source,omitempty"`
	RemainingRequests    *int64     `json:"remaining_requests,omitempty"`
	RemainingTokens      *int64     `json:"remaining_tokens,omitempty"`
	ModelRequests        int        `json:"model_requests"`
	ModelRequestLimit    int        `json:"model_request_limit"`
	CallsInHour          int        `json:"calls_in_hour"`
	CallsPerHourLimit    int        `json:"calls_per_hour_limit"`
	ToolCalls            int        `json:"tool_calls"`
	ToolCallLimit        int        `json:"tool_call_limit"`
	InputBytes           int        `json:"input_bytes"`
	OutputBytes          int64      `json:"output_bytes"`
	MaxOutputTokens      int        `json:"max_output_tokens"`
	Observed             int64      `json:"observed,omitempty"`
	Limit                int64      `json:"limit,omitempty"`
	RequestTimeout       string     `json:"request_timeout"`
	InvestigationTimeout string     `json:"investigation_timeout"`
}

// Error formats allow-listed failure counters, bytes and retry hints without raw SDK text.
func (f *Failure) Error() string {
	message := fmt.Sprintf("AI investigation failed: %s (model_requests=%d/%d, calls_in_hour=%d/%d, tool_calls=%d/%d",
		f.Kind, f.ModelRequests, f.ModelRequestLimit, f.CallsInHour, f.CallsPerHourLimit, f.ToolCalls, f.ToolCallLimit)
	if f.HTTPStatus != 0 {
		message += fmt.Sprintf(", http_status=%d", f.HTTPStatus)
	}

	if f.Limit != 0 {
		message += fmt.Sprintf(", observed=%d, limit=%d", f.Observed, f.Limit)
	}

	if f.RetryNotBefore != nil {
		message += ", retry_not_before=" + f.RetryNotBefore.Format(time.RFC3339Nano)
	}

	if f.RequestTimeout != "" {
		message += ", request_timeout=" + f.RequestTimeout + ", investigation_timeout=" + f.InvestigationTimeout
	}

	message += fmt.Sprintf(", input_bytes=%d, output_bytes=%d", f.InputBytes, f.OutputBytes)
	if f.RemainingRequests != nil {
		message += fmt.Sprintf(", remaining_requests=%d", *f.RemainingRequests)
	}

	if f.RemainingTokens != nil {
		message += fmt.Sprintf(", remaining_tokens=%d", *f.RemainingTokens)
	}

	return message + ")"
}

// providerFailure classifies HTTP failures using status and validated quota headers only.
func providerFailure(response *http.Response, now time.Time) *Failure {
	f := &Failure{Kind: "provider_http_error", HTTPStatus: response.StatusCode}
	switch response.StatusCode {
	case http.StatusTooManyRequests:
		f.Kind = "provider_rate_limit"
	case http.StatusUnauthorized, http.StatusForbidden:
		f.Kind = "provider_authentication"
	}

	f.RemainingRequests = numericHeader(response.Header.Get("x-ratelimit-remaining-requests"))
	f.RemainingTokens = numericHeader(response.Header.Get("x-ratelimit-remaining-tokens"))

	for _, header := range []struct {
		name string
		unit time.Duration
	}{
		{"Retry-After", time.Second},
		{"retry-after-ms", time.Millisecond},
		{"x-ms-retry-after-ms", time.Millisecond},
	} {
		value := response.Header.Get(header.name)

		var retryAt time.Time

		if number := numericHeader(value); number != nil && *number <= int64((1<<63-1)/header.unit) {
			retryAt = now.Add(time.Duration(*number) * header.unit)
		} else if header.name == "Retry-After" && len(value) <= 64 {
			retryAt, _ = http.ParseTime(value)
		}

		if !retryAt.Before(now) && (f.RetryNotBefore == nil || retryAt.After(*f.RetryNotBefore)) {
			retryAt = retryAt.UTC()
			f.RetryNotBefore, f.RetryHintSource = &retryAt, header.name
		}
	}

	return f
}

// numericHeader accepts only bounded nonnegative integers, never arbitrary header text.
func numericHeader(value string) *int64 {
	if len(value) == 0 || len(value) > 20 {
		return nil
	}

	for _, char := range value {
		if char < '0' || char > '9' {
			return nil
		}
	}

	number, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil
	}

	return &number
}
