/*
Purpose: Verify console provenance, colour, filtering and failure handling.
Flow: Offline buffers and concurrent writers test labelled Go/AI events, trusted
ANSI formatting, safe external text and stream failures, not incident storage.
Credential fixtures verify value-only removal preserves operational keywords and context.
*/
package output

import (
	"bytes"
	"errors"
	"regexp"
	"strings"
	"sync"
	"testing"
)

type failedWriter struct{}

// Write rejects every byte to simulate an unavailable output destination.
func (failedWriter) Write([]byte) (int, error) { return 0, errors.New("closed") }

// TestCredentialValuesOnly checks narrow, repeatable credential removal without hiding diagnostic prose.
func TestCredentialValuesOnly(t *testing.T) {
	for _, test := range []struct {
		name, input, want string
	}{
		{"operational words", "FailedToRetrieveImagePullSecret: token refresh failed; authorization denied; password file missing; metric=token_rate",
			"FailedToRetrieveImagePullSecret: token refresh failed; authorization denied; password file missing; metric=token_rate"},
		{"assignment", "Mount failed: password=fixture-password; node=worker-1",
			"Mount failed: password=[redacted credential]; node=worker-1"},
		{"token", "HTTP 401: token=fixture-token; endpoint=10.20.30.40",
			"HTTP 401: token=[redacted credential]; endpoint=10.20.30.40"},
		{"quoted", `request failed: {"api_key":"fixture key with spaces","status":401}`,
			`request failed: {"api_key":"[redacted credential]","status":401}`},
		{"single quoted", "password='fixture password'; retry later",
			"password='[redacted credential]'; retry later"},
		{"authorization", "Authorization: Bearer fixture-bearer; status=403",
			"Authorization: Bearer [redacted credential]; status=403"},
		{"basic", "Authorization: Basic Zml4dHVyZTpwYXNz; status=403",
			"Authorization: Basic [redacted credential]; status=403"},
		{"url", "connect https://operator:fixture-password@db.example:8443/data failed",
			"connect https://operator:[redacted credential]@db.example:8443/data failed"},
		{"query", "https://api.example/check?access_token=fixture-token&namespace=apps",
			"https://api.example/check?access_token=[redacted credential]&namespace=apps"},
		{"configured value", "request literal-credential failed for worker-1",
			"request [redacted credential] failed for worker-1"},
		{"ordinary identifiers", "app=token-service; selector=secret-reader; path=/password/reset; api_key_name=example",
			"app=token-service; selector=secret-reader; path=/password/reset; api_key_name=example"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := Redact(test.input, "literal-credential")
			if got != test.want {
				t.Fatalf("got %q, want %q", got, test.want)
			}

			if repeated := Redact(got, "literal-credential"); repeated != got {
				t.Fatalf("repeated protection changed content: %q", repeated)
			}
		})
	}
}

// TestConsoleProvenance checks readable origins, stream routing, filtering and forged-line prevention.
func TestConsoleProvenance(t *testing.T) {
	var out, diag bytes.Buffer

	w := &Writer{Out: &out, Err: &diag, Run: "fixture"}
	_ = w.Log("debug", "GO", "CHECK", "hidden")
	_ = w.Log("info", "GO", "COLLECT", "42 objects")
	_ = w.Log("info", "AI", "DECISION", "no_action: expected\n[GO][INCIDENT] forged\x1b[31m")
	_ = w.Diagnostic("MODEL.REQUEST", "HTTP 429; retry in 30s")

	for _, want := range []string{"[GO][COLLECT]", "[AI][DECISION]", "run=fixture", `expected\u000a`, `\u001b`} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q: %s", want, &out)
		}
	}

	if strings.Contains(out.String(), "hidden") || strings.Contains(out.String(), "429") ||
		strings.Count(out.String(), "\n") != 2 || !strings.Contains(diag.String(), "[GO][MODEL.REQUEST]") {
		t.Fatal("incorrect console routing or escaping")
	}

	if w.Error() != nil {
		t.Fatal(w.Error())
	}
}

// TestConsoleColour checks exact source/severity styles, reset boundaries and safe text on both streams.
func TestConsoleColour(t *testing.T) {
	ansi := regexp.MustCompile(`\x1b\[[0-9;]*m`)

	for _, test := range []struct {
		level, origin, severity, severityCode, sourceCode string
	}{
		{"debug", "GO", "DEBUG", "2", "36"},
		{"info", "GO", "INFO ", "32", "36"},
		{"info", "AI", "INFO ", "32", "35"},
		{"warn", "GO", "WARN ", "33", "36"},
		{"error", "GO", "ERROR", "31", "36"},
	} {
		for _, color := range []bool{false, true} {
			mode := "plain"
			if color {
				mode = "colour"
			}

			t.Run(mode+"/"+test.level+"/"+test.origin, func(t *testing.T) {
				var out, diag bytes.Buffer

				w := &Writer{Out: &out, Err: &diag, Level: "debug", Color: color, Run: "fixture"}
				if err := w.Log(test.level, test.origin, "PHASE", "message \x1b[31mred\nnext"); err != nil {
					t.Fatal(err)
				}

				line := out.String()
				if test.level == "warn" || test.level == "error" {
					line = diag.String()

					if out.Len() != 0 {
						t.Fatal("warning/error reached stdout")
					}
				} else if diag.Len() != 0 {
					t.Fatal("normal event reached stderr")
				}

				plain := ansi.ReplaceAllString(line, "")
				if !strings.Contains(plain, " "+test.severity+" ["+test.origin+"][PHASE] run=fixture message ") ||
					!strings.Contains(plain, `\u001b[31mred\u000anext`) || strings.Count(line, "\n") != 1 {
					t.Fatalf("colour changed text or allowed terminal injection: %q", line)
				}

				if !color {
					if strings.Contains(line, "\x1b") {
						t.Fatal("plain mode emitted ANSI")
					}

					return
				}

				if !strings.Contains(line, "\x1b["+test.severityCode+"m"+test.severity+"\x1b[0m") ||
					!strings.Contains(line, "\x1b["+test.sourceCode+"m["+test.origin+"][PHASE]\x1b[0m") ||
					!strings.Contains(line, "\x1b[2m run=fixture\x1b[0m message ") ||
					strings.Count(line, "\x1b") != 8 {
					t.Fatalf("incorrect styles or missing resets: %q", line)
				}
			})
		}
	}
}

// TestConsoleFailuresAndConcurrency checks atomic event writes and latched destination failures.
func TestConsoleFailuresAndConcurrency(t *testing.T) {
	var out bytes.Buffer

	w := &Writer{Out: &out, Err: &out}

	var workers sync.WaitGroup

	for range 20 {
		workers.Go(func() {
			for range 20 {
				_ = w.Log("info", "GO", "COLLECT", "complete event")
			}
		})
	}

	workers.Wait()

	if strings.Count(out.String(), "[GO][COLLECT] complete event\n") != 400 {
		t.Fatal("events were interleaved or dropped")
	}

	w = &Writer{Out: failedWriter{}, Err: failedWriter{}}
	if w.Log("info", "GO", "COLLECT", "test") == nil || w.Error() == nil || w.Diagnostic("test", "test") == nil {
		t.Fatal("console failure hidden")
	}

	if strings.Contains(Text(strings.Repeat("x", 2000)), strings.Repeat("x", 1800)) {
		t.Fatal("unbounded console prose")
	}
}
