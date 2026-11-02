/*
Purpose: Render operator-facing text with explicit Go/model provenance and phases.
Flow: Concurrent callers emit bounded single-line events to stdout or stderr,
optionally colouring trusted labels after escaping external text. Credential
values are removed without discarding surrounding operational details. Console
failures are retained for the runner. Logs describe work, never store
incidents or turn deterministic observations into AI conclusions.
*/
package output

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
)

type Writer struct {
	Out, Err io.Writer
	Level    string
	Run      string
	Secrets  []string
	Color    bool
	mu       sync.Mutex
	failure  error
}

type investigationKey struct{}

var credentialText = regexp.MustCompile(`(?i)\b((?:bearer|basic)[ \t]+|(?:authorization|password|passwd|api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|token|secret)[ \t]*["']?[ \t]*[:=][ \t]*(?:(?:bearer|basic)[ \t]+)?)(\[redacted credential\]|"[^"\r\n]*"|'[^'\r\n]*'|[^\s"',;&<>]+)`)
var credentialURL = regexp.MustCompile(`(?i)(https?://[^/\s:@]+:)(?:\[redacted credential\]|[^@\s]+)@`)

// Redact removes credential values while retaining field names, URLs and surrounding operational prose.
func Redact(text string, secrets ...string) string {
	for _, secret := range secrets {
		if secret != "" {
			text = strings.ReplaceAll(text, secret, "[redacted credential]")
		}
	}

	text = credentialText.ReplaceAllStringFunc(text, func(match string) string {
		parts := credentialText.FindStringSubmatch(match)
		value := "[redacted credential]"

		if parts[2][0] == '"' || parts[2][0] == '\'' {
			quote := parts[2][:1]
			value = quote + value + quote
		}

		return parts[1] + value
	})

	return credentialURL.ReplaceAllString(text, "${1}[redacted credential]@")
}

// WithInvestigation attaches diagnostic correlation without changing model input or incident identity.
func WithInvestigation(ctx context.Context, id, resource string) context.Context {
	return context.WithValue(ctx, investigationKey{}, "inv="+id+" "+resource)
}

// Scope returns the investigation label carried by this context, if any.
func Scope(ctx context.Context) string {
	label, _ := ctx.Value(investigationKey{}).(string)
	return label
}

// Text escapes controls and limits console text so untrusted prose cannot forge log lines.
func Text(value string) string {
	var out strings.Builder

	for _, r := range value {
		if out.Len() >= 1600 {
			out.WriteString(" [console excerpt; see AI transcript]")
			break
		}

		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp) {
			fmt.Fprintf(&out, "\\u%04x", r)
		} else {
			out.WriteRune(r)
		}
	}

	return out.String()
}

// Log writes one optionally coloured event atomically; warnings/errors use stderr, other levels stdout.
// A nil writer disables optional instrumentation; configured destination failures remain observable.
func (w *Writer) Log(level, origin, phase, message string) error {
	if w == nil {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.failure != nil {
		return w.failure
	}

	if rank(level) < rank(w.Level) {
		return nil
	}

	dest := w.Out
	if level == "warn" || level == "error" {
		dest = w.Err
	}

	if dest == nil {
		w.failure = errors.New("console destination unavailable")
		return w.failure
	}

	run := ""
	if w.Run != "" {
		run = " run=" + Text(w.Run)
	}

	timestamp := time.Now().UTC().Format(time.RFC3339)
	severity := fmt.Sprintf("%-5s", strings.ToUpper(level))
	label := "[" + Text(origin) + "][" + Text(phase) + "]"

	if w.Color {
		timestamp = colorize(timestamp, "2")
		if run != "" {
			run = colorize(run, "2")
		}

		switch level {
		case "debug":
			severity = colorize(severity, "2")
		case "info":
			severity = colorize(severity, "32")
		case "warn":
			severity = colorize(severity, "33")
		case "error":
			severity = colorize(severity, "31")
		}

		switch origin {
		case "GO":
			label = colorize(label, "36")
		case "AI":
			label = colorize(label, "35")
		}
	}

	line := fmt.Sprintf("%s %s %s%s %s\n", timestamp, severity, label, run, Text(Redact(message, w.Secrets...)))

	n, err := io.WriteString(dest, line)
	if err != nil || n != len(line) {
		w.failure = errors.New("console output failed")
	}

	return w.failure
}

// colorize wraps an escaped formatter field in a trusted ANSI style and resets before subsequent text.
func colorize(text, code string) string {
	return "\x1b[" + code + "m" + text + "\x1b[0m"
}

// Error returns a latched console failure, including failures from concurrent instrumentation.
func (w *Writer) Error() error {
	if w == nil {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	return w.failure
}

// Diagnostic emits a Go operational warning, never an AI conclusion or incident record.
func (w *Writer) Diagnostic(code, message string) error {
	return w.Log("warn", "GO", code, message)
}

// rank maps validated log levels to filtering priorities; the zero value selects info.
func rank(level string) int {
	switch level {
	case "debug":
		return 0
	case "warn":
		return 2
	case "error":
		return 3
	default:
		return 1
	}
}
