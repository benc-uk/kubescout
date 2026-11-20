/*
Purpose: Write private rotating AI proposal diagnostics in either execution mode.
Flow: The runner supplies validated, fresh proposal text; shared confined
rotation redacts diagnostics. Normal capture failures warn and retry without blocking
Incident persistence; required read-only output failures return to the caller.
These files are neither Incident records nor replayable incident/model state.
*/
package output

import (
	"errors"
	"path/filepath"
)

type ProposalLog struct {
	storage *Transcript
}

// NewProposalLog selects a separate fixed-name diagnostic log using existing private rotation limits.
func NewProposalLog(dir string, console *Writer) *ProposalLog {
	storage := NewTranscript(dir, console)
	storage.filename = "ai-proposals.log"

	return &ProposalLog{storage: storage}
}

// Write appends redacted proposal diagnostics, returning failure rather than claiming they were logged.
func (p *ProposalLog) Write(scope, content string) error {
	if p == nil || p.storage == nil {
		return errors.New("read-only proposal logging is not configured")
	}

	t := p.storage

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Console != nil {
		content = Redact(content, t.Console.Secrets...)
	}

	if err := t.write(scope, "GO", "PROPOSAL", content); err != nil {
		return errors.New("cannot write/rotate/close read-only proposal log; assessment incomplete")
	}

	return nil
}

// Capture appends optional diagnostics with bounded retry warnings, reporting only completed capture.
func (p *ProposalLog) Capture(scope, content string) bool {
	if p == nil || p.storage == nil {
		return false
	}

	return p.storage.record(scope, "GO", "PROPOSAL", content)
}

// Path returns the diagnostic filename for operator startup guidance.
func (p *ProposalLog) Path() string {
	return filepath.Join(p.storage.Path(), p.storage.filename)
}
