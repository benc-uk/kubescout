/*
Purpose: Encode schema 3.0 execution and incident-change output separately from diagnostics.
Flow: Serialize supplied results to stdout and optionally an atomically replaced
file, marking output failures incomplete while sending diagnostics to stderr.
This output layer neither evaluates evidence nor decides incident lifecycle changes.
*/
package output

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/benc-uk/kubescout/internal/incident"
)

type Result struct {
	SchemaVersion string            `json:"schema_version"`
	Type          string            `json:"type"`
	Status        string            `json:"execution_status"`
	Changes       []incident.Change `json:"changes"`
	Errors        []string          `json:"errors"`
}

type Writer struct {
	Out, Err io.Writer
	File     string
}

// Diagnostic writes a timestamped diagnostic to stderr without touching incident output.
func (w Writer) Diagnostic(code, message string) error {
	if err := json.NewEncoder(w.Err).Encode(struct {
		Type    string    `json:"type"`
		Code    string    `json:"code"`
		Message string    `json:"message"`
		At      time.Time `json:"at"`
	}{"diagnostic", code, message, time.Now().UTC()}); err != nil {
		return errors.New("diagnostic output failed")
	}

	return nil
}

// WriteChange wraps a supplied accepted incident change in a completed schema 3.0 envelope.
func (w Writer) WriteChange(c incident.Change) error {
	return w.WriteResult(Result{"3.0", "incident_change", "completed", []incident.Change{c}, []string{}})
}

// WriteResult emits the envelope to stdout and optionally replaces the output file.
// Failed destinations are reported as incomplete in the remaining writable destination.
func (w Writer) WriteResult(r Result) error {
	var fileErr error
	if w.File != "" {
		fileErr = atomicWrite(w.File, r)
		if fileErr != nil {
			r.Status = "incomplete"
			r.Errors = append(r.Errors, "output file write failed")
		}
	}

	if err := json.NewEncoder(w.Out).Encode(r); err != nil {
		if w.File != "" && fileErr == nil {
			r.Status = "incomplete"

			r.Errors = append(r.Errors, "stdout output failed")
			if err := atomicWrite(w.File, r); err != nil {
				return errors.New("stdout and output file write failed")
			}
		}

		return errors.New("stdout output failed")
	}

	if fileErr != nil {
		return errors.New("output file write failed")
	}

	return nil
}

// atomicWrite encodes and syncs private JSON beside the destination, then replaces it by rename.
// Failed writes leave the destination unchanged and remove the staging file.
func atomicWrite(path string, value any) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".kubescout-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())

	if err := json.NewEncoder(f).Encode(value); err != nil {
		_ = f.Close()
		return err
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(f.Name(), path)
}
