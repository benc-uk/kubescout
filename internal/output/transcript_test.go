/*
Purpose: Verify private AI transcript rotation, retention and visible capture gaps.
Flow: Offline temporary files exercise exact production limits, chunking,
restart, unsafe paths, console-colour isolation and recoverable failures without model or cluster calls.
Read-only proposal files share private rotation without altering transcripts; required output failures propagate.
Optional normal-mode proposal capture reports throttled gaps and resumes without stopping persistence.
*/
package output

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// TestTranscriptConsoleColourIsolation verifies console styling never enters captured AI content.
func TestTranscriptConsoleColourIsolation(t *testing.T) {
	var out, diag bytes.Buffer

	console := &Writer{Out: &out, Err: &diag, Color: true, Run: "fixture"}
	if err := console.Log("info", "AI", "DECISION", "fixture response"); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	trace := NewTranscript(dir, console)
	trace.Record("inv=1", "AI", "RESPONSE", "fixture response")

	data, err := os.ReadFile(filepath.Join(dir, transcriptName(0)))
	if err != nil || trace.failed || !bytes.Contains(data, []byte("[AI][RESPONSE]")) ||
		bytes.Contains(data, []byte("\x1b")) || !strings.Contains(out.String(), "\x1b[35m") {
		t.Fatal("transcript inherited console styling or failed capture")
	}
}

// TestTranscriptRetention checks the real ten-file/100 MiB ceiling and private file permissions.
func TestTranscriptRetention(t *testing.T) {
	dir := t.TempDir()
	trace := NewTranscript(dir, nil)

	for range TranscriptFiles + 2 {
		trace.Record("inv=1", "AI", "RESPONSE", "fixture response")

		if trace.failed {
			t.Fatal("capture failed")
		}

		if err := os.Truncate(filepath.Join(dir, transcriptName(0)), TranscriptFileBytes); err != nil {
			t.Fatal(err)
		}
	}

	trace = NewTranscript(dir, nil)
	trace.Record("inv=2", "GO", "RUN.START", "restart")

	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != TranscriptFiles {
		t.Fatalf("retention count=%d error=%v", len(entries), err)
	}

	var total int64

	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || info.Size() > TranscriptFileBytes || info.Mode().Perm() != 0600 {
			t.Fatalf("unbounded or nonprivate file: %v", err)
		}

		total += info.Size()
	}

	if total > TranscriptFiles*TranscriptFileBytes {
		t.Fatalf("retained %d bytes", total)
	}
}

// TestTranscriptChunking checks large sections retain correlated chunks without silent truncation.
func TestTranscriptChunking(t *testing.T) {
	dir := t.TempDir()
	trace := NewTranscript(dir, nil)
	trace.limit = 512
	trace.Record("inv=7", "AI", "RESPONSE", strings.Repeat("\u00e9", 900)+"end-marker")

	var combined strings.Builder

	for i := trace.files - 1; i >= 0; i-- {
		data, err := os.ReadFile(filepath.Join(dir, transcriptName(i)))
		if os.IsNotExist(err) {
			continue
		}

		if err != nil || len(data) > trace.limit || !utf8.Valid(data) || !strings.Contains(string(data), "inv=7") {
			t.Fatal("chunk missing correlation or exceeds limit")
		}

		combined.Write(data)
	}

	if !strings.Contains(combined.String(), "part=2") || !strings.Contains(combined.String(), "end-marker") ||
		strings.Count(combined.String(), "\u00e9") != 900 {
		t.Fatal("transcript silently truncated")
	}
}

// TestTranscriptFailureRecovery checks warnings, retry throttling and resumption without losing unrelated files.
func TestTranscriptFailureRecovery(t *testing.T) {
	base := t.TempDir()

	path := filepath.Join(base, "blocked")
	if err := os.WriteFile(path, []byte("unrelated"), 0600); err != nil {
		t.Fatal(err)
	}

	var out, diag bytes.Buffer

	trace := NewTranscript(path, &Writer{Out: &out, Err: &diag})
	trace.Record("inv=1", "GO", "REQUEST", "first")
	trace.Record("inv=1", "AI", "RESPONSE", "second")

	if !trace.failed || strings.Count(diag.String(), "AI capture incomplete") != 1 || trace.Console.Error() != nil {
		t.Fatal("capture failure hidden, flooding, or fatal")
	}

	data, _ := os.ReadFile(path)
	if string(data) != "unrelated" {
		t.Fatal("capture altered unrelated file")
	}

	trace.Dir = filepath.Join(base, "working")
	trace.retryAt = time.Time{}
	trace.Record("inv=2", "GO", "REQUEST", "recovered")

	if trace.failed || !strings.Contains(out.String(), "AI capture resumed") {
		t.Fatal("capture did not recover")
	}
}

// TestTranscriptRefusesSymlink checks rotation cannot overwrite an unrelated symlink target.
func TestTranscriptRefusesSymlink(t *testing.T) {
	dir := t.TempDir()

	target := filepath.Join(t.TempDir(), "keep")
	if err := os.WriteFile(target, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}

	if err := os.Symlink(target, filepath.Join(dir, transcriptName(0))); err != nil {
		t.Fatal(err)
	}

	trace := NewTranscript(dir, nil)
	trace.Record("", "GO", "REQUEST", "must not write")

	data, _ := os.ReadFile(target)

	if !trace.failed || string(data) != "keep" {
		t.Fatal("symlink protection failed")
	}
}

// TestTranscriptRotationFailure checks interrupted capture preserves earlier content and resumes with a gap marker.
func TestTranscriptRotationFailure(t *testing.T) {
	dir := t.TempDir()

	var out, diag bytes.Buffer

	trace := NewTranscript(dir, &Writer{Out: &out, Err: &diag})
	trace.limit = 512
	trace.Record("inv=1", "GO", "REQUEST", "original capture")

	if err := os.Mkdir(filepath.Join(dir, transcriptName(1)), 0700); err != nil {
		t.Fatal(err)
	}

	trace.Record("inv=1", "AI", "RESPONSE", "missing response")

	data, err := os.ReadFile(filepath.Join(dir, transcriptName(0)))
	if err != nil || !trace.failed || !strings.Contains(string(data), "original capture") ||
		strings.Contains(string(data), "missing response") || !strings.Contains(diag.String(), "incomplete") {
		t.Fatal("mid-investigation storage failure hidden or earlier capture overwritten")
	}

	if err := os.Remove(filepath.Join(dir, transcriptName(1))); err != nil {
		t.Fatal(err)
	}

	trace.retryAt = time.Time{}
	trace.Record("inv=2", "GO", "REQUEST", "new request")

	data, err = os.ReadFile(filepath.Join(dir, transcriptName(0)))
	if err != nil || trace.failed || !strings.Contains(string(data), "preceding exchanges may be missing") {
		t.Fatal("resumed transcript omitted its capture gap")
	}
}

// TestProposalLogRetentionAndIsolation checks the real private ten-file ceiling,
// credential redaction and separation from pre-existing AI transcript files.
func TestProposalLogRetentionAndIsolation(t *testing.T) {
	dir := t.TempDir()

	transcript := filepath.Join(dir, transcriptName(0))
	if err := os.WriteFile(transcript, []byte("retain-transcript"), 0600); err != nil {
		t.Fatal(err)
	}

	log := NewProposalLog(dir, &Writer{Secrets: []string{"fixture-credential"}})
	for range TranscriptFiles + 2 {
		if err := log.Write("inv=1", "diagnosis: fixture-credential"); err != nil {
			t.Fatal(err)
		}

		if err := os.Truncate(log.Path(), TranscriptFileBytes); err != nil {
			t.Fatal(err)
		}
	}

	log = NewProposalLog(dir, &Writer{Secrets: []string{"fixture-credential"}})
	if err := log.Write("inv=2", "proposal: fixture-credential"); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(transcript)
	if err != nil || string(data) != "retain-transcript" {
		t.Fatal("proposal rotation altered the AI transcript")
	}

	files, err := filepath.Glob(filepath.Join(dir, "ai-proposals.log*"))
	if err != nil || len(files) != TranscriptFiles {
		t.Fatal("proposal retention exceeded or lost its bounded file family")
	}

	var total int64

	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil || info.Size() > TranscriptFileBytes || info.Mode().Perm() != 0600 {
			t.Fatal("proposal log is unbounded or not private")
		}

		total += info.Size()
	}

	data, err = os.ReadFile(log.Path())
	if err != nil || bytes.Contains(data, []byte("fixture-credential")) ||
		!bytes.Contains(data, []byte("[redacted credential]")) || total > TranscriptFiles*TranscriptFileBytes {
		t.Fatal("proposal diagnostics leaked credentials or exceeded retention")
	}
}

// TestProposalCaptureFailureRecovery shares optional capture retry warnings and gap markers
// while keeping failed or throttled writes distinguishable from completed proposal output.
func TestProposalCaptureFailureRecovery(t *testing.T) {
	base := t.TempDir()

	blocked := filepath.Join(base, "blocked")
	if err := os.WriteFile(blocked, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}

	var out, diag bytes.Buffer

	log := NewProposalLog(blocked, &Writer{Out: &out, Err: &diag})
	if log.Capture("inv=1", "first") || log.Capture("inv=2", "throttled") ||
		strings.Count(diag.String(), "AI proposal capture incomplete") != 1 || log.storage.Console.Error() != nil {
		t.Fatal("optional proposal capture hid failure, flooded warnings or claimed success")
	}

	log.storage.Dir = filepath.Join(base, "working")

	log.storage.retryAt = time.Time{}
	if !log.Capture("inv=3", "recovered") {
		t.Fatal("optional proposal capture did not resume")
	}

	data, err := os.ReadFile(log.Path())
	if err != nil || !bytes.Contains(data, []byte("Capture resumed; preceding exchanges may be missing")) ||
		!strings.Contains(out.String(), "AI proposal capture resumed") {
		t.Fatal("proposal capture recovery hid the preceding gap")
	}
}

// TestProposalLogUnsafePathFails refuses symlinks and preserves unrelated content
// while returning a required-output failure rather than silently dropping a proposal.
func TestProposalLogUnsafePathFails(t *testing.T) {
	dir := t.TempDir()

	target := filepath.Join(t.TempDir(), "keep")
	if err := os.WriteFile(target, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}

	log := NewProposalLog(dir, nil)
	if err := os.Symlink(target, log.Path()); err != nil {
		t.Fatal(err)
	}

	if err := log.Write("inv=1", "must-not-write"); err == nil {
		t.Fatal("proposal log ignored a write failure")
	}

	data, err := os.ReadFile(target)
	if err != nil || string(data) != "keep" {
		t.Fatal("proposal logging changed an unrelated file")
	}
}
