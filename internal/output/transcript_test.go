/*
Purpose: Verify private AI transcript rotation, retention and visible capture gaps.
Flow: Offline temporary files exercise exact production limits, chunking,
restart, unsafe paths, console-colour isolation and recoverable failures without model or cluster calls.
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
