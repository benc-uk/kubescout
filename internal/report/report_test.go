package report

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"k8s-ops-agent/internal/incident"
)

func TestOutputSeparation(t *testing.T) {
	var stdout, stderr bytes.Buffer
	path := filepath.Join(t.TempDir(), "result.json")
	w := Writer{Out: &stdout, Err: &stderr, File: path}
	if err := w.Diagnostic("collection_unknown", "watch renewal"); err != nil {
		t.Fatal(err)
	}
	if stdout.Len() != 0 || stderr.Len() == 0 {
		t.Fatal("diagnostic reached incident output")
	}
	result := Result{"3.0", "execution", "completed", []incident.Change{}, []string{}}
	if err := w.WriteResult(result); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, stdout.Bytes()) {
		t.Fatal("file differs from stdout")
	}
	var decoded Result
	if json.Unmarshal(data, &decoded) != nil || decoded.Changes == nil || decoded.SchemaVersion != "3.0" {
		t.Fatal("invalid no-action output")
	}
	stat, _ := os.Stat(path)
	if stat.Mode().Perm() != 0600 {
		t.Fatal("output file not private")
	}
	w.File = filepath.Join(t.TempDir(), "missing", "out.json")
	stdout.Reset()
	if w.WriteResult(result) == nil {
		t.Fatal("file failure hidden")
	}
	if json.Unmarshal(stdout.Bytes(), &decoded) != nil || decoded.Status != "incomplete" {
		t.Fatal("file failure not reflected")
	}
	w = Writer{Out: failedWriter{}, Err: failedWriter{}}
	if w.WriteResult(result) == nil || w.Diagnostic("test", "test") == nil {
		t.Fatal("output failure hidden")
	}
}

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, errors.New("closed") }
