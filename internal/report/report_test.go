package report

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"k8s-ops-agent/internal/health"
)

func TestDiffIncludesEvidenceAndResolution(t *testing.T) {
	f := health.NewFinding("test", "warning", health.Resource{Name: "pod", UID: "id"}, "", "bad", "old")
	g := f
	g.Evidence = []string{"new"}
	if c := Diff(nil, []health.Finding{f}); len(c.New) != 1 {
		t.Fatal(c)
	}
	if c := Diff([]health.Finding{f}, []health.Finding{g}); len(c.Updated) != 1 {
		t.Fatal(c)
	}
	if c := Diff([]health.Finding{g}, nil); len(c.Resolved) != 1 {
		t.Fatal(c)
	}
	if !Diff([]health.Finding{g}, []health.Finding{g}).Empty() {
		t.Fatal("unchanged finding reported")
	}
}

func TestWriter(t *testing.T) {
	var out bytes.Buffer
	path := filepath.Join(t.TempDir(), "report.json")
	w := Writer{Out: &out, File: path}
	r := New("test", []string{"apps"}, time.Now())
	if err := w.Write(r); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, out.Bytes()) || bytes.Count(data, []byte("\n")) != 1 {
		t.Fatal("expected matching one-line JSON")
	}
	stat, _ := os.Stat(path)
	if stat.Mode().Perm() != 0600 {
		t.Fatal("report file must be private")
	}
	out.Reset()
	w.File = filepath.Join(t.TempDir(), "absent", "report.json")
	if err := w.Write(r); err == nil {
		t.Fatal("missing directory did not fail")
	}
	var failed Report
	if err := json.Unmarshal(out.Bytes(), &failed); err != nil {
		t.Fatal(err)
	}
	if failed.Execution != "incomplete" || len(failed.Errors) == 0 {
		t.Fatal("stdout hid file error")
	}
	w = Writer{Out: failingWriter{}, File: path}
	if err := w.Write(r); err == nil {
		t.Fatal("stdout failure not returned")
	}
	data, _ = os.ReadFile(path)
	if err := json.Unmarshal(data, &failed); err != nil {
		t.Fatal(err)
	}
	if failed.Execution != "incomplete" {
		t.Fatal("file hid stdout failure")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("closed") }
