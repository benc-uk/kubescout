/*
Purpose: Retain bounded, private, readable AI interaction transcripts locally.
Flow: Callers supply approved content; fixed-name rotation caps disk retention.
File failures warn and retry without changing investigations or CRD persistence.
Transcript files are diagnostic evidence, not incident storage or model memory.
*/
package output

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type Transcript struct {
	Dir     string
	Console *Writer
	mu      sync.Mutex
	limit   int
	files   int
	failed  bool
	retryAt time.Time
}

const TranscriptFileBytes = 10 * 1024 * 1024
const TranscriptFiles = 10

// NewTranscript selects private rotating storage; the first record tests directory availability.
func NewTranscript(dir string, console *Writer) *Transcript {
	return &Transcript{Dir: dir, Console: console, limit: TranscriptFileBytes, files: TranscriptFiles}
}

// Record appends approved content with correlation, splitting large sections across bounded files.
// Failures mark capture incomplete, throttle retries, and never replace the caller's outcome.
func (t *Transcript) Record(scope, origin, phase, content string) {
	if t == nil {
		return
	}

	t.mu.Lock()

	if time.Now().Before(t.retryAt) {
		t.mu.Unlock()
		return
	}

	wasFailed := t.failed
	if wasFailed {
		content = "[Capture resumed; preceding exchanges may be missing.]\n" + content
	}

	if t.Console != nil {
		content = Redact(content, t.Console.Secrets...)
	}

	err := t.write(scope, origin, phase, content)
	t.failed = err != nil

	if err != nil {
		t.retryAt = time.Now().Add(30 * time.Second)
	} else {
		t.retryAt = time.Time{}
	}

	t.mu.Unlock()

	if err != nil {
		_ = t.Console.Log("warn", "GO", "TRANSCRIPT", "AI capture incomplete: cannot write/rotate/close transcript in "+t.Dir+"; investigations continue; retry in 30s")
	} else if wasFailed {
		_ = t.Console.Log("info", "GO", "TRANSCRIPT", "AI capture resumed; earlier exchanges may be missing")
	}
}

// write opens a confined directory and closes each record before returning, including on failure.
func (t *Transcript) write(scope, origin, phase, content string) error {
	if err := os.MkdirAll(t.Dir, 0700); err != nil {
		return err
	}

	info, err := os.Lstat(t.Dir)
	if err != nil {
		return err
	}

	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("transcript directory must not be a symlink")
	}

	root, err := os.OpenRoot(t.Dir)
	if err != nil {
		return err
	}
	defer root.Close()

	if err := t.checkFiles(root); err != nil {
		return err
	}

	run := ""
	if t.Console != nil {
		run = t.Console.Run
	}

	// Prefix every payload line so model text cannot impersonate a record header.
	content = "  " + strings.ReplaceAll(content, "\n", "\n  ") + "\n"
	for part := 1; len(content) > 0; part++ {
		header := fmt.Sprintf("\n=== %s run=%s %s [%s][%s] part=%d ===\n",
			time.Now().UTC().Format(time.RFC3339Nano), Text(run), Text(scope), Text(origin), Text(phase), part)

		size := min(len(content), t.limit-len(header))
		for size > 0 && size < len(content) && !utf8.RuneStart(content[size]) {
			size--
		}

		if size <= 0 {
			return errors.New("transcript header exceeds file limit")
		}

		record := header + content[:size]
		if err := t.append(root, record); err != nil {
			return err
		}

		content = content[size:]
	}

	return nil
}

// transcriptName returns only writer-owned filenames, never a caller-controlled cleanup path.
func transcriptName(index int) string {
	if index == 0 {
		return "ai-transcript.log"
	}

	return fmt.Sprintf("ai-transcript.log.%d", index)
}

// checkFiles refuses special files and oversized owned files rather than modifying unknown content.
func (t *Transcript) checkFiles(root *os.Root) error {
	for i := range t.files {
		info, err := root.Lstat(transcriptName(i))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}

		if err != nil {
			return err
		}

		if !info.Mode().IsRegular() {
			return errors.New("transcript path is not a regular file")
		}

		if info.Size() > int64(t.limit) {
			return errors.New("existing transcript exceeds retention limit")
		}
	}

	return nil
}

// append rotates before writing, enforces private permissions and detects short writes and close failures.
func (t *Transcript) append(root *os.Root, record string) error {
	name := transcriptName(0)
	info, err := root.Stat(name)

	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	if err == nil && info.Size()+int64(len(record)) > int64(t.limit) {
		if err := t.rotate(root); err != nil {
			return err
		}
	}

	f, err := root.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	if err := f.Chmod(0600); err != nil {
		_ = f.Close()
		return err
	}

	n, writeErr := io.WriteString(f, record)
	closeErr := f.Close()

	if writeErr != nil {
		return writeErr
	}

	if n != len(record) {
		return io.ErrShortWrite
	}

	return closeErr
}

// rotate deletes only the oldest owned file and shifts the remaining bounded siblings.
func (t *Transcript) rotate(root *os.Root) error {
	if err := root.Remove(transcriptName(t.files - 1)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	for i := t.files - 2; i >= 0; i-- {
		if err := root.Rename(transcriptName(i), transcriptName(i+1)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	return nil
}

// Path resolves the transcript directory for readable startup diagnostics.
func (t *Transcript) Path() string {
	path, err := filepath.Abs(t.Dir)
	if err != nil {
		return t.Dir + " (absolute path unavailable)"
	}

	return path
}
