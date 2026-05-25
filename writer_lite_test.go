package filerotate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLiteWriterBasic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")
	w, err := NewLiteWriter(LiteConfig{FilePath: path, PerProcSizeMB: 10, MaxAgeDays: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	msg := "hello\n"
	n, err := w.Write([]byte(msg))
	if err != nil {
		t.Fatal(err)
	}
	if n != len(msg) {
		t.Fatalf("wrote %d, want %d", n, len(msg))
	}
	content, _ := os.ReadFile(path)
	if string(content) != msg {
		t.Fatalf("content %q, want %q", string(content), msg)
	}
}

func TestLiteWriterRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rotate.log")
	w, err := NewLiteWriter(LiteConfig{FilePath: path, PerProcSizeMB: 0, MaxAgeDays: 0})
	if err != nil {
		t.Fatal(err)
	}
	w.perProcSize = 10
	defer w.Close()
	_, err = w.Write([]byte(strings.Repeat("a", 100)))
	if err != nil {
		t.Fatal(err)
	}
	backups, _ := filepath.Glob(path + ".*")
	if len(backups) == 0 {
		t.Fatal("expected backup file")
	}
}

func TestLiteWriterCleanup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "clean.log")
	oldTime := time.Now().AddDate(0, 0, -10)
	for i := 0; i < 3; i++ {
		ts := oldTime.Add(time.Duration(i) * time.Hour).Format("20060102_150405")
		f, _ := os.Create(path + "." + ts)
		f.Close()
		os.Chtimes(path+"."+ts, oldTime, oldTime)
	}
	recent := time.Now().Format("20060102_150405")
	f, _ := os.Create(path + "." + recent)
	f.Close()
	w, err := NewLiteWriter(LiteConfig{FilePath: path, PerProcSizeMB: 10, MaxAgeDays: 7})
	if err != nil {
		t.Fatal(err)
	}
	w.perProcSize = 10
	defer w.Close()
	_, err = w.Write([]byte(strings.Repeat("x", 100)))
	if err != nil {
		t.Fatal(err)
	}
	backups, _ := filepath.Glob(path + ".*")
	for _, b := range backups {
		fi, _ := os.Stat(b)
		if fi.ModTime().Before(time.Now().AddDate(0, 0, -7)) {
			t.Errorf("old backup %s should have been deleted", b)
		}
	}
}

func TestLiteWriterConcurrent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "concurrent.log")
	w, err := NewLiteWriter(LiteConfig{FilePath: path, PerProcSizeMB: 10, MaxAgeDays: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	const goroutines = 5
	const writes = 20
	msg := "test\n"
	errCh := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < writes; j++ {
				if _, err := w.Write([]byte(msg)); err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}()
	}
	for i := 0; i < goroutines; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
	fi, _ := os.Stat(path)
	expected := int64(goroutines * writes * len(msg))
	if fi.Size() != expected {
		t.Fatalf("file size %d, want %d", fi.Size(), expected)
	}
}

func TestLiteWriterEmptyWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.log")
	w, _ := NewLiteWriter(LiteConfig{FilePath: path, PerProcSizeMB: 10, MaxAgeDays: 0})
	defer w.Close()
	n, err := w.Write([]byte{})
	if err != nil {
		t.Fatalf("empty write: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes, got %d", n)
	}
}

func TestLiteWriterMultipleRotations(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "multi-rotate.log")
	w, _ := NewLiteWriter(LiteConfig{FilePath: path, PerProcSizeMB: 0, MaxAgeDays: 0})
	w.perProcSize = 10
	defer w.Close()
	for i := 0; i < 3; i++ {
		w.Write([]byte(strings.Repeat("a", 100)))
	}
	backups, _ := filepath.Glob(path + ".*")
	if len(backups) < 2 {
		t.Fatalf("expected at least 2 backup files, got %d", len(backups))
	}
}

func TestLiteWriterCloseAndWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "close-write.log")
	w, _ := NewLiteWriter(LiteConfig{FilePath: path, PerProcSizeMB: 10, MaxAgeDays: 0})
	w.Close()
	_, err := w.Write([]byte("hello\n"))
	if err == nil {
		t.Fatal("expected error after close")
	}
}

func TestLiteWriterTimeIntervalCheck(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "time-interval.log")
	w, err := NewLiteWriter(LiteConfig{
		FilePath:         path,
		PerProcSizeMB:    100,
		MaxWriteInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.Write([]byte("first\n"))
	time.Sleep(200 * time.Millisecond)
	_, err = w.Write([]byte("second\n"))
	if err != nil {
		t.Fatal(err)
	}
	backups, _ := filepath.Glob(path + ".*")
	if len(backups) == 0 {
		t.Fatal("expected backup file after time interval check")
	}
}
