package filerotate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWriterBasic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")
	w, err := New(Config{FilePath: path, MaxSizeMB: 10, MaxAgeDays: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	msg := "hello world\n"
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

func TestWriterRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rotate.log")
	w, err := New(Config{FilePath: path, MaxSizeMB: 0, CheckInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.maxSize = 10
	w.checkInterval = 50 * time.Millisecond
	go w.runLeader()
	time.Sleep(200 * time.Millisecond)
	data := strings.Repeat("a", 100)
	_, err = w.Write([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	backups, _ := filepath.Glob(path + ".*")
	if len(backups) == 0 {
		t.Fatal("expected backup files")
	}
}

func TestWriterCleanup(t *testing.T) {
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
	w, err := New(Config{FilePath: path, MaxSizeMB: 0, MaxAgeDays: 7, CheckInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.maxSize = 10
	go w.runLeader()
	time.Sleep(200 * time.Millisecond)
	_, err = w.Write([]byte(strings.Repeat("x", 100)))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	backups, _ := filepath.Glob(path + ".*")
	for _, b := range backups {
		fi, _ := os.Stat(b)
		if fi.ModTime().Before(time.Now().AddDate(0, 0, -7)) {
			t.Errorf("old backup %s should have been deleted", b)
		}
	}
}

func TestWriterLeaderFailover(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "failover.log")
	w1, _ := New(Config{FilePath: path, MaxSizeMB: 0, CheckInterval: 50 * time.Millisecond})
	w1.maxSize = 10
	w2, _ := New(Config{FilePath: path, MaxSizeMB: 0, CheckInterval: 50 * time.Millisecond})
	w2.maxSize = 10
	w1.Close()
	time.Sleep(500 * time.Millisecond)
	_, err := w2.Write([]byte("hello after failover\n"))
	if err != nil {
		t.Fatalf("write after failover: %v", err)
	}
	w2.Close()
}

func TestWriterConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "concurrent-write.log")
	w, err := New(Config{FilePath: path, MaxSizeMB: 100, MaxAgeDays: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	const goroutines = 10
	const writesPer = 50
	msg := "concurrent test message\n"
	errCh := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < writesPer; j++ {
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
	expected := int64(goroutines * writesPer * len(msg))
	if fi.Size() != expected {
		t.Fatalf("file size %d, want %d", fi.Size(), expected)
	}
}
