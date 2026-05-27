// writer_lite_test.go
// Lite 模式 Writer 的单元测试。
// 所有日志文件均写入 ./example/log 目录，避免系统临时目录的权限问题。
package filerotate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestLiteWriterBasic 验证基本写入功能。
func TestLiteWriterBasic(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestLiteWriterBasic.log")
	defer cleanupLogs(t, path)

	w, err := NewLite(LiteConfig{FilePath: path, MaxSizeMB: 10, MaxAgeDays: 0})
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

// TestLiteWriterRotation 验证写入超过阈值后经 notifier 轮询触发轮转。
func TestLiteWriterRotation(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestLiteWriterRotation.log")
	defer cleanupLogs(t, path)

	w, err := NewLite(LiteConfig{FilePath: path, MaxSizeMB: 0, MaxAgeDays: 0, PollInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.maxSize = 10
	defer w.Close()

	// 写入超过阈值的数据
	_, err = w.Write([]byte(strings.Repeat("a", 100)))
	if err != nil {
		t.Fatal(err)
	}

	// 等待 notifier 轮询检测到文件大小超阈值并发送 ROTATE
	time.Sleep(200 * time.Millisecond)

	// 再次写入，Write() 会收到 ROTATE 信号并执行轮转
	_, err = w.Write([]byte("trigger\n"))
	if err != nil {
		t.Fatal(err)
	}

	// 检查备份文件
	base := filepath.Base(path)
	matches, _ := filepath.Glob(path + ".*")
	found := false
	for _, m := range matches {
		if isBackupFile(base, filepath.Base(m)) {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected backup file")
	}
}

// TestLiteWriterCleanup 验证过期备份的自动删除。
func TestLiteWriterCleanup(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestLiteWriterCleanup.log")
	defer cleanupLogs(t, path)

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

	w, err := NewLite(LiteConfig{FilePath: path, MaxSizeMB: 0, MaxAgeDays: 7, PollInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.maxSize = 10
	defer w.Close()

	_, err = w.Write([]byte(strings.Repeat("x", 100)))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	_, err = w.Write([]byte("trigger\n"))
	if err != nil {
		t.Fatal(err)
	}

	backups, _ := filepath.Glob(path + ".*")
	for _, b := range backups {
		if !isBackupFile(filepath.Base(path), filepath.Base(b)) {
			continue
		}
		fi, _ := os.Stat(b)
		if fi.ModTime().Before(time.Now().AddDate(0, 0, -7)) {
			t.Errorf("old backup %s should have been deleted", b)
		}
	}
}

// TestLiteWriterConcurrent 验证多 goroutine 并发写入的安全性。
func TestLiteWriterConcurrent(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestLiteWriterConcurrent.log")
	defer cleanupLogs(t, path)

	w, err := NewLite(LiteConfig{FilePath: path, MaxSizeMB: 10, MaxAgeDays: 0})
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

// TestLiteWriterEmptyWrite 验证空写入。
func TestLiteWriterEmptyWrite(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestLiteWriterEmptyWrite.log")
	defer cleanupLogs(t, path)

	w, err := NewLite(LiteConfig{FilePath: path, MaxSizeMB: 10, MaxAgeDays: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	n, err := w.Write([]byte{})
	if err != nil {
		t.Fatalf("empty write: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes, got %d", n)
	}
}

// TestLiteWriterMultipleRotations 验证多次轮转。
func TestLiteWriterMultipleRotations(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestLiteWriterMultipleRotations.log")
	defer cleanupLogs(t, path)

	w, err := NewLite(LiteConfig{FilePath: path, MaxSizeMB: 0, MaxAgeDays: 0, PollInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.maxSize = 10

	for i := 0; i < 3; i++ {
		_, err := w.Write([]byte(strings.Repeat("a", 100)))
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		// 等待 notifier 检测并发送 ROTATE，下一次 Write 执行轮转
		time.Sleep(200 * time.Millisecond)
	}

	base := filepath.Base(path)
	matches, _ := filepath.Glob(path + ".*")
	count := 0
	for _, m := range matches {
		if isBackupFile(base, filepath.Base(m)) {
			count++
		}
	}
	if count < 2 {
		t.Fatalf("expected at least 2 backup files, got %d", count)
	}
}

// TestLiteWriterCloseAndWrite 验证关闭后写入应报错。
func TestLiteWriterCloseAndWrite(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestLiteWriterCloseAndWrite.log")
	defer cleanupLogs(t, path)

	w, err := NewLite(LiteConfig{FilePath: path, MaxSizeMB: 10, MaxAgeDays: 0})
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	_, err = w.Write([]byte("hello\n"))
	if err == nil {
		t.Fatal("expected error after close")
	}
}

// TestLiteWriterPollingDetection 验证 notifier 轮询检测到其他进程轮转后，本进程能重开文件。
func TestLiteWriterPollingDetection(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestLiteWriterPollingDetection.log")
	defer cleanupLogs(t, path)

	w, err := NewLite(LiteConfig{
		FilePath:     path,
		MaxSizeMB:    0,
		MaxAgeDays:   0,
		PollInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.maxSize = 10

	// 写入超过阈值
	_, err = w.Write([]byte(strings.Repeat("a", 100)))
	if err != nil {
		t.Fatal(err)
	}

	// 等待 notifier 检测到大小超阈值并发送 ROTATE
	time.Sleep(200 * time.Millisecond)

	// 再次写入触发轮转
	_, err = w.Write([]byte("after rotation\n"))
	if err != nil {
		t.Fatal(err)
	}

	// 当前文件应该只有 "after rotation\n"
	content, _ := os.ReadFile(path)
	if !strings.Contains(string(content), "after rotation") {
		t.Fatal("new file should contain data written after rotation")
	}
}
