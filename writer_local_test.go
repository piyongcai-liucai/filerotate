// writer_local_test.go
// 单机模式 Writer 的单元测试。
// 所有日志文件均写入 ./example/log 目录，避免系统临时目录的权限问题。
package filerotate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestLocalWriterBasic 验证基本写入功能。
func TestLocalWriterBasic(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestLocalWriterBasic.log")
	defer cleanupLogs(t, path)

	w, err := New(Config{FilePath: path, MaxSizeMB: 10, MaxAgeDays: 0})
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

// TestLocalWriterRotation 验证写入超过阈值后经 notifier 轮询触发轮转。
func TestLocalWriterRotation(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestLocalWriterRotation.log")
	defer cleanupLogs(t, path)

	w, err := New(Config{FilePath: path, MaxSizeMB: 0, MaxAgeDays: 0, CheckInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.maxSize = 10 // 测试用极小值，int64 在现代平台上原子读写
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

// TestLocalWriterCleanup 验证过期备份的自动删除。
func TestLocalWriterCleanup(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestLocalWriterCleanup.log")
	defer cleanupLogs(t, path)

	oldTime := time.Now().AddDate(0, 0, -10)
	for i := 0; i < 3; i++ {
		ts := oldTime.Add(time.Duration(i) * time.Hour).Format("20060102_150405")
		f, err := os.Create(path + "." + ts)
		if err != nil {
			t.Fatalf("创建备份文件失败: %v", err)
		}
		f.Close()
		if err := os.Chtimes(path+"."+ts, oldTime, oldTime); err != nil {
			t.Fatalf("设置备份文件时间失败: %v", err)
		}
	}

	recent := time.Now().Format("20060102_150405")
	f, err := os.Create(path + "." + recent)
	if err != nil {
		t.Fatalf("创建备份文件失败: %v", err)
	}
	f.Close()

	w, err := New(Config{FilePath: path, MaxSizeMB: 0, MaxAgeDays: 7, CheckInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.maxSize = 10 // 测试用极小值，int64 在现代平台上原子读写
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

// TestLocalWriterConcurrent 验证多 goroutine 并发写入的安全性。
func TestLocalWriterConcurrent(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestLocalWriterConcurrent.log")
	defer cleanupLogs(t, path)

	w, err := New(Config{FilePath: path, MaxSizeMB: 10, MaxAgeDays: 0})
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

// TestLocalWriterEmptyWrite 验证空写入。
func TestLocalWriterEmptyWrite(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestLocalWriterEmptyWrite.log")
	defer cleanupLogs(t, path)

	w, err := New(Config{FilePath: path, MaxSizeMB: 10, MaxAgeDays: 0})
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

// TestLocalWriterMultipleRotations 验证多次轮转。
func TestLocalWriterMultipleRotations(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestLocalWriterMultipleRotations.log")
	defer cleanupLogs(t, path)

	w, err := New(Config{FilePath: path, MaxSizeMB: 0, MaxAgeDays: 0, CheckInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.maxSize = 10 // 测试用极小值，int64 在现代平台上原子读写

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

// TestLocalWriterCloseAndWrite 验证关闭后写入应报错。
func TestLocalWriterCloseAndWrite(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestLocalWriterCloseAndWrite.log")
	defer cleanupLogs(t, path)

	w, err := New(Config{FilePath: path, MaxSizeMB: 10, MaxAgeDays: 0})
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	_, err = w.Write([]byte("hello\n"))
	if err == nil {
		t.Fatal("expected error after close")
	}
}

// TestLocalWriterPollingDetection 验证 notifier 轮询检测到其他进程轮转后，本进程能重开文件。
func TestLocalWriterPollingDetection(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestLocalWriterPollingDetection.log")
	defer cleanupLogs(t, path)

	w, err := New(Config{
		FilePath:     path,
		MaxSizeMB:    0,
		MaxAgeDays:   0,
		CheckInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.maxSize = 10 // 测试用极小值，int64 在现代平台上原子读写

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

// ---------- 单机模式文件重建 / 外部轮转检测测试 ----------

// TestLocalWriter_FileRemovedRecreates 验证文件被外部删除后 Write() 能通过 reopenFile 重建。
func TestLocalWriter_FileRemovedRecreates(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, t.Name()+".log")
	defer cleanupLogs(t, path)

	w, err := New(Config{FilePath: path, MaxSizeMB: 10, MaxAgeDays: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// 写入数据后删除文件
	w.Write([]byte("data\n"))
	time.Sleep(50 * time.Millisecond)
	os.Remove(path)

	// 手动触发轮转信号 — Write() 收到信号后调用 reopenFile（O_CREATE 重建文件）
	w.rotateCh <- struct{}{}
	time.Sleep(100 * time.Millisecond)

	// Write 应能正常工作（文件已重建）
	_, err = w.Write([]byte("after removal\n"))
	if err != nil {
		t.Fatalf("write after file removal: %v", err)
	}

	content, _ := os.ReadFile(path)
	if !strings.Contains(string(content), "after removal") {
		t.Fatal("file should be recreated")
	}
}

// TestLocalWriter_ExternalRotationReopen 验证外部轮转后 Write() 能重开新文件。
func TestLocalWriter_ExternalRotationReopen(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, t.Name()+".log")
	defer cleanupLogs(t, path)

	w, err := New(Config{FilePath: path, MaxSizeMB: 10, MaxAgeDays: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	w.Write([]byte("data\n"))
	time.Sleep(50 * time.Millisecond)

	// 模拟外部轮转：重命名旧文件 + 创建新文件
	backupPath := path + "." + time.Now().Format("20060102_150405")
	os.Rename(path, backupPath)
	defer os.Remove(backupPath)
	f, _ := os.Create(path)
	f.Close()

	// 手动触发轮转信号 — Write() 收到后调用 reopenFile 重开新文件
	w.rotateCh <- struct{}{}
	time.Sleep(100 * time.Millisecond)

	// Write 应正常工作（已重开新文件）
	_, err = w.Write([]byte("after external rotation\n"))
	if err != nil {
		t.Fatalf("write after external rotation: %v", err)
	}
}

// TestLocalWriter_TryLockError 验证 runLocalLoop 中 rotateLocker.TryLock 错误时的处理。
func TestLocalWriter_TryLockError(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, t.Name()+".log")
	defer cleanupLogs(t, path)

	// 写入足够大的数据触发 size 检查
	if err := os.WriteFile(path, []byte(strings.Repeat("a", 200)), 0o644); err != nil {
		t.Fatal(err)
	}

	f, err := openFileAppend(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var capturedErr error
	var capturedMsg string
	w := &Writer{
		local:         true,
		file:          f,
		filePath:      path,
		maxSize:       10,
		checkInterval: 50 * time.Millisecond,
		rotateLocker:  &errorLocker{},
		rotateCh:      make(chan struct{}, 1),
		done:          make(chan struct{}),
		errorHandler:  func(err error) { capturedErr = err; capturedMsg = err.Error() },
	}

	cmdCh := make(chan string, 1)
	cmdCh <- CmdRotate
	close(cmdCh)

	w.wg.Add(1)
	w.runLocalLoop(cmdCh)

	if capturedErr == nil {
		t.Fatal("expected error to be reported from TryLock")
	}
	if !strings.Contains(capturedMsg, "mock error") {
		t.Fatalf("expected mock lock error, got: %v", capturedErr)
	}
}

// TestLocalWriter_TwoWritersCoexist 验证两个单机 Writer 共存且各自独立工作。
func TestLocalWriter_TwoWritersCoexist(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, t.Name()+".log")
	defer cleanupLogs(t, path)

	w1, err := New(Config{FilePath: path, MaxSizeMB: 10, MaxAgeDays: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer w1.Close()

	w2, err := New(Config{FilePath: path, MaxSizeMB: 10, MaxAgeDays: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	// 两者都能正常写入
	_, err = w1.Write([]byte("from w1\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = w2.Write([]byte("from w2\n"))
	if err != nil {
		t.Fatal(err)
	}
}

