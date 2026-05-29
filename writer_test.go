// writer_test.go
// Writer 的单元测试。
package filerotate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const logDir = "./log"

// silentErrors 静默丢弃错误，避免测试中预期的错误输出污染测试日志。
func silentErrors(_ error) {}

func ensureLogDir(t *testing.T) {
	t.Helper()
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("创建日志目录失败: %v", err)
	}
}

// cleanupLogs 清理指定测试生成的日志文件、备份文件和锁文件
func cleanupLogs(t *testing.T, path string) {
	t.Helper()
	// 删除主日志文件
	os.Remove(path)
	// 删除所有备份文件（仅匹配带时间戳的文件）
	backups, _ := filepath.Glob(path + ".2*") // 备份文件以 ".2" 开头（年份）
	for _, b := range backups {
		os.Remove(b)
	}
	// 删除锁文件
	os.Remove(path + ".lock")
	os.Remove(path + ".leader.lock")
}

// isBackupFile 判断文件名是否为合法的备份文件。
// 支持两种格式: <base>.20060102_150405 (旧格式) 和 <base>.20060102_150405.000000000 (新格式，纳秒精度)
func isBackupFile(base, name string) bool {
	ext := strings.TrimPrefix(name, base+".")
	switch len(ext) {
	case 15: // 旧格式: 20060102_150405
		return ext[8] == '_'
	case 25: // 新格式: 20060102_150405.000000000
		return ext[8] == '_' && ext[15] == '.'
	default:
		return false
	}
}

// TestWriterBasic 验证基本写入功能。
func TestWriterBasic(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestWriterBasic.log")
	defer cleanupLogs(t, path)

	w, err := New(Config{
		FilePath:     path,
		MaxSizeMB:    10,
		MaxAgeDays:   0,
		ErrorHandler: silentErrors,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		w.Close()
		time.Sleep(200 * time.Millisecond)
	}()

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

// TestWriterRotation 验证文件大小达到阈值后能够自动轮转。
func TestWriterRotation(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestWriterRotation.log")
	defer cleanupLogs(t, path)

	w, err := New(Config{
		FilePath:      path,
		MaxSizeMB:     1,
		CheckInterval: 50 * time.Millisecond,
		ErrorHandler:  silentErrors,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		w.Close()
		time.Sleep(200 * time.Millisecond)
	}()

	// 覆盖 maxSize 为 10 字节，使小数据量写入也能触发轮转
	w.maxSize = 10 // 测试用极小值，int64 在现代平台上原子读写
	time.Sleep(200 * time.Millisecond)

	data := strings.Repeat("a", 100)
	_, err = w.Write([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	// 检查真正的备份文件（过滤掉锁文件）
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
		t.Fatal("expected backup files, but none found")
	}
}

// TestWriterCleanup 验证过期备份文件的自动清理。
func TestWriterCleanup(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestWriterCleanup.log")
	defer cleanupLogs(t, path)

	// 创建3个旧备份文件，修改时间为10天前
	oldTime := time.Now().AddDate(0, 0, -10)
	for i := range 3 {
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

	w, err := New(Config{
		FilePath:      path,
		MaxSizeMB:     1,
		MaxAgeDays:    7,
		CheckInterval: 50 * time.Millisecond,
		ErrorHandler:  silentErrors,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		w.Close()
		time.Sleep(200 * time.Millisecond)
	}()

	w.maxSize = 10 // 测试用极小值，int64 在现代平台上原子读写
	time.Sleep(200 * time.Millisecond)

	_, err = w.Write([]byte(strings.Repeat("x", 100)))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	// 检查旧备份是否被删除
	backups, _ := filepath.Glob(path + ".*")
	for _, b := range backups {
		if !isBackupFile(filepath.Base(path), filepath.Base(b)) {
			continue // 跳过锁文件
		}
		fi, _ := os.Stat(b)
		if fi.ModTime().Before(time.Now().AddDate(0, 0, -7)) {
			t.Errorf("old backup %s should have been deleted", b)
		}
	}
}

// TestWriterWriteAfterRotation 验证轮转后新文件能继续接收写入。
func TestWriterWriteAfterRotation(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestWriterWriteAfterRotation.log")
	defer cleanupLogs(t, path)

	w, err := New(Config{
		FilePath:      path,
		MaxSizeMB:     1,
		CheckInterval: 50 * time.Millisecond,
		ErrorHandler:  silentErrors,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		w.Close()
		time.Sleep(200 * time.Millisecond)
	}()

	w.maxSize = 10 // 测试用极小值，int64 在现代平台上原子读写

	_, err = w.Write([]byte(strings.Repeat("a", 100)))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	_, err = w.Write([]byte("after rotation\n"))
	if err != nil {
		t.Fatalf("write after rotation: %v", err)
	}

	content, _ := os.ReadFile(path)
	if !strings.Contains(string(content), "after rotation") {
		t.Fatal("new file should contain data written after rotation")
	}
}

// TestWriterCloseWrite 验证关闭后写入应报错。
func TestWriterCloseWrite(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestWriterCloseWrite.log")
	defer cleanupLogs(t, path)

	w, err := New(Config{
		FilePath:     path,
		MaxSizeMB:    10,
		ErrorHandler: silentErrors,
	})
	if err != nil {
		t.Fatal(err)
	}

	w.Close()
	_, err = w.Write([]byte("after close\n"))
	if err == nil {
		t.Fatal("expected error after close")
	}
}

// TestWriterConcurrentWrites 验证并发写入安全性。
func TestWriterConcurrentWrites(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestWriterConcurrentWrites.log")
	defer cleanupLogs(t, path)

	w, err := New(Config{
		FilePath:     path,
		MaxSizeMB:    100,
		MaxAgeDays:   0,
		ErrorHandler: silentErrors,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		w.Close()
		time.Sleep(200 * time.Millisecond)
	}()

	const goroutines = 10
	const writesPer = 50
	msg := "concurrent test message\n"

	errCh := make(chan error, goroutines)
	for range goroutines {
		go func() {
			for range writesPer {
				if _, err := w.Write([]byte(msg)); err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}()
	}

	for range goroutines {
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

func TestRotateChSignal(t *testing.T) {
	w := &Writer{rotateCh: make(chan struct{}, 1)}
	w.rotateCh <- struct{}{}
	select {
	case <-w.rotateCh:
	default:
		t.Fatal("expected signal in rotateCh")
	}
}

func TestReopenFile_Error(t *testing.T) {
	ensureLogDir(t)
	// 使用目录路径作为文件路径，openFileAppend 应失败
	dirPath := filepath.Join(logDir, t.Name())
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirPath)

	w := &Writer{
		filePath:     dirPath,
		errorHandler: silentErrors,
	}
	if err := w.reopenFile(); err == nil {
		t.Fatal("expected error reopening with directory path")
	}
}

func TestOpenFileAppend_NonexistentDir(t *testing.T) {
	_, err := openFileAppend(filepath.Join(logDir, "nonexistent_"+t.Name(), "test.log"))
	if err == nil {
		t.Fatal("expected error for nonexistent directory")
	}
}

func TestNew_MkdirAllError(t *testing.T) {
	ensureLogDir(t)
	// 创建一个普通文件，然后在它的"子路径"下创建日志文件，
	// MkdirAll 会因为该文件名已存在（不是目录）而失败
	blocker := filepath.Join(logDir, t.Name()+"_blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(blocker)

	path := filepath.Join(blocker, "test.log")
	_, err := New(Config{
		FilePath:     path,
		MaxSizeMB:    10,
		ErrorHandler: silentErrors,
	})
	if err == nil {
		t.Fatal("expected error from MkdirAll")
	}
}

func TestDoRotation_FileRenamed(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, t.Name()+".log")
	defer cleanupLogs(t, path)

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("test data")
	f.Close()

	w := &Writer{
		filePath:     path,
		errorHandler: silentErrors,
	}

	if err := doFileRotation(w.filePath, w.maxSize, w.maxAgeDays); err != nil {
		t.Fatalf("doRotation failed: %v", err)
	}

	// 原文件应已被 rename，备份文件应存在
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("original file should not exist after rotation")
	}
	backups, _ := filepath.Glob(path + ".2*")
	if len(backups) == 0 {
		t.Fatal("backup file should exist after rotation")
	}
}

func TestOpenFileAppend_ReadOnlyFile(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, t.Name()+".log")
	defer os.Remove(path)

	// 创建只读文件
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(path, 0o644)

	_, err = openFileAppend(path)
	if err == nil {
		t.Fatal("expected error opening read-only file for append")
	}
}

// TestWriterConcurrent 验证多 goroutine 并发写入的安全性。
func TestWriterConcurrent(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestWriterConcurrent.log")
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
	for range goroutines {
		go func() {
			for range writes {
				if _, err := w.Write([]byte(msg)); err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}()
	}

	for range goroutines {
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

// TestWriterEmptyWrite 验证空写入。
func TestWriterEmptyWrite(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestWriterEmptyWrite.log")
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

// TestWriterMultipleRotations 验证多次轮转。
func TestWriterMultipleRotations(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestWriterMultipleRotations.log")
	defer cleanupLogs(t, path)

	w, err := New(Config{FilePath: path, MaxSizeMB: 1, MaxAgeDays: 0, CheckInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.maxSize = 10 // 测试用极小值，int64 在现代平台上原子读写

	for i := range 3 {
		_, err := w.Write([]byte(strings.Repeat("a", 100)))
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
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

// TestWriterPollingDetection 验证轮询检测到文件大小超阈值后能触发轮转。
func TestWriterPollingDetection(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestWriterPollingDetection.log")
	defer cleanupLogs(t, path)

	w, err := New(Config{
		FilePath:      path,
		MaxSizeMB:     1,
		MaxAgeDays:    0,
		CheckInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.maxSize = 10 // 测试用极小值，int64 在现代平台上原子读写

	_, err = w.Write([]byte(strings.Repeat("a", 100)))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	_, err = w.Write([]byte("after rotation\n"))
	if err != nil {
		t.Fatal(err)
	}

	content, _ := os.ReadFile(path)
	if !strings.Contains(string(content), "after rotation") {
		t.Fatal("new file should contain data written after rotation")
	}
}

// TestWriter_FileRemovedRecreates 验证文件被外部删除后 Write() 能通过 reopenFile 重建。
func TestWriter_FileRemovedRecreates(t *testing.T) {
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
	os.Remove(path)

	w.rotateCh <- struct{}{}
	time.Sleep(100 * time.Millisecond)

	_, err = w.Write([]byte("after removal\n"))
	if err != nil {
		t.Fatalf("write after file removal: %v", err)
	}

	content, _ := os.ReadFile(path)
	if !strings.Contains(string(content), "after removal") {
		t.Fatal("file should be recreated")
	}
}

// TestWriter_ExternalRotationReopen 验证外部轮转后 Write() 能重开新文件。
func TestWriter_ExternalRotationReopen(t *testing.T) {
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

	backupPath := path + "." + time.Now().Format("20060102_150405")
	os.Rename(path, backupPath)
	defer os.Remove(backupPath)
	f, _ := os.Create(path)
	f.Close()

	w.rotateCh <- struct{}{}
	time.Sleep(100 * time.Millisecond)

	_, err = w.Write([]byte("after external rotation\n"))
	if err != nil {
		t.Fatalf("write after external rotation: %v", err)
	}
}

// TestWriter_TwoWritersCoexist 验证两个 Writer 共存且各自独立工作。
func TestWriter_TwoWritersCoexist(t *testing.T) {
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

	_, err = w1.Write([]byte("from w1\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = w2.Write([]byte("from w2\n"))
	if err != nil {
		t.Fatal(err)
	}
}
