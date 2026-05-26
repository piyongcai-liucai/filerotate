// writer_test.go
// 标准版 Writer 的单元测试。
// 测试覆盖基本写入、轮转、备份清理、Leader故障恢复以及并发写入安全性。
// 所有日志文件均写入 ./example/log 目录，避免系统临时目录的权限问题。
package filerotate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const logDir = "./example/log"

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
		FilePath:      path,
		MaxSizeMB:     10,
		MaxAgeDays:    0,
		ErrorHandler:  silentErrors,
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
		MaxSizeMB:     0,
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
	w.maxSize = 10
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
	for i := 0; i < 3; i++ {
		ts := oldTime.Add(time.Duration(i) * time.Hour).Format("20060102_150405")
		f, _ := os.Create(path + "." + ts)
		f.Close()
		os.Chtimes(path+"."+ts, oldTime, oldTime)
	}

	recent := time.Now().Format("20060102_150405")
	f, _ := os.Create(path + "." + recent)
	f.Close()

	w, err := New(Config{
		FilePath:      path,
		MaxSizeMB:     0,
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

	w.maxSize = 10
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

// TestWriterLeaderFailover 验证Leader崩溃后其他进程能够接管。
func TestWriterLeaderFailover(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestWriterLeaderFailover.log")
	defer cleanupLogs(t, path)

	w1, err := New(Config{
		FilePath:      path,
		MaxSizeMB:     0,
		CheckInterval: 50 * time.Millisecond,
		ErrorHandler:  silentErrors,
	})
	if err != nil {
		t.Fatal(err)
	}
	w1.maxSize = 10

	w2, err := New(Config{
		FilePath:      path,
		MaxSizeMB:     0,
		CheckInterval: 50 * time.Millisecond,
		ErrorHandler:  silentErrors,
	})
	if err != nil {
		t.Fatal(err)
	}
	w2.maxSize = 10

	w1.Close()
	time.Sleep(200 * time.Millisecond)

	defer func() {
		w2.Close()
		time.Sleep(200 * time.Millisecond)
	}()

	_, err = w2.Write([]byte("hello after failover\n"))
	if err != nil {
		t.Fatalf("write after failover: %v", err)
	}
}

// TestWriterWriteAfterRotation 验证轮转后新文件能继续接收写入。
func TestWriterWriteAfterRotation(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestWriterWriteAfterRotation.log")
	defer cleanupLogs(t, path)

	w, err := New(Config{
		FilePath:      path,
		MaxSizeMB:     0,
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

	w.maxSize = 10

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

// TestWriterDoubleClose 验证多次调用 Close 不会 panic（sync.Once 保护）。
func TestWriterDoubleClose(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestWriterDoubleClose.log")
	defer cleanupLogs(t, path)

	w, err := New(Config{
		FilePath:     path,
		MaxSizeMB:    10,
		ErrorHandler: silentErrors,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second close: %v", err)
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

// TestWriterNewLockerFactoryError 验证 LockerFactory 返回错误时 New 应失败。
func TestWriterNewLockerFactoryError(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestWriterNewLockerFactoryError.log")
	defer cleanupLogs(t, path)

	_, err := New(Config{
		FilePath:  path,
		MaxSizeMB: 10,
		LockerFactory: func(lockPath string) (Locker, error) {
			return nil, fmt.Errorf("custom locker error")
		},
		ErrorHandler: silentErrors,
	})
	if err == nil {
		t.Fatal("expected error from LockerFactory")
	}
}

// TestWriterNewNotifierFactoryError 验证 NotifierFactory 返回错误时 New 应失败。
func TestWriterNewNotifierFactoryError(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestWriterNewNotifierFactoryError.log")
	defer cleanupLogs(t, path)

	_, err := New(Config{
		FilePath:  path,
		MaxSizeMB: 10,
		NotifierFactory: func(commPath string, errorHandler func(error)) (Notifier, error) {
			return nil, fmt.Errorf("custom notifier error")
		},
		ErrorHandler: silentErrors,
	})
	if err == nil {
		t.Fatal("expected error from NotifierFactory")
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
