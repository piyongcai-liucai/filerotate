// writer_test.go
// 标准版 Writer 的单元测试。
// 测试覆盖基本写入、轮转、备份清理、Leader故障恢复以及并发写入安全性。
// 所有日志文件均写入 ./example/log 目录，避免系统临时目录的权限问题。
package filerotate

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
		NotifierFactory: func(errorHandler func(error)) (Notifier, error) { return &errorNotifier{}, nil },
		ErrorHandler: silentErrors,
	})
	if err == nil {
		t.Fatal("expected error from LockerFactory")
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

// ---------- mock 类型 ----------

type errorLocker struct{}

func (e *errorLocker) TryLock() (bool, error) { return false, errors.New("mock error") }
func (e *errorLocker) Unlock() error           { return nil }

type errorNotifier struct{ broadcastErr error }

func (e *errorNotifier) Serve() error                          { return nil }
func (e *errorNotifier) Connect() (<-chan string, error)       { ch := make(chan string); return ch, nil }
func (e *errorNotifier) Broadcast(_ string) error              { return e.broadcastErr }
func (e *errorNotifier) Close() error                          { return nil }

// ---------- requeueRotate 测试 ----------

func TestReququeueRotate(t *testing.T) {
	w := &Writer{rotateCh: make(chan struct{}, 1)}
	w.requeueRotate()
	select {
	case <-w.rotateCh:
	default:
		t.Fatal("expected signal in rotateCh")
	}
}

func TestReququeueRotate_NoDoubleQueue(t *testing.T) {
	w := &Writer{rotateCh: make(chan struct{}, 1)}
	w.rotateCh <- struct{}{} // 填满通道
	w.requeueRotate()        // 不应阻塞（通道已满，丢弃）

	select {
	case <-w.rotateCh:
	default:
		t.Fatal("expected signal in rotateCh")
	}
	// 通道应为空（没有第二个信号）
	select {
	case <-w.rotateCh:
		t.Fatal("expected no second signal")
	default:
	}
}

// ---------- tryBecomeLeader 测试 ----------

func TestTryBecomeLeader_TryLockError(t *testing.T) {
	w := &Writer{
		leaderLocker: &errorLocker{},
		errorHandler: silentErrors,
	}
	if w.tryBecomeLeader() {
		t.Fatal("expected false when TryLock returns error")
	}
}

// ---------- broadcastRotate 测试 ----------

func TestBroadcastRotate_Error(t *testing.T) {
	var capturedErr error
	w := &Writer{
		notifier:     &errorNotifier{broadcastErr: errors.New("broadcast failed")},
		errorHandler: func(err error) { capturedErr = err },
	}
	w.broadcastRotate()
	if capturedErr == nil {
		t.Fatal("expected error to be reported")
	}
	if !strings.Contains(capturedErr.Error(), "broadcast failed") {
		t.Fatalf("unexpected error: %v", capturedErr)
	}
}

// ---------- reopenFile 测试 ----------

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

// ---------- openFileAppend 测试 ----------

func TestOpenFileAppend_NonexistentDir(t *testing.T) {
	_, err := openFileAppend(filepath.Join(logDir, "nonexistent_"+t.Name(), "test.log"))
	if err == nil {
		t.Fatal("expected error for nonexistent directory")
	}
}

// ---------- 构造错误路径测试 ----------

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

func TestNew_LockerFactoryError(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, t.Name()+".log")
	defer cleanupLogs(t, path)

	_, err := New(Config{
		FilePath:  path,
		MaxSizeMB: 10,
		LockerFactory: func(lockPath string) (Locker, error) {
			return nil, errors.New("custom locker error")
		},
		NotifierFactory: func(errorHandler func(error)) (Notifier, error) { return &errorNotifier{}, nil },
		ErrorHandler: silentErrors,
	})
	if err == nil {
		t.Fatal("expected error from LockerFactory")
	}
}

// TestNew_NotifierWithoutLocker 验证标准模式下设置通知器但未设置锁工厂时报错。
func TestNew_NotifierWithoutLocker(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, t.Name()+".log")
	defer cleanupLogs(t, path)

	_, err := New(Config{
		FilePath:     path,
		MaxSizeMB:    10,
		NotifierFactory: func(errorHandler func(error)) (Notifier, error) {
			return &errorNotifier{}, nil
		},
		ErrorHandler: silentErrors,
	})
	if err == nil {
		t.Fatal("expected error when NotifierFactory is set but LockerFactory is nil")
	}
}

// ---------- doRotation 测试 ----------

func TestDoRotation_StandardMode(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, t.Name()+".log")
	defer cleanupLogs(t, path)

	// 创建文件并写入一些数据
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("test data")
	f.Close()

	w := &Writer{
		filePath:     path,
		mu:           sync.Mutex{},
		errorHandler: silentErrors,
	}
	// 手动设置 file，doRotation 需要它
	w.file, err = openFileAppend(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.doRotation(); err != nil {
		t.Fatalf("doRotation failed: %v", err)
	}

	// 新文件应已创建
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("new file should exist after rotation")
	}
}

// ---------- New 工厂成功路径测试 ----------



func TestNew_CustomLockerFactorySuccess(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, t.Name()+".log")
	defer cleanupLogs(t, path)

	w, err := New(Config{
		FilePath:      path,
		MaxSizeMB:     10,
		ErrorHandler:  silentErrors,
		LockerFactory: newLocalLocker,
		NotifierFactory: func(errorHandler func(error)) (Notifier, error) {
			return &errorNotifier{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { w.Close(); time.Sleep(200 * time.Millisecond) }()

	_, err = w.Write([]byte("hello with custom locker\n"))
	if err != nil {
		t.Fatalf("write: %v", err)
	}
}

// ---------- openFileAppend 错误路径测试 ----------

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
