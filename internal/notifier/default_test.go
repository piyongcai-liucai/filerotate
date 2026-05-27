// Package notifier 提供 DefaultNotifier 的单元测试。
// 测试覆盖构造、信号检测、去重、外部轮转检测和资源清理。
package notifier

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// newMaxSize 创建一个 maxSize 辅助指针。
func newMaxSize(n int64) *int64 { return &n }

// tempFile 创建一个临时文件用于测试，返回文件路径和清理函数。
func tempFile(t *testing.T) (string, func()) {
	t.Helper()
	dir := "../../example/log"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create log dir: %v", err)
	}
	path := filepath.Join(dir, t.Name()+".log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	f.Close()
	return path, func() { os.Remove(path) }
}

// ---------- 构造测试 ----------

func TestNewDefault_FileNotExist(t *testing.T) {
	_, err := NewDefault("/nonexistent/path/test.log", newMaxSize(1024), time.Second, discardErrors)
	if err == nil {
		t.Fatal("expected error for non-existent file")
	}
}

func TestNewDefault_Success(t *testing.T) {
	path, _ := tempFile(t)
	n, err := NewDefault(path, newMaxSize(1024), 100*time.Millisecond, discardErrors)
	if err != nil {
		t.Fatalf("NewDefault: %v", err)
	}
	n.Close()
}

// ---------- Connect / Broadcast 测试 ----------

func TestDefaultNotifier_Connect(t *testing.T) {
	path, _ := tempFile(t)
	n, err := NewDefault(path, newMaxSize(1024), time.Second, discardErrors)
	if err != nil {
		t.Fatalf("NewDefault: %v", err)
	}
	defer n.Close()

	ch, err := n.Connect()
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if ch == nil {
		t.Fatal("expected non-nil channel")
	}
}

func TestDefaultNotifier_Broadcast(t *testing.T) {
	path, _ := tempFile(t)
	n, err := NewDefault(path, newMaxSize(1024), time.Second, discardErrors)
	if err != nil {
		t.Fatalf("NewDefault: %v", err)
	}
	defer n.Close()

	ch, _ := n.Connect()
	if err := n.Broadcast("HELLO"); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}

	select {
	case cmd := <-ch:
		if cmd != "HELLO" {
			t.Fatalf("expected %q, got %q", "HELLO", cmd)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for broadcast")
	}
}

// ---------- 轮询信号测试 ----------

func TestDefaultNotifier_SizeExceedsThreshold(t *testing.T) {
	path, _ := tempFile(t)
	maxSize := int64(10)
	n, err := NewDefault(path, &maxSize, 50*time.Millisecond, discardErrors)
	if err != nil {
		t.Fatalf("NewDefault: %v", err)
	}
	defer n.Close()

	ch, _ := n.Connect()
	n.Serve()

	// 写入超过阈值的数据
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	f.Write(make([]byte, 20))
	f.Close()

	assertChannelReceived(t, ch, cmdRotate, 2*time.Second)
}

func TestDefaultNotifier_SignalSentPreventsDuplicate(t *testing.T) {
	path, _ := tempFile(t)
	maxSize := int64(10)
	n, err := NewDefault(path, &maxSize, 50*time.Millisecond, discardErrors)
	if err != nil {
		t.Fatalf("NewDefault: %v", err)
	}
	defer n.Close()

	ch, _ := n.Connect()
	n.Serve()

	// 写入数据触发信号
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write(make([]byte, 20))
	f.Close()

	// 应收到第一个 ROTATE
	assertChannelReceived(t, ch, cmdRotate, 2*time.Second)

	// 在 Reset 前，不应再收到第二个（去重）
	select {
	case cmd := <-ch:
		t.Fatalf("unexpected duplicate signal: %q", cmd)
	case <-time.After(300 * time.Millisecond):
		// 预期超时，无重复信号
	}

	// Reset 后，继续写入应触发新信号
	n.Reset()
	f, _ = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write(make([]byte, 30))
	f.Close()
	assertChannelReceived(t, ch, cmdRotate, 2*time.Second)
}

func TestDefaultNotifier_MaxSizeZero_NoSignalBySize(t *testing.T) {
	path, _ := tempFile(t)
	maxSize := int64(0) // 永不以大小触发
	n, err := NewDefault(path, &maxSize, 50*time.Millisecond, discardErrors)
	if err != nil {
		t.Fatalf("NewDefault: %v", err)
	}
	defer n.Close()

	ch, _ := n.Connect()
	n.Serve()

	// 写入大量数据
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write(make([]byte, 1024))
	f.Close()

	// 不应收到 ROTATE
	select {
	case cmd := <-ch:
		t.Fatalf("unexpected signal with maxSize=0: %q", cmd)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestDefaultNotifier_SameFileTriggersRotate(t *testing.T) {
	path, _ := tempFile(t)
	maxSize := int64(10)
	n, err := NewDefault(path, &maxSize, 50*time.Millisecond, discardErrors)
	if err != nil {
		t.Fatalf("NewDefault: %v", err)
	}
	defer n.Close()

	ch, _ := n.Connect()
	n.Serve()
	time.Sleep(100 * time.Millisecond) // 等待首个 tick 完成

	// 模拟外部轮转：rename 旧文件 + 创建新文件
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	f.Close()

	// inode 变化应触发 ROTATE
	assertChannelReceived(t, ch, cmdRotate, 2*time.Second)
}

func TestDefaultNotifier_SameFileNoSignal(t *testing.T) {
	path, _ := tempFile(t)
	maxSize := int64(1024 * 1024) // 1MB，远超测试数据
	n, err := NewDefault(path, &maxSize, 50*time.Millisecond, discardErrors)
	if err != nil {
		t.Fatalf("NewDefault: %v", err)
	}
	defer n.Close()

	ch, _ := n.Connect()
	n.Serve()

	// 写入少量数据，不超阈值
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write(make([]byte, 100))
	f.Close()

	// 不应收到信号（未超阈值，未改变 inode）
	select {
	case cmd := <-ch:
		t.Fatalf("unexpected signal: %q", cmd)
	case <-time.After(300 * time.Millisecond):
	}
}

// ---------- Close 测试 ----------

func TestDefaultNotifier_Close_StopsPolling(t *testing.T) {
	path, _ := tempFile(t)
	n, err := NewDefault(path, newMaxSize(1024), 50*time.Millisecond, discardErrors)
	if err != nil {
		t.Fatalf("NewDefault: %v", err)
	}

	ch, _ := n.Connect()
	n.Serve()

	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// channel 应被关闭
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel should be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("channel should be closed")
	}
}

// ---------- Reset 测试 ----------

func TestDefaultNotifier_Reset_BeforeServe(t *testing.T) {
	path, _ := tempFile(t)
	n, err := NewDefault(path, newMaxSize(1024), time.Second, discardErrors)
	if err != nil {
		t.Fatalf("NewDefault: %v", err)
	}
	defer n.Close()

	// Reset 在未 Serve 时不应 panic
	n.Reset()
}

// ---------- reportError + nil errorHandler 测试 ----------

func TestDefaultNotifier_NilErrorHandler(t *testing.T) {
	path, _ := tempFile(t)
	// 传入 nil errorHandler，不应 panic
	n, err := NewDefault(path, newMaxSize(1024), time.Second, nil)
	if err != nil {
		t.Fatalf("NewDefault: %v", err)
	}
	defer n.Close()

	// reportError 在 nil handler 时应安全返回
	n.reportError(nil)
	n.reportError(ErrLeaderExists)
}

func TestDefaultNotifier_CheckAndSignal_StatError(t *testing.T) {
	path, _ := tempFile(t)
	maxSize := int64(10)
	n, err := NewDefault(path, &maxSize, 50*time.Millisecond, discardErrors)
	if err != nil {
		t.Fatalf("NewDefault: %v", err)
	}
	defer n.Close()

	// 删除文件后 checkAndSignal 应处理 Stat 错误
	os.Remove(path)
	n.checkAndSignal() // 不应 panic
}

// ---------- Serve (LocalNotifier) nil errorHandler 测试 ----------

func TestLocalNotifier_NilErrorHandler(t *testing.T) {
	n := NewLocalNotifier("test_nil_handler", nil)
	defer n.Close()

	// reportError 在 nil handler 时应安全返回
	n.reportError(nil)
	n.reportError(ErrLeaderExists)
}
