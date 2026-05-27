// Package notifier 提供本地 IPC 通知器的单元测试。
package notifier

import (
	"testing"
	"time"
)

// startLocalServer 启动一个本地通知器服务端，返回通知器实例和清理函数。
// 通过 Connect 重试循环验证 Serve 已启动成功，避免固定 sleep 的竞态。
func startLocalServer(t *testing.T, commPath string) (*LocalNotifier, func()) {
	t.Helper()
	n := NewLocalNotifier(commPath, discardErrors)
	go n.Serve()

	// 重试 Connect 直到成功或超时，确保 Serve 已就绪
	var ch <-chan string
	var err error
	for i := 0; i < 20; i++ {
		ch, err = n.Connect()
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Serve did not start within timeout: %v", err)
	}

	// 关闭验证连接，让真正的测试代码自行 Connect
	go func() {
		for range ch {
		}
	}()

	return n, func() {
		n.Close()
		time.Sleep(50 * time.Millisecond)
	}
}

// TestLocalNotifierServeAndConnect 验证服务端正常启动且客户端可成功连接。
func TestLocalNotifierServeAndConnect(t *testing.T) {
	testPipe := "filerotate_test_serve"
	n, cleanup := startLocalServer(t, testPipe)
	defer cleanup()

	ch, err := n.Connect()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if ch == nil {
		t.Fatal("expected non-nil channel")
	}
}

// TestLocalNotifierBroadcast 验证单客户端能够正确接收广播命令。
func TestLocalNotifierBroadcast(t *testing.T) {
	testPipe := "filerotate_test_broadcast"
	n, cleanup := startLocalServer(t, testPipe)
	defer cleanup()

	ch, err := n.Connect()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	if err := n.Broadcast(cmdRotate); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	assertChannelReceived(t, ch, cmdRotate, 2*time.Second)
}

// TestLocalNotifierMultipleClients 验证广播时所有已连接客户端都能收到命令。
func TestLocalNotifierMultipleClients(t *testing.T) {
	testPipe := "filerotate_test_multi"
	n, cleanup := startLocalServer(t, testPipe)
	defer cleanup()

	var channels []<-chan string
	for i := 0; i < 3; i++ {
		ch, err := n.Connect()
		if err != nil {
			t.Fatalf("connect %d: %v", i, err)
		}
		channels = append(channels, ch)
	}

	time.Sleep(100 * time.Millisecond)
	if err := n.Broadcast(cmdRotate); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	for _, ch := range channels {
		assertChannelReceived(t, ch, cmdRotate, 2*time.Second)
	}
}

// TestLocalNotifierClose 验证关闭通知器后资源被释放。
func TestLocalNotifierClose(t *testing.T) {
	testPipe := "filerotate_test_close"
	n := NewLocalNotifier(testPipe, discardErrors)

	go n.Serve()

	// 通过 Connect 重试验证 Serve 已启动
	var err error
	for i := 0; i < 20; i++ {
		_, err = n.Connect()
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Serve did not start: %v", err)
	}

	if err := n.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestLocalNotifierConnectToClosedServer 验证连接到未启动的服务端时应返回错误。
func TestLocalNotifierConnectToClosedServer(t *testing.T) {
	testPipe := "filerotate_test_closed"
	n := NewLocalNotifier(testPipe, discardErrors)

	done := make(chan error, 1)
	go func() {
		_, err := n.Connect()
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error connecting to non-existent server")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout connecting to closed server")
	}
}
