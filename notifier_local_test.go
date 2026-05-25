package filerotate

import (
	"os"
	"testing"
	"time"
)

// startLocalServer 启动一个本地通知器服务端，返回 Notifier 和清理函数。
func startLocalServer(t *testing.T, commPath string) (Notifier, func()) {
	t.Helper()
	n := NewLocalNotifier(commPath, nil)
	go n.Serve()
	time.Sleep(100 * time.Millisecond)
	return n, func() {
		n.Close()
		time.Sleep(50 * time.Millisecond)
	}
}

// TestLocalNotifierServeAndConnect 测试服务端启动和客户端连接。
func TestLocalNotifierServeAndConnect(t *testing.T) {
	sockPath := t.TempDir() + "/test.sock"
	n, cleanup := startLocalServer(t, sockPath)
	defer cleanup()

	ch, err := n.Connect()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if ch == nil {
		t.Fatal("expected non-nil channel")
	}
}

// TestLocalNotifierBroadcast 测试单客户端接收广播命令。
func TestLocalNotifierBroadcast(t *testing.T) {
	sockPath := t.TempDir() + "/test.sock"
	n, cleanup := startLocalServer(t, sockPath)
	defer cleanup()

	ch, err := n.Connect()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	if err := n.Broadcast(CmdRotate); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	assertChannelReceived(t, ch, CmdRotate, 1*time.Second)
}

// TestLocalNotifierMultipleClients 测试多个客户端都能收到广播命令。
func TestLocalNotifierMultipleClients(t *testing.T) {
	sockPath := t.TempDir() + "/test.sock"
	n, cleanup := startLocalServer(t, sockPath)
	defer cleanup()

	var channels []<-chan string
	for i := 0; i < 3; i++ {
		ch, err := n.Connect()
		if err != nil {
			t.Fatalf("connect %d: %v", i, err)
		}
		channels = append(channels, ch)
	}

	if err := n.Broadcast(CmdRotate); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	for _, ch := range channels {
		assertChannelReceived(t, ch, CmdRotate, 1*time.Second)
	}
}

// TestLocalNotifierClose 测试关闭通知器后资源被正确释放。
func TestLocalNotifierClose(t *testing.T) {
	sockPath := t.TempDir() + "/test.sock"
	n := NewLocalNotifier(sockPath, nil)

	go n.Serve()
	time.Sleep(100 * time.Millisecond)

	if err := n.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	if _, err := os.Stat(sockPath); err == nil {
		t.Fatal("socket file should be removed after close")
	}
}

// TestLocalNotifierConnectToClosedServer 测试连接到已关闭的服务端应失败。
func TestLocalNotifierConnectToClosedServer(t *testing.T) {
	sockPath := t.TempDir() + "/test.sock"
	n := NewLocalNotifier(sockPath, nil)
	_, err := n.Connect()
	if err == nil {
		t.Fatal("expected error connecting to non-existent server")
	}
}
