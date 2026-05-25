package filerotate

import (
	"testing"
	"time"
)

// TestNATSNotifierConnect 测试 NATS 客户端连接功能。
func TestNATSNotifierConnect(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	n := NewNATSNotifier(conn, "filerotate.test.connect", nil)
	defer n.Close()
	ch, err := n.Connect()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if ch == nil {
		t.Fatal("expected non-nil channel")
	}
}

// TestNATSNotifierBroadcast 测试单客户端接收广播命令。
func TestNATSNotifierBroadcast(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	n := NewNATSNotifier(conn, "filerotate.test.broadcast", nil)
	defer n.Close()
	ch, _ := n.Connect()

	// 等待订阅完成
	time.Sleep(100 * time.Millisecond)

	n.Broadcast(CmdRotate)
	assertChannelReceived(t, ch, CmdRotate, 2*time.Second)
}

// TestNATSNotifierMultipleClients 测试多个客户端都能收到广播命令。
func TestNATSNotifierMultipleClients(t *testing.T) {
	conn1, conn2 := connectNATSOrSkip(t), connectNATSOrSkip(t)
	defer conn1.Close()
	defer conn2.Close()
	subject := "filerotate.test.multi"
	n1 := NewNATSNotifier(conn1, subject, nil)
	defer n1.Close()
	n2 := NewNATSNotifier(conn2, subject, nil)
	defer n2.Close()
	ch1, _ := n1.Connect()
	ch2, _ := n2.Connect()

	// 等待订阅完成
	time.Sleep(100 * time.Millisecond)

	n1.Broadcast(CmdRotate)
	assertChannelReceived(t, ch1, CmdRotate, 2*time.Second)
	assertChannelReceived(t, ch2, CmdRotate, 2*time.Second)
}

// TestNATSNotifierClose 测试关闭后通道被关闭。
func TestNATSNotifierClose(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	n := NewNATSNotifier(conn, "filerotate.test.close", nil)
	ch, _ := n.Connect()

	// 等待订阅完成后再关闭
	time.Sleep(100 * time.Millisecond)

	n.Close()

	// 验证通道已关闭
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel should be closed after Close()")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("channel should be closed")
	}
}
