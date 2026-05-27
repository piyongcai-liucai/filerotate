// Package notifier 提供 NATS 核心通知器的单元测试。
// 测试覆盖连接、广播、多客户端以及主题隔离。
// 需要本地运行 NATS 服务，否则测试会自动跳过。
package notifier

import (
	"testing"
	"time"
)

// TestNATSNotifierConnect 验证能够成功订阅并返回有效命令通道。
func TestNATSNotifierConnect(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	n := NewNATSNotifier(conn, "filerotate.test.connect", discardErrors)
	defer n.Close()

	ch, err := n.Connect()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if ch == nil {
		t.Fatal("expected non-nil channel")
	}
}

// TestNATSNotifierBroadcast 验证单个客户端可以收到广播命令。
func TestNATSNotifierBroadcast(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	n := NewNATSNotifier(conn, "filerotate.test.broadcast", discardErrors)
	defer n.Close()

	ch, err := n.Connect()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	// 给订阅一点时间完成
	time.Sleep(100 * time.Millisecond)

	if err := n.Broadcast(cmdRotate); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	assertChannelReceived(t, ch, cmdRotate, 2*time.Second)
}

// TestNATSNotifierMultipleClients 验证多个客户端都能收到广播命令（广播模式）。
func TestNATSNotifierMultipleClients(t *testing.T) {
	conn1 := connectNATSOrSkip(t)
	defer conn1.Close()
	conn2 := connectNATSOrSkip(t)
	defer conn2.Close()

	subject := "filerotate.test.multi"
	n1 := NewNATSNotifier(conn1, subject, discardErrors)
	defer n1.Close()
	n2 := NewNATSNotifier(conn2, subject, discardErrors)
	defer n2.Close()

	ch1, _ := n1.Connect()
	ch2, _ := n2.Connect()
	time.Sleep(100 * time.Millisecond)

	// 通过第一个通知器广播
	if err := n1.Broadcast(cmdRotate); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	// 两个客户端都应该收到
	assertChannelReceived(t, ch1, cmdRotate, 2*time.Second)
	assertChannelReceived(t, ch2, cmdRotate, 2*time.Second)
}

// TestNATSNotifierClose 验证关闭通知器后取消订阅不报错。
func TestNATSNotifierClose(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	n := NewNATSNotifier(conn, "filerotate.test.close", discardErrors)
	// 忽略返回的通道，因为只测试 Close 不报错
	_, _ = n.Connect()
	time.Sleep(100 * time.Millisecond)

	n.Close()
	// NATS 的 Unsubscribe 是安全的，这里只验证不 panic
}

// TestNATSNotifierDifferentSubjects 验证不同主题的隔离性。
// 订阅不同 subject 的客户端不会收到对方的广播。
func TestNATSNotifierDifferentSubjects(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()

	n1 := NewNATSNotifier(conn, "filerotate.test.subject1", discardErrors)
	defer n1.Close()
	n2 := NewNATSNotifier(conn, "filerotate.test.subject2", discardErrors)
	defer n2.Close()

	ch1, _ := n1.Connect()
	ch2, _ := n2.Connect()
	time.Sleep(100 * time.Millisecond)

	// 向 subject1 广播
	if err := n1.Broadcast(cmdRotate); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	// ch1 应收到
	assertChannelReceived(t, ch1, cmdRotate, 2*time.Second)

	// ch2 不应收到
	select {
	case <-ch2:
		t.Fatal("ch2 should not receive message on different subject")
	case <-time.After(500 * time.Millisecond):
		// 预期超时
	}
}

// ---------- 边界测试 ----------

func TestNATSNotifier_NilErrorHandler(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	n := NewNATSNotifier(conn, "filerotate.test.nilhandler", nil)
	defer n.Close()

	// reportError 在 nil handler 时应安全返回
	n.reportError(nil)
}

func TestNATSNotifier_CloseWithoutConnect(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	n := NewNATSNotifier(conn, "filerotate.test.noconnect", discardErrors)

	// 未调用 Connect 直接 Close，应安全（走 else 分支关闭初始 channel）
	if err := n.Close(); err != nil {
		t.Fatalf("Close without Connect: %v", err)
	}
}

func TestNATSNotifier_BroadcastError(t *testing.T) {
	conn := connectNATSOrSkip(t)
	n := NewNATSNotifier(conn, "filerotate.test.bcasterr", discardErrors)

	conn.Close() // 关闭连接使后续操作失败
	err := n.Broadcast("ROTATE")
	if err == nil {
		t.Fatal("expected broadcast error after connection close")
	}
	n.Close()
}

func TestNATSNotifier_Reconnect(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	n := NewNATSNotifier(conn, "filerotate.test.reconnect", discardErrors)
	defer n.Close()

	// 两次 Connect 应安全替换订阅
	ch1, _ := n.Connect()
	ch2, _ := n.Connect() // 这次会 Unsubscribe 旧的

	// ch1 应很快关闭（旧订阅被取消）
	select {
	case _, ok := <-ch1:
		if ok {
			t.Fatal("old channel should be closed after reconnect")
		}
	case <-time.After(time.Second):
		t.Fatal("old channel should have been closed")
	}

	// ch2 应能收到消息
	time.Sleep(100 * time.Millisecond)
	n.Broadcast("ROTATE")
	select {
	case cmd := <-ch2:
		if cmd != "ROTATE" {
			t.Fatalf("expected ROTATE, got %q", cmd)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message on new channel")
	}
}

func TestNATSNotifier_Serve(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	n := NewNATSNotifier(conn, "filerotate.test.serve", discardErrors)
	defer n.Close()

	if err := n.Serve(); err != nil {
		t.Fatalf("Serve should return nil: %v", err)
	}
}
