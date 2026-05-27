// Package notifier 提供 JetStream 通知器的单元测试。
// 测试覆盖临时消费者连接、广播、Stream 缺失错误处理以及消费者自动清理。
// 需要本地运行 NATS 并启用 JetStream，否则测试会自动跳过。
package notifier

import (
	"testing"
	"time"
)

// ---------- 测试用例 ----------

// TestJetStreamNotifierConnect 验证 JetStream 通知器能正常连接并返回命令通道。
func TestJetStreamNotifierConnect(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	js := getJetStreamOrSkip(t, conn)

	subject := "filerotate.js.test.connect"
	streamName := "FILEROTATE_CONNECT"
	ensureJetStreamStream(t, js, streamName, subject)

	n := NewJetStreamNotifier(js, subject, discardErrors)
	defer n.Close()

	ch, err := n.Connect()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if ch == nil {
		t.Fatal("expected non-nil channel")
	}
}

// TestJetStreamNotifierBroadcast 验证单客户端可以接收到广播命令。
func TestJetStreamNotifierBroadcast(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	js := getJetStreamOrSkip(t, conn)

	subject := "filerotate.js.test.broadcast"
	streamName := "FILEROTATE_BCAST"
	ensureJetStreamStream(t, js, streamName, subject)

	n := NewJetStreamNotifier(js, subject, discardErrors)
	defer n.Close()

	ch, _ := n.Connect()
	time.Sleep(100 * time.Millisecond)

	if err := n.Broadcast(cmdRotate); err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	assertChannelReceived(t, ch, cmdRotate, 2*time.Second)
}

// TestJetStreamNotifierConnectFailsForMissingStream 验证 Stream 不存在时，Connect 返回错误。
func TestJetStreamNotifierConnectFailsForMissingStream(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	js := getJetStreamOrSkip(t, conn)

	n := NewJetStreamNotifier(js, "filerotate.nonexistent", discardErrors)
	_, err := n.Connect()
	if err == nil {
		t.Fatal("Connect should fail for non-existent stream")
	}
}

// TestJetStreamNotifierEphemeralCleanup 验证临时消费者在 Close 后被清理。
func TestJetStreamNotifierEphemeralCleanup(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	js := getJetStreamOrSkip(t, conn)

	subject := "filerotate.js.test.cleanup"
	streamName := "FILEROTATE_CLEAN"
	ensureJetStreamStream(t, js, streamName, subject)

	n1 := NewJetStreamNotifier(js, subject, discardErrors)
	ch1, _ := n1.Connect()
	n1.Close()

	select {
	case _, ok := <-ch1:
		if ok {
			t.Fatal("channel should be closed after Close()")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("channel should be closed")
	}

	n2 := NewJetStreamNotifier(js, subject, discardErrors)
	defer n2.Close()
	ch2, err := n2.Connect()
	if err != nil {
		t.Fatalf("connect after cleanup: %v", err)
	}
	if ch2 == nil {
		t.Fatal("expected non-nil channel")
	}
}

// TestJetStreamNotifierMultipleClients 验证多个客户端都能收到广播命令。
func TestJetStreamNotifierMultipleClients(t *testing.T) {
	conn1 := connectNATSOrSkip(t)
	defer conn1.Close()
	conn2 := connectNATSOrSkip(t)
	defer conn2.Close()

	js1 := getJetStreamOrSkip(t, conn1)
	js2 := getJetStreamOrSkip(t, conn2)

	subject := "filerotate.js.test.multi"
	streamName := "FILEROTATE_MULTI"
	ensureJetStreamStream(t, js1, streamName, subject)

	n1 := NewJetStreamNotifier(js1, subject, discardErrors)
	defer n1.Close()
	n2 := NewJetStreamNotifier(js2, subject, discardErrors)
	defer n2.Close()

	ch1, _ := n1.Connect()
	ch2, _ := n2.Connect()
	time.Sleep(100 * time.Millisecond)

	if err := n1.Broadcast(cmdRotate); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	assertChannelReceived(t, ch1, cmdRotate, 2*time.Second)
	assertChannelReceived(t, ch2, cmdRotate, 2*time.Second)
}

// TestJetStreamNotifierRestartConsumer 验证新消费者只接收新消息，不会收到历史消息。
func TestJetStreamNotifierRestartConsumer(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	js := getJetStreamOrSkip(t, conn)

	subject := "filerotate.js.test.restart"
	streamName := "FILEROTATE_RESTART"
	ensureJetStreamStream(t, js, streamName, subject)

	n1 := NewJetStreamNotifier(js, subject, discardErrors)
	ch1, _ := n1.Connect()
	if err := n1.Broadcast("MSG1"); err != nil {
		t.Fatalf("broadcast MSG1: %v", err)
	}
	assertChannelReceived(t, ch1, "MSG1", 2*time.Second)
	n1.Close()

	n1.Broadcast("MSG2")
	time.Sleep(200 * time.Millisecond)

	n2 := NewJetStreamNotifier(js, subject, discardErrors)
	defer n2.Close()
	ch2, _ := n2.Connect()
	n2.Broadcast("MSG3")
	assertChannelReceived(t, ch2, "MSG3", 2*time.Second)

	select {
	case cmd := <-ch2:
		t.Fatalf("should not receive old message, got %q", cmd)
	case <-time.After(500 * time.Millisecond):
	}
}

// ---------- 边界测试 ----------

func TestJetStreamNotifier_NilErrorHandler(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	js := getJetStreamOrSkip(t, conn)

	n := NewJetStreamNotifier(js, "filerotate.js.test.nilhandler", nil)
	defer n.Close()

	// reportError 在 nil handler 时应安全返回
	n.reportError(nil)
}

func TestJetStreamNotifier_Serve(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	js := getJetStreamOrSkip(t, conn)

	n := NewJetStreamNotifier(js, "filerotate.js.test.serve", discardErrors)
	defer n.Close()

	if err := n.Serve(); err != nil {
		t.Fatalf("Serve should return nil: %v", err)
	}
}

func TestJetStreamNotifier_BroadcastError(t *testing.T) {
	conn := connectNATSOrSkip(t)
	js := getJetStreamOrSkip(t, conn)

	n := NewJetStreamNotifier(js, "filerotate.js.test.bcasterr", discardErrors)
	conn.Close() // 关闭连接使 Publish 失败

	err := n.Broadcast("ROTATE")
	if err == nil {
		t.Fatal("expected broadcast error after connection close")
	}
	n.Close()
}
