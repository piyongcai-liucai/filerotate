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
// 需要预先创建对应的 Stream，Serve 方法会检查其存在。
func TestJetStreamNotifierConnect(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	js := getJetStreamOrSkip(t, conn)

	subject := "filerotate.js.test.connect"
	streamName := "FILEROTATE_CONNECT"
	ensureJetStreamStream(t, js, streamName, subject)

	n := NewJetStreamNotifier(js, subject, streamName, discardErrors)
	defer n.Close()

	// Serve 应验证 Stream 存在
	if err := n.Serve(); err != nil {
		t.Fatalf("Serve: %v", err)
	}

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

	n := NewJetStreamNotifier(js, subject, streamName, discardErrors)
	defer n.Close()

	ch, _ := n.Connect()
	// 给临时消费者一点时间建立连接
	time.Sleep(100 * time.Millisecond)

	n.Broadcast(cmdRotate)
	assertChannelReceived(t, ch, cmdRotate, 2*time.Second)
}

// TestJetStreamNotifierServeFailsForMissingStream 验证当 Stream 不存在时，Serve 应返回错误。
func TestJetStreamNotifierServeFailsForMissingStream(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	js := getJetStreamOrSkip(t, conn)

	n := NewJetStreamNotifier(js, "filerotate.nonexistent", "NONEXISTENT_STREAM", discardErrors)
	err := n.Serve()
	if err == nil {
		t.Fatal("Serve should fail for non-existent stream")
	}
}

// TestJetStreamNotifierEphemeralCleanup 验证临时消费者在 Close 后被清理。
// 第一个通知器关闭后，第二个通知器应能正常连接，且旧的消费者不再残留。
func TestJetStreamNotifierEphemeralCleanup(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	js := getJetStreamOrSkip(t, conn)

	subject := "filerotate.js.test.cleanup"
	streamName := "FILEROTATE_CLEAN"
	ensureJetStreamStream(t, js, streamName, subject)

	// 创建第一个消费者
	n1 := NewJetStreamNotifier(js, subject, streamName, discardErrors)
	ch1, _ := n1.Connect()
	n1.Close()

	// 关闭后通道应自动关闭（由于临时消费者被清理，内部 goroutine 退出）
	select {
	case _, ok := <-ch1:
		if ok {
			t.Fatal("channel should be closed after Close()")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("channel should be closed")
	}

	// 第二个消费者应能正常连接
	n2 := NewJetStreamNotifier(js, subject, streamName, discardErrors)
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
// 使用临时消费者模式，每个客户端独立消费。
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

	n1 := NewJetStreamNotifier(js1, subject, streamName, discardErrors)
	defer n1.Close()
	n2 := NewJetStreamNotifier(js2, subject, streamName, discardErrors)
	defer n2.Close()

	ch1, _ := n1.Connect()
	ch2, _ := n2.Connect()
	time.Sleep(100 * time.Millisecond)

	// 通过 n1 广播
	if err := n1.Broadcast(cmdRotate); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	// 两个客户端都应收到
	assertChannelReceived(t, ch1, cmdRotate, 2*time.Second)
	assertChannelReceived(t, ch2, cmdRotate, 2*time.Second)
}

// TestJetStreamNotifierRestartConsumer 验证新消费者只接收新消息，不会收到历史消息。
// 使用 DeliverNew 策略，旧消息会被忽略。
func TestJetStreamNotifierRestartConsumer(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	js := getJetStreamOrSkip(t, conn)

	subject := "filerotate.js.test.restart"
	streamName := "FILEROTATE_RESTART"
	ensureJetStreamStream(t, js, streamName, subject)

	// 第一个消费者接收一条消息后关闭
	n1 := NewJetStreamNotifier(js, subject, streamName, discardErrors)
	ch1, _ := n1.Connect()
	n1.Broadcast("MSG1")
	assertChannelReceived(t, ch1, "MSG1", 2*time.Second)
	n1.Close()

	// 在无消费者时广播一条消息
	n1.Broadcast("MSG2")
	time.Sleep(200 * time.Millisecond)

	// 第二个消费者只应收到 MSG3（新消息），MSG2 因为 DeliverNew 被跳过
	n2 := NewJetStreamNotifier(js, subject, streamName, discardErrors)
	defer n2.Close()
	ch2, _ := n2.Connect()
	n2.Broadcast("MSG3")
	assertChannelReceived(t, ch2, "MSG3", 2*time.Second)

	// 确认不会收到旧消息
	select {
	case cmd := <-ch2:
		t.Fatalf("should not receive old message, got %q", cmd)
	case <-time.After(500 * time.Millisecond):
		// 预期超时
	}
}
