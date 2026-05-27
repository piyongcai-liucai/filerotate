// Package notifier 提供 Valkey Pub/Sub 通知器的单元测试。
// 测试覆盖连接、广播、多客户端、通道关闭、频道隔离以及无持久化特性。
// 需要本地运行 Valkey/Redis 服务，否则测试会自动跳过。
package notifier

import (
	"testing"
	"time"
)

// TestValkeyNotifierConnect 验证通知器能正常订阅并返回命令通道。
func TestValkeyNotifierConnect(t *testing.T) {
	client := connectValkeyOrSkip(t)
	defer client.Close()

	n := NewValkeyNotifier(client, "filerotate.test.connect", discardErrors)
	defer n.Close()

	ch, err := n.Connect()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if ch == nil {
		t.Fatal("expected non-nil channel")
	}
}

// TestValkeyNotifierBroadcast 验证单客户端可以接收到广播命令。
func TestValkeyNotifierBroadcast(t *testing.T) {
	client := connectValkeyOrSkip(t)
	defer client.Close()

	n := NewValkeyNotifier(client, "filerotate.test.broadcast", discardErrors)
	defer n.Close()

	ch, _ := n.Connect()
	// 等待订阅完成，否则可能丢失消息
	time.Sleep(100 * time.Millisecond)

	if err := n.Broadcast(cmdRotate); err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	assertChannelReceived(t, ch, cmdRotate, 2*time.Second)
}

// TestValkeyNotifierMultipleClients 验证多个客户端都能收到广播命令。
func TestValkeyNotifierMultipleClients(t *testing.T) {
	client1 := connectValkeyOrSkip(t)
	defer client1.Close()
	client2 := connectValkeyOrSkip(t)
	defer client2.Close()

	channel := "filerotate.test.multi"
	n1 := NewValkeyNotifier(client1, channel, discardErrors)
	defer n1.Close()
	n2 := NewValkeyNotifier(client2, channel, discardErrors)
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

// TestValkeyNotifierClose 验证关闭通知器后命令通道被正确关闭。
func TestValkeyNotifierClose(t *testing.T) {
	client := connectValkeyOrSkip(t)
	defer client.Close()

	n := NewValkeyNotifier(client, "filerotate.test.close", discardErrors)
	ch, _ := n.Connect()
	time.Sleep(100 * time.Millisecond)

	n.Close()

	// 通道应被关闭
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel should be closed after Close()")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("channel should be closed")
	}
}

// TestValkeyNotifierDifferentChannels 验证不同频道的隔离性。
func TestValkeyNotifierDifferentChannels(t *testing.T) {
	client := connectValkeyOrSkip(t)
	defer client.Close()

	n1 := NewValkeyNotifier(client, "filerotate.test.channel1", discardErrors)
	defer n1.Close()
	n2 := NewValkeyNotifier(client, "filerotate.test.channel2", discardErrors)
	defer n2.Close()

	ch1, _ := n1.Connect()
	ch2, _ := n2.Connect()
	time.Sleep(100 * time.Millisecond)

	// 向 channel1 广播
	n1.Broadcast(cmdRotate)

	// ch1 应收到
	assertChannelReceived(t, ch1, cmdRotate, 2*time.Second)

	// ch2 不应收到
	select {
	case <-ch2:
		t.Fatal("ch2 should not receive message on different channel")
	case <-time.After(500 * time.Millisecond):
		// 预期超时
	}
}

// TestValkeyNotifierNoPersistence 验证 Valkey Pub/Sub 不持久化消息。
// 订阅前发布的消息不会被收到。
func TestValkeyNotifierNoPersistence(t *testing.T) {
	client := connectValkeyOrSkip(t)
	defer client.Close()

	n := NewValkeyNotifier(client, "filerotate.test.nopersist", discardErrors)

	// 订阅前发布消息
	n.Broadcast("BEFORE_SUBSCRIBE")
	time.Sleep(100 * time.Millisecond)

	// 订阅后再发布
	ch, _ := n.Connect()
	time.Sleep(100 * time.Millisecond)
	n.Broadcast("AFTER_SUBSCRIBE")

	// 应只收到订阅后的消息
	assertChannelReceived(t, ch, "AFTER_SUBSCRIBE", 2*time.Second)

	// 不应收到订阅前的消息
	select {
	case cmd := <-ch:
		t.Fatalf("should not receive pre-subscribe message, got %q", cmd)
	case <-time.After(500 * time.Millisecond):
		// 预期超时
	}
}

// ---------- 边界测试 ----------

func TestValkeyNotifier_NilErrorHandler(t *testing.T) {
	client := connectValkeyOrSkip(t)
	defer client.Close()

	n := NewValkeyNotifier(client, "filerotate.test.nilhandler", nil)
	defer n.Close()

	// reportError 在 nil handler 时应安全返回
	n.reportError(nil)
}

func TestValkeyNotifier_Serve(t *testing.T) {
	client := connectValkeyOrSkip(t)
	defer client.Close()

	n := NewValkeyNotifier(client, "filerotate.test.serve", discardErrors)
	defer n.Close()

	if err := n.Serve(); err != nil {
		t.Fatalf("Serve should return nil: %v", err)
	}
}

func TestValkeyNotifier_BroadcastError(t *testing.T) {
	client := connectValkeyOrSkip(t)
	n := NewValkeyNotifier(client, "filerotate.test.bcasterr", discardErrors)

	client.Close() // 关闭连接使后续操作失败
	err := n.Broadcast("ROTATE")
	if err == nil {
		t.Fatal("expected broadcast error after client close")
	}
	n.Close()
}
