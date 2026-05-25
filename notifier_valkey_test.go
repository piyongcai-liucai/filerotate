package filerotate

import (
	"testing"
	"time"
)

// TestValkeyNotifierConnect 测试 Valkey 客户端连接功能。
func TestValkeyNotifierConnect(t *testing.T) {
	client := connectValkeyOrSkip(t)
	defer client.Close()
	n := NewValkeyNotifier(client, "filerotate.test.connect", nil)
	defer n.Close()
	ch, err := n.Connect()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if ch == nil {
		t.Fatal("expected non-nil channel")
	}
}

// TestValkeyNotifierBroadcast 测试单客户端接收广播命令。
func TestValkeyNotifierBroadcast(t *testing.T) {
	client := connectValkeyOrSkip(t)
	defer client.Close()
	n := NewValkeyNotifier(client, "filerotate.test.broadcast", nil)
	defer n.Close()
	ch, _ := n.Connect()

	// 等待订阅完成
	time.Sleep(100 * time.Millisecond)

	n.Broadcast(CmdRotate)
	assertChannelReceived(t, ch, CmdRotate, 2*time.Second)
}

// TestValkeyNotifierMultipleClients 测试多个客户端都能收到广播命令。
func TestValkeyNotifierMultipleClients(t *testing.T) {
	client1, client2 := connectValkeyOrSkip(t), connectValkeyOrSkip(t)
	defer client1.Close()
	defer client2.Close()
	channel := "filerotate.test.multi"
	n1 := NewValkeyNotifier(client1, channel, nil)
	defer n1.Close()
	n2 := NewValkeyNotifier(client2, channel, nil)
	defer n2.Close()
	ch1, _ := n1.Connect()
	ch2, _ := n2.Connect()

	// 等待订阅完成
	time.Sleep(100 * time.Millisecond)

	n1.Broadcast(CmdRotate)
	assertChannelReceived(t, ch1, CmdRotate, 2*time.Second)
	assertChannelReceived(t, ch2, CmdRotate, 2*time.Second)
}

// TestValkeyNotifierClose 测试关闭后通道被关闭。
func TestValkeyNotifierClose(t *testing.T) {
	client := connectValkeyOrSkip(t)
	defer client.Close()
	n := NewValkeyNotifier(client, "filerotate.test.close", nil)
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
