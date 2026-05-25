package filerotate

import (
	"testing"
	"time"
)

// TestJetStreamNotifierConnect 测试 JetStream 客户端连接功能。
func TestJetStreamNotifierConnect(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	js := getJetStreamOrSkip(t, conn)
	subject := "filerotate.js.test.connect"
	streamName := "FILEROTATE_CONNECT"
	ensureJetStreamStream(t, js, streamName, subject)
	n := NewJetStreamNotifier(js, subject, streamName, nil)
	defer n.Close()
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

// TestJetStreamNotifierBroadcast 测试单客户端接收广播命令。
func TestJetStreamNotifierBroadcast(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	js := getJetStreamOrSkip(t, conn)
	subject := "filerotate.js.test.broadcast"
	streamName := "FILEROTATE_BCAST"
	ensureJetStreamStream(t, js, streamName, subject)
	n := NewJetStreamNotifier(js, subject, streamName, nil)
	defer n.Close()
	ch, _ := n.Connect()
	n.Broadcast(CmdRotate)
	assertChannelReceived(t, ch, CmdRotate, 2*time.Second)
}

// TestJetStreamNotifierServeFailsForMissingStream 测试缺少 Stream 时 Serve 报错。
func TestJetStreamNotifierServeFailsForMissingStream(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	js := getJetStreamOrSkip(t, conn)
	n := NewJetStreamNotifier(js, "filerotate.nonexistent", "NONEXISTENT_STREAM", nil)
	if err := n.Serve(); err == nil {
		t.Fatal("Serve should fail")
	}
}

// TestJetStreamNotifierEphemeralCleanup 测试临时消费者的自动清理。
func TestJetStreamNotifierEphemeralCleanup(t *testing.T) {
	conn := connectNATSOrSkip(t)
	defer conn.Close()
	js := getJetStreamOrSkip(t, conn)
	subject := "filerotate.js.test.cleanup"
	streamName := "FILEROTATE_CLEAN"
	ensureJetStreamStream(t, js, streamName, subject)
	n1 := NewJetStreamNotifier(js, subject, streamName, nil)
	ch1, _ := n1.Connect()
	n1.Close()
	select {
	case _, ok := <-ch1:
		if ok {
			t.Fatal("channel should be closed")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("channel should be closed")
	}
	n2 := NewJetStreamNotifier(js, subject, streamName, nil)
	defer n2.Close()
	ch2, err := n2.Connect()
	if err != nil {
		t.Fatalf("connect after cleanup: %v", err)
	}
	if ch2 == nil {
		t.Fatal("expected non-nil channel")
	}
}
