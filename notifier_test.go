package filerotate

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/valkey-io/valkey-go"
)

// connectNATSOrSkip 尝试连接本地 NATS 服务器，失败则跳过测试。
func connectNATSOrSkip(t *testing.T) *nats.Conn {
	t.Helper()
	conn, err := nats.Connect("nats://172.20.130.90:4222")
	if err != nil {
		t.Skipf("NATS server not available: %v", err)
	}
	return conn
}

// getJetStreamOrSkip 获取 JetStream 上下文，失败则跳过测试。
func getJetStreamOrSkip(t *testing.T, conn *nats.Conn) nats.JetStreamContext {
	t.Helper()
	js, err := conn.JetStream()
	if err != nil {
		t.Skipf("JetStream not available: %v", err)
	}
	return js
}

// ensureJetStreamStream 确保测试用的 JetStream Stream 存在。
func ensureJetStreamStream(t *testing.T, js nats.JetStreamContext, streamName, subject string) {
	t.Helper()
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
	})
	if err != nil && err != nats.ErrStreamNameAlreadyInUse {
		t.Fatalf("add stream: %v", err)
	}
}

// connectValkeyOrSkip 尝试连接本地 Valkey/Redis 服务器，失败则跳过测试。
func connectValkeyOrSkip(t *testing.T) valkey.Client {
	t.Helper()
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{"localhost:6379"},
	})
	if err != nil {
		t.Skipf("Valkey/Redis not available: %v", err)
	}
	if err := client.Do(context.Background(), client.B().Ping().Build()).Error(); err != nil {
		t.Skipf("Valkey/Redis ping failed: %v", err)
	}
	return client
}

// assertChannelReceived 断言通道在超时前收到指定命令。
func assertChannelReceived(t *testing.T, ch <-chan string, expected string, timeout time.Duration) {
	t.Helper()
	select {
	case cmd := <-ch:
		if cmd != expected {
			t.Fatalf("expected %q, got %q", expected, cmd)
		}
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for %q", expected)
	}
}
