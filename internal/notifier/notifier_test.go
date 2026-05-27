// Package notifier 提供通知器的公共测试辅助函数和常量。
// 所有同包的测试文件均可直接使用这些定义，避免重复。
package notifier

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/valkey-io/valkey-go"
)

// discardErrors 静默丢弃错误，用于测试中抑制预期的错误输出。
func discardErrors(_ error) {}

// cmdRotate 模拟轮转命令，与上层 filerotate.CmdRotate 保持一致。
const cmdRotate = "ROTATE"

// connectNATSOrSkip 尝试连接本地 NATS 服务器，失败则跳过当前测试。
// 默认连接地址为 nats://172.20.130.90:4222。
func connectNATSOrSkip(t *testing.T) *nats.Conn {
	t.Helper()
	conn, err := nats.Connect("nats://172.20.130.90:4222")
	if err != nil {
		t.Skipf("NATS server not available: %v", err)
	}
	return conn
}

// getJetStreamOrSkip 从 NATS 连接获取 JetStream 上下文，失败则跳过测试。
// 通常在调用 connectNATSOrSkip 之后使用。
func getJetStreamOrSkip(t *testing.T, conn *nats.Conn) nats.JetStreamContext {
	t.Helper()
	js, err := conn.JetStream()
	if err != nil {
		t.Skipf("JetStream not available: %v", err)
	}
	return js
}

// ensureJetStreamStream 确保测试用的 JetStream Stream 存在。
// 如果 Stream 已存在则忽略错误，否则创建指定的 Stream。
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
// 通过发送 PING 命令验证连接可用性。
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

// assertChannelReceived 断言在超时时间内从通道中收到指定的命令。
// 如果超时未收到或收到的命令不匹配，则标记测试失败。
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
