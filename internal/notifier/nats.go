// Package notifier 提供进程间通知的内部实现。
//
// 本文件实现基于 NATS 核心 Pub/Sub 的通知器。
package notifier

import (
	"fmt"
	"os"

	"github.com/nats-io/nats.go"
)

// NATSNotifier 使用 NATS 核心 Pub/Sub 实现广播通知。
//
// 所有订阅同一 subject 的客户端都会收到命令，消息无持久化。
// 适合已有 NATS 基础设施的简单广播场景。
//
// NATS 的 Pub/Sub 是广播模式（fan-out），每个订阅者都会收到消息副本。
// 无需额外的队列组或持久化配置，开箱即用。
type NATSNotifier struct {
	// conn NATS 连接，由调用者创建并管理生命周期
	conn *nats.Conn

	// subject 通信主题，所有进程必须使用相同主题
	// 例如 "filerotate.rotate"
	subject string

	// sub 订阅句柄，用于取消订阅
	sub *nats.Subscription

	// msgCh 内部命令接收通道，Connect() 返回此通道给调用者
	msgCh chan string

	// errorHandler 错误处理回调，如果为 nil，错误将打印到 stderr
	errorHandler func(error)
}

// NewNATSNotifier 创建一个 NATS 通知器。
//
// 参数：
//   - conn: 已建立的 NATS 连接，由调用者管理生命周期
//   - subject: 用于通信的主题，所有进程必须使用相同的 subject
//   - errorHandler: 错误处理回调，如果为 nil，错误将打印到 stderr
//
// 示例：
//
//	nc, _ := nats.Connect("nats://localhost:4222")
//	notifier := notifier.NewNATSNotifier(nc, "filerotate.rotate", nil)
func NewNATSNotifier(conn *nats.Conn, subject string, errorHandler func(error)) *NATSNotifier {
	if errorHandler == nil {
		errorHandler = func(err error) {
			fmt.Fprintf(os.Stderr, "[NATS] 错误: %v\n", err)
		}
	}
	return &NATSNotifier{
		conn:         conn,
		subject:      subject,
		msgCh:        make(chan string, 8),
		errorHandler: errorHandler,
	}
}

// Serve 在 Leader 端启动服务。
//
// NATS 无需显式监听，服务端由 NATS Server 提供。
// 直接返回 nil 表示服务已就绪。
func (n *NATSNotifier) Serve() error {
	return nil
}

// Connect 订阅主题，返回命令接收通道。
//
// 所有订阅该 subject 的客户端都会收到消息（广播模式）。
// NATS 的 Subscribe 是异步的，回调函数在收到消息时被调用。
func (n *NATSNotifier) Connect() (<-chan string, error) {
	if n.sub != nil {
		n.sub.Unsubscribe()
	}
	sub, err := n.conn.Subscribe(n.subject, func(msg *nats.Msg) {
		select {
		case n.msgCh <- string(msg.Data):
		default:
		}
	})
	if err != nil {
		n.reportError(fmt.Errorf("NATS 订阅失败: %w", err))
		return nil, err
	}
	n.sub = sub
	return n.msgCh, nil
}

// Broadcast 发布命令到主题，所有订阅者都会收到。
func (n *NATSNotifier) Broadcast(cmd string) error {
	err := n.conn.Publish(n.subject, []byte(cmd))
	if err != nil {
		n.reportError(fmt.Errorf("NATS 发布失败: %w", err))
	}
	return err
}

// Close 取消订阅并关闭通道，释放资源。
// 使用 Drain() 等待所有在途回调完成，避免向已关闭通道发送导致 panic。
func (n *NATSNotifier) Close() error {
	if n.sub != nil {
		n.sub.Drain()
	}
	close(n.msgCh)
	return nil
}

// reportError 向错误处理器报告错误。
func (n *NATSNotifier) reportError(err error) {
	if n.errorHandler != nil {
		n.errorHandler(err)
	}
}
