// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件实现基于 NATS 核心 Pub/Sub 的通知器，用于多进程间的广播通信。
package filerotate

import (
	"fmt"
	"sync"

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

	// wg 等待监控协程退出（当前未使用，保留用于未来扩展）
	wg sync.WaitGroup

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
//	notifier := filerotate.NewNATSNotifier(nc, "filerotate.rotate", nil)
func NewNATSNotifier(conn *nats.Conn, subject string, errorHandler func(error)) *NATSNotifier {
	if errorHandler == nil {
		errorHandler = func(err error) {
			fmt.Printf("[NATS] 错误: %v\n", err)
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
//
// 返回：
//   - error: 始终为 nil
func (n *NATSNotifier) Serve() error {
	// NATS 的服务端是 NATS Server，无需在客户端启动服务
	return nil
}

// Connect 订阅主题，返回命令接收通道。
//
// 所有订阅该 subject 的客户端都会收到消息（广播模式）。
// NATS 的 Subscribe 是异步的，回调函数在收到消息时被调用。
//
// 返回：
//   - <-chan string: 命令接收通道，当订阅取消时通道不会自动关闭
//   - error: 订阅失败的错误
func (n *NATSNotifier) Connect() (<-chan string, error) {
	// 订阅主题，设置回调函数
	// 回调函数在收到消息时将消息内容发送到内部通道
	sub, err := n.conn.Subscribe(n.subject, func(msg *nats.Msg) {
		// 将消息内容发送到内部通道
		// 注意：如果通道已满，会阻塞，建议设置适当的通道缓冲
		n.msgCh <- string(msg.Data)
	})
	if err != nil {
		n.reportError(fmt.Errorf("NATS 订阅失败: %w", err))
		return nil, err
	}
	n.sub = sub
	return n.msgCh, nil
}

// Broadcast 发布命令到主题，所有订阅者都会收到。
//
// NATS 的 Publish 是广播模式（fan-out），每个订阅者都会收到消息副本。
// 消息无持久化，如果订阅者离线，消息会丢失。
//
// 参数：
//   - cmd: 要发送的命令（通常为 CmdRotate）
//
// 返回：
//   - error: 发布失败的错误
func (n *NATSNotifier) Broadcast(cmd string) error {
	err := n.conn.Publish(n.subject, []byte(cmd))
	if err != nil {
		n.reportError(fmt.Errorf("NATS 发布失败: %w", err))
	}
	return err
}

// Close 取消订阅，释放资源。
//
// 调用 Unsubscribe 后，回调函数不再被触发。
//
// 返回：
//   - error: 取消订阅失败的错误
func (n *NATSNotifier) Close() error {
	if n.sub != nil {
		return n.sub.Unsubscribe()
	}
	return nil
}

// reportError 向错误处理器报告错误。
//
// 参数：
//   - err: 要报告的错误
func (n *NATSNotifier) reportError(err error) {
	if n.errorHandler != nil {
		n.errorHandler(err)
	}
}
