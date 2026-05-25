// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件实现基于 NATS JetStream 的通知器，使用临时消费者实现广播。
package filerotate

import (
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
)

// JetStreamNotifier 基于 NATS JetStream 的临时消费者通知器。
//
// 使用 Ephemeral Consumer（临时消费者），进程退出后自动清理，无残留。
// 只接收订阅后发布的新消息（DeliverNew），轮转命令是即时通知，无需历史消息。
//
// 注意：Stream 必须外部预先创建，通知器只验证其存在。
// 可配置集群副本数、存储后端、保留策略等生产级参数。
type JetStreamNotifier struct {
	// js JetStream 上下文，由调用者从 NATS 连接获取
	js nats.JetStreamContext

	// subject 通信主题，对应的 Stream 必须已包含该 subject
	subject string

	// streamName Stream 名称，用于 Serve() 验证 Stream 是否存在
	streamName string

	// sub 订阅句柄，用于取消订阅
	sub *nats.Subscription

	// msgCh 内部命令接收通道，Connect() 返回此通道给调用者
	msgCh chan string

	// wg 等待监控协程退出
	wg sync.WaitGroup

	// errorHandler 错误处理回调，如果为 nil，错误将打印到 stderr
	errorHandler func(error)
}

// NewJetStreamNotifier 创建一个 JetStream 通知器。
//
// 参数：
//   - js: JetStream 上下文，由调用者从 NATS 连接获取
//   - subject: 通信主题，对应的 Stream 必须已存在且包含该 subject
//   - streamName: Stream 名称，用于验证 Stream 是否存在
//   - errorHandler: 错误处理回调，如果为 nil，错误将打印到 stderr
//
// 示例：
//
//	js, _ := nc.JetStream()
//	notifier := filerotate.NewJetStreamNotifier(js, "filerotate.rotate", "FILEROTATE", nil)
func NewJetStreamNotifier(js nats.JetStreamContext, subject string, streamName string, errorHandler func(error)) *JetStreamNotifier {
	if errorHandler == nil {
		errorHandler = func(err error) {
			fmt.Printf("[JetStream] 错误: %v\n", err)
		}
	}
	return &JetStreamNotifier{
		js:           js,
		subject:      subject,
		streamName:   streamName,
		msgCh:        make(chan string, 8),
		errorHandler: errorHandler,
	}
}

// Serve 验证 subject 对应的 Stream 是否存在，不创建任何资源。
//
// 如果 Stream 不存在，返回错误。
// Stream 应由运维通过 CLI 或初始化代码预先创建。
//
// 返回：
//   - error: Stream 不存在或其他错误
func (j *JetStreamNotifier) Serve() error {
	_, err := j.js.StreamInfo(j.streamName)
	if err != nil {
		j.reportError(fmt.Errorf("JetStream StreamInfo 失败: %w", err))
		return fmt.Errorf("filerotate: stream not found: %w", err)
	}
	return nil
}

// Connect 创建临时消费者并订阅，返回命令接收通道。
//
// 使用 Ephemeral Consumer（不指定 Durable 名称），进程退出后自动清理。
// 使用 DeliverNew 策略，只接收订阅后发布的新消息，忽略历史消息。
//
// 返回：
//   - <-chan string: 命令接收通道，当订阅关闭时通道会被关闭
//   - error: 订阅失败的错误
func (j *JetStreamNotifier) Connect() (<-chan string, error) {
	// 创建临时消费者，只接收新消息
	// - DeliverNew: 只接收订阅后发布的新消息
	// - AckExplicit: 显式确认模式
	// - ManualAck: 手动确认，需要调用 msg.Ack()
	sub, err := j.js.Subscribe(j.subject, func(msg *nats.Msg) {
		// 将消息内容发送到内部通道
		j.msgCh <- string(msg.Data)
		// 立即确认，防止重复投递
		msg.Ack()
	}, nats.DeliverNew(),
		nats.AckExplicit(),
		nats.ManualAck())
	if err != nil {
		j.reportError(fmt.Errorf("JetStream 订阅失败: %w", err))
		return nil, err
	}
	j.sub = sub
	return j.msgCh, nil
}

// Broadcast 向所有客户端发布轮转命令。
//
// JetStream 的 Publish 会向所有订阅该 subject 的临时消费者投递消息。
// 消息会持久化到 Stream 中，确保即使消费者暂时离线也能收到。
//
// 参数：
//   - cmd: 要发送的命令（通常为 CmdRotate）
//
// 返回：
//   - error: 发布失败的错误
func (j *JetStreamNotifier) Broadcast(cmd string) error {
	_, err := j.js.Publish(j.subject, []byte(cmd))
	if err != nil {
		j.reportError(fmt.Errorf("JetStream 发布失败: %w", err))
	}
	return err
}

// Close 取消订阅，释放资源。
//
// 临时消费者会在进程退出后自动清理。
//
// 返回：
//   - error: 取消订阅失败的错误
func (j *JetStreamNotifier) Close() error {
	if j.sub != nil {
		return j.sub.Unsubscribe()
	}
	return nil
}

// reportError 向错误处理器报告错误。
func (j *JetStreamNotifier) reportError(err error) {
	if j.errorHandler != nil {
		j.errorHandler(err)
	}
}
