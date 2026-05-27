// Package notifier 提供进程间通知的内部实现。
//
// 本文件实现基于 NATS JetStream 的临时消费者通知器。
// Stream 必须由外部预先创建，通知器只验证其存在。
package notifier

import (
	"fmt"
	"os"
	"sync"
	"time"

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

	// wg 用于等待消息处理协程结束
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
//	notifier := notifier.NewJetStreamNotifier(js, "filerotate.rotate", "FILEROTATE", nil)
func NewJetStreamNotifier(js nats.JetStreamContext, subject string, streamName string, errorHandler func(error)) *JetStreamNotifier {
	if errorHandler == nil {
		errorHandler = func(err error) {
			fmt.Fprintf(os.Stderr, "[JetStream] 错误: %v\n", err)
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
	// 取消旧订阅，避免重复连接时资源泄漏
	if j.sub != nil {
		j.sub.Unsubscribe()
	}

	sub, err := j.js.Subscribe(j.subject, func(msg *nats.Msg) {
		select {
		case j.msgCh <- string(msg.Data):
			msg.Ack()
		default:
		}
	}, nats.DeliverNew(),
		nats.AckExplicit(),
		nats.ManualAck())
	if err != nil {
		j.reportError(fmt.Errorf("JetStream 订阅失败: %w", err))
		return nil, err
	}
	j.sub = sub

	// 启动一个 goroutine 等待订阅结束，并在退出时关闭通道
	j.wg.Add(1)
	go func() {
		defer j.wg.Done()
		defer close(j.msgCh)
		for j.sub.IsValid() {
			time.Sleep(100 * time.Millisecond)
		}
	}()

	return j.msgCh, nil
}

// Broadcast 向所有客户端发布轮转命令。
//
// JetStream 的 Publish 会向所有订阅该 subject 的临时消费者投递消息。
// 消息会持久化到 Stream 中，确保即使消费者暂时离线也能收到。
//
// 参数：
//   - cmd: 要发送的命令（通常为 "ROTATE"）
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
// 使用 Drain() 等待所有在途回调完成后再关闭通道，避免向已关闭通道发送导致 panic。
//
// 返回：
//   - error: 取消订阅失败的错误
func (j *JetStreamNotifier) Close() error {
	if j.sub != nil {
		j.sub.Drain()
	}
	// 等待后台 goroutine 退出，它会负责关闭 msgCh
	j.wg.Wait()
	return nil
}

// reportError 向错误处理器报告错误。
func (j *JetStreamNotifier) reportError(err error) {
	if j.errorHandler != nil {
		j.errorHandler(err)
	}
}
