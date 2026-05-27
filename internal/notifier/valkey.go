// Package notifier 提供进程间通知的内部实现。
//
// 本文件实现基于 Valkey (Redis) Pub/Sub 的通知器。
package notifier

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/valkey-io/valkey-go"
)

// ValkeyNotifier 基于 Valkey (Redis) Pub/Sub 的通知器。
//
// 所有订阅同一频道的客户端都会收到命令，消息无持久化。
// 适合已有 Valkey 基础设施的场景。
//
// Valkey 的 Pub/Sub 是广播模式（fan-out），每个订阅者都会收到消息副本。
// 消息不持久化，如果订阅者离线，消息会丢失。
type ValkeyNotifier struct {
	// client Valkey 客户端，由调用者创建并管理生命周期
	client valkey.Client

	// channel Pub/Sub 频道名称，所有进程必须使用相同的频道
	// 例如 "filerotate.rotate"
	channel string

	// msgCh 内部命令接收通道，Connect() 返回此通道给调用者
	msgCh chan string

	// cancel 取消上下文，用于关闭订阅
	// 调用 cancel() 会取消 ctx，通知 Receive 协程退出
	cancel context.CancelFunc

	// ctx 上下文，控制订阅生命周期
	// 当 ctx 被取消时，Receive 方法会返回
	ctx context.Context

	// wg 等待订阅协程退出
	// 确保 Close() 时订阅协程已完全退出并关闭通道
	wg sync.WaitGroup

	// errorHandler 错误处理回调，如果为 nil，错误将打印到 stderr
	errorHandler func(error)
}

// NewValkeyNotifier 创建一个 Valkey 通知器。
//
// 参数：
//   - client: 已连接的 Valkey 客户端，由调用者管理生命周期
//   - channel: Pub/Sub 频道名称，所有进程必须使用相同的频道
//   - errorHandler: 错误处理回调，如果为 nil，错误将打印到 stderr
//
// 示例：
//
//	client, _ := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"localhost:6379"}})
//	notifier := notifier.NewValkeyNotifier(client, "filerotate.rotate", nil)
func NewValkeyNotifier(client valkey.Client, channel string, errorHandler func(error)) *ValkeyNotifier {
	// 创建可取消的上下文，用于控制订阅生命周期
	ctx, cancel := context.WithCancel(context.Background())

	if errorHandler == nil {
		errorHandler = func(err error) {
			fmt.Fprintf(os.Stderr, "[Valkey] 错误: %v\n", err)
		}
	}

	return &ValkeyNotifier{
		client:       client,
		channel:      channel,
		msgCh:        make(chan string, 8),
		ctx:          ctx,
		cancel:       cancel,
		errorHandler: errorHandler,
	}
}

// Serve 在 Leader 端启动服务。
//
// Valkey Pub/Sub 无需显式监听，服务端由 Valkey Server 提供。
// 直接返回 nil 表示服务已就绪。
//
// 返回：
//   - error: 始终为 nil
func (v *ValkeyNotifier) Serve() error {
	// Valkey 的服务端是 Valkey Server，无需在客户端启动服务
	return nil
}

// Connect 订阅频道，返回命令接收通道。
//
// 使用 valkey-go 的 Receive 方法监听 Pub/Sub 消息。
// 内部启动一个 goroutine 持续接收消息，并将消息内容发送到 msgCh 通道。
//
// 当 Close() 被调用时，ctx 被取消，Receive 方法返回，goroutine 退出。
// goroutine 退出时会自动关闭 msgCh 通道。
//
// 返回：
//   - <-chan string: 命令接收通道，当订阅关闭时通道会被关闭
//   - error: 订阅失败的错误
func (v *ValkeyNotifier) Connect() (<-chan string, error) {
	// 取消旧订阅并等待退出，避免重复连接时 close 已关闭的 channel
	if v.cancel != nil {
		v.cancel()
		v.wg.Wait()
	}

	// 为本次连接创建新的 channel 和 context
	v.msgCh = make(chan string, 8)
	v.ctx, v.cancel = context.WithCancel(context.Background())

	// 构建 SUBSCRIBE 命令
	subscribeCmd := v.client.B().Subscribe().Channel(v.channel).Build()

	// 启动协程，使用 Receive 监听消息
	v.wg.Add(1)
	go func() {
		defer v.wg.Done()
		defer close(v.msgCh) // 协程退出时关闭通道

		// Receive 接受 SUBSCRIBE 命令和一个回调函数
		// 回调函数会在收到消息时被调用
		err := v.client.Receive(v.ctx, subscribeCmd, func(msg valkey.PubSubMessage) {
			// 将消息内容发送到内部通道
			// 如果 ctx 已取消，则不发送
			select {
			case v.msgCh <- msg.Message:
			case <-v.ctx.Done():
				// 上下文取消，停止发送
			}
		})
		if err != nil {
			// 上下文取消或连接断开时退出
			v.reportError(fmt.Errorf("Valkey Receive 失败: %w", err))
		}
	}()

	return v.msgCh, nil
}

// Broadcast 发布命令到频道，所有订阅者都会收到。
//
// Valkey 的 Publish 是广播模式（fan-out），每个订阅者都会收到消息副本。
// 消息无持久化，如果订阅者离线，消息会丢失。
//
// 参数：
//   - cmd: 要发送的命令（通常为 "ROTATE"）
//
// 返回：
//   - error: 发布失败的错误
func (v *ValkeyNotifier) Broadcast(cmd string) error {
	// 构建 PUBLISH 命令
	publishCmd := v.client.B().Publish().Channel(v.channel).Message(cmd).Build()

	// 执行命令
	err := v.client.Do(v.ctx, publishCmd).Error()
	if err != nil {
		v.reportError(fmt.Errorf("Valkey 发布失败: %w", err))
	}
	return err
}

// Close 取消订阅，释放资源。
//
// 取消上下文，通知 Receive 协程退出。
// 等待订阅协程完全退出后再返回，确保 msgCh 通道被正确关闭。
//
// 返回：
//   - error: 始终为 nil
func (v *ValkeyNotifier) Close() error {
	// 取消上下文，通知 Receive 协程退出
	v.cancel()

	// 等待订阅协程完全退出
	// 协程退出时会自动关闭 msgCh 通道
	v.wg.Wait()

	return nil
}

// reportError 向错误处理器报告错误。
//
// 参数：
//   - err: 要报告的错误
func (v *ValkeyNotifier) reportError(err error) {
	if v.errorHandler != nil {
		v.errorHandler(err)
	}
}
