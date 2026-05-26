// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件定义 Notifier 接口及工厂函数，用于进程间轮转通知。
// 所有通知器实现（本地 IPC、NATS、JetStream、Valkey）均通过此接口对外暴露，
// 具体实现位于 internal/notifier 包中，外部不可见。
package filerotate

import (
	"github.com/nats-io/nats.go"
	"github.com/valkey-io/valkey-go"

	"github.com/piyongcai-liucai/filerotate/internal/notifier"
)

// Notifier 定义进程间轮转通知的通信接口。
// Leader 通过此接口向所有客户端广播轮转命令（CmdRotate）。
// 所有进程（包括 Leader 自己）都需要通过 Connect() 订阅命令，
// 以便在收到 ROTATE 命令时执行文件重开操作。
type Notifier interface {
	// Serve 在 Leader 端启动服务，等待客户端连接。
	// 对于无需服务端初始化的实现（如 NATS），可直接返回 nil。
	Serve() error

	// Connect 客户端连接到 Leader，返回一个接收命令的字符串通道。
	// 通道中会收到 CmdRotate 等命令，当连接断开时通道关闭。
	Connect() (<-chan string, error)

	// Broadcast 向所有已连接的客户端发送命令。
	// 应确保所有当前在线的客户端都能收到该命令。
	Broadcast(cmd string) error

	// Close 关闭通知器，释放资源。
	Close() error
}

const (
	// CmdRotate 是 Leader 通知客户端执行文件重开的命令。
	CmdRotate = "ROTATE"
)

// NewLocalNotifier 创建一个本地 IPC 通知器。
// commPath 为通信路径：Unix 上为 Socket 文件路径，Windows 上为命名管道名称（不含 \\.\pipe\ 前缀）。
// errorHandler 为错误处理回调，若为 nil 则默认打印到 stderr。
func NewLocalNotifier(commPath string, errorHandler func(error)) Notifier {
	return notifier.NewLocalNotifier(commPath, errorHandler)
}

// NewNATSNotifier 创建一个基于 NATS 核心 Pub/Sub 的通知器。
// conn 为已建立的 NATS 连接，subject 为通信主题，所有进程必须使用相同的主题。
// errorHandler 为错误处理回调，若为 nil 则默认打印到 stderr。
func NewNATSNotifier(conn *nats.Conn, subject string, errorHandler func(error)) Notifier {
	return notifier.NewNATSNotifier(conn, subject, errorHandler)
}

// NewJetStreamNotifier 创建一个基于 NATS JetStream 的通知器。
// 使用临时消费者（Ephemeral Consumer），进程退出后自动清理，只接收新消息。
// streamName 为预先创建的 Stream 名称，Serve 会验证其存在。
// errorHandler 为错误处理回调，若为 nil 则默认打印到 stderr。
func NewJetStreamNotifier(js nats.JetStreamContext, subject string, streamName string, errorHandler func(error)) Notifier {
	return notifier.NewJetStreamNotifier(js, subject, streamName, errorHandler)
}

// NewValkeyNotifier 创建一个基于 Valkey Pub/Sub 的通知器。
// client 为已连接的 Valkey 客户端，channel 为频道名称，所有进程必须使用相同的频道。
// errorHandler 为错误处理回调，若为 nil 则默认打印到 stderr。
func NewValkeyNotifier(client valkey.Client, channel string, errorHandler func(error)) Notifier {
	return notifier.NewValkeyNotifier(client, channel, errorHandler)
}
