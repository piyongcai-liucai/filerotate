// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件定义进程间轮转通知的通信接口（Notifier）。
// 所有通知器（本地 IPC、NATS、JetStream、Valkey）均需实现此接口。
//
// Notifier 接口抽象了 Leader 与客户端之间的通信机制：
//   - Leader 通过 Serve() 启动服务端，等待客户端连接
//   - 客户端通过 Connect() 连接到 Leader，获取命令接收通道
//   - Leader 通过 Broadcast() 向所有客户端发送轮转命令
//   - 所有进程通过 Close() 释放资源
//
// 内置实现：
//   - NewLocalNotifier: 本地 IPC（Unix Socket / Windows 命名管道）
//   - NewNATSNotifier: NATS 核心 Pub/Sub
//   - NewJetStreamNotifier: NATS JetStream 临时消费者
//   - NewValkeyNotifier: Valkey Pub/Sub
package filerotate

// Notifier 定义进程间轮转通知的通信接口。
//
// Leader 通过此接口向所有客户端广播轮转命令（CmdRotate）。
// 所有进程（包括 Leader 自己）都需要通过 Connect() 订阅命令，
// 以便在收到 ROTATE 命令时执行文件重开操作。
//
// 接口设计遵循以下原则：
//   - 所有方法都是线程安全的
//   - Serve() 和 Connect() 可以在不同 goroutine 中并发调用
//   - Broadcast() 应确保所有当前在线的客户端都能收到命令
//   - Close() 应释放所有资源，关闭所有连接
type Notifier interface {
	// Serve 在 Leader 端启动服务，等待客户端连接。
	//
	// 对于需要启动服务端的实现（如本地 IPC），此方法会阻塞直到 Close() 被调用。
	// 对于无需服务端初始化的实现（如 NATS、Valkey），可直接返回 nil。
	//
	// 返回：
	//   - error: 服务启动失败的错误
	Serve() error

	// Connect 客户端连接到 Leader，返回一个接收命令的字符串通道。
	//
	// 此方法由所有进程（包括 Leader 自己）调用。
	// 通道中会收到 CmdRotate 等命令，当连接断开时通道关闭。
	//
	// 返回：
	//   - <-chan string: 命令接收通道，通道缓冲至少为 1
	//   - error: 连接失败的错误
	Connect() (<-chan string, error)

	// Broadcast 向所有已连接的客户端发送命令。
	//
	// 此方法由 Leader 在完成文件轮转后调用。
	// 应确保所有当前在线的客户端都能收到该命令，包括 Leader 自己。
	//
	// 参数：
	//   - cmd: 要发送的命令（通常为 CmdRotate）
	//
	// 返回：
	//   - error: 发送失败的错误
	Broadcast(cmd string) error

	// Close 关闭通知器，释放资源。
	//
	// 此方法在 Writer.Close() 中调用。
	// 应停止接受新连接，关闭所有现有连接，释放所有资源。
	//
	// 返回：
	//   - error: 关闭失败的错误
	Close() error
}

const (
	// CmdRotate 是 Leader 通知客户端执行文件重开的命令。
	//
	// 当 Leader 完成文件轮转（重命名旧文件、创建新文件）后，
	// 会通过 Broadcast 发送此命令给所有客户端。
	// 客户端收到此命令后，应关闭旧文件句柄，重新打开新文件。
	CmdRotate = "ROTATE"
)
