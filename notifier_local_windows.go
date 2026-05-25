//go:build windows

// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件实现基于 Windows 命名管道的本地 IPC 通知器，用于 Windows 系统。
package filerotate

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sync"

	"gopkg.in/natefinch/npipe.v2"
)

// localNotifier 基于 Windows 命名管道的本地 IPC 通知器实现。
//
// 在 Windows 下，Leader 监听命名管道，客户端通过管道名称连接。
// 所有进程运行在同一台机器上，通信效率高，无网络开销。
//
// 命名管道是 Windows 下的进程间通信机制，类似于 Unix 域 Socket。
// 管道名称格式为 \\.\pipe\<name>，其中 <name> 由用户指定。
type localNotifier struct {
	// pipeName 命名管道完整路径，格式 \\.\pipe\<name>
	pipeName string

	// listener 监听器，仅 Leader 持有，用于接受客户端连接
	listener net.Listener

	// clients 已连接的客户端集合，key 为连接对象
	// Leader 向所有客户端广播命令
	clients map[net.Conn]struct{}

	// mu 保护 clients 的互斥锁
	mu sync.Mutex

	// done 关闭信号，用于优雅退出 Accept 循环
	done chan struct{}

	// errorHandler 错误处理回调，如果为 nil，错误将打印到 stderr
	errorHandler func(error)
}

// NewLocalNotifier 创建一个本地 IPC 通知器。
//
// commPath 为通信路径，在 Windows 上为管道名称（不含 \\.\pipe\ 前缀）。
// 同一机器上的所有进程应使用相同的 commPath。
//
// 参数：
//   - commPath: 管道名称（不含 \\.\pipe\ 前缀）
//   - errorHandler: 错误处理回调，如果为 nil，错误将打印到 stderr
//
// 返回：
//   - Notifier: 通知器接口实例
func NewLocalNotifier(commPath string, errorHandler func(error)) Notifier {
	if errorHandler == nil {
		errorHandler = func(err error) {
			fmt.Fprintf(os.Stderr, "[Local] 错误: %v\n", err)
		}
	}
	return &localNotifier{
		// 构造完整的命名管道路径：\\.\pipe\<name>
		pipeName:     `\\.\pipe\` + commPath,
		clients:      make(map[net.Conn]struct{}),
		done:         make(chan struct{}),
		errorHandler: errorHandler,
	}
}

// Serve 启动命名管道监听，接受客户端连接。
//
// 调用 Close 后，Accept 循环会检测到 done 关闭并退出。
// 此方法通常由 Leader 调用，运行在独立的 goroutine 中。
//
// 返回：
//   - error: 监听失败的错误
func (l *localNotifier) Serve() error {
	// 创建命名管道监听器
	// npipe.Listen 是对 Windows API CreateNamedPipe 的封装
	listener, err := npipe.Listen(l.pipeName)
	if err != nil {
		l.reportError(fmt.Errorf("命名管道 Listen 失败: %w", err))
		return err
	}
	l.listener = listener

	// 循环接受客户端连接
	for {
		conn, err := listener.Accept()
		if err != nil {
			// 检查是否为正常关闭（Close 方法关闭了 done 通道）
			select {
			case <-l.done:
				return nil
			default:
				// 其他临时错误，继续监听
				l.reportError(fmt.Errorf("命名管道 Accept 失败: %w", err))
				continue
			}
		}

		// 将新连接加入客户端列表
		l.mu.Lock()
		l.clients[conn] = struct{}{}
		l.mu.Unlock()

		// 为每个连接启动独立的处理协程
		go l.handleConn(conn)
	}
}

// handleConn 处理单个客户端连接，持续读取直到对方断开。
//
// 连接断开时会自动从客户端列表中移除。
// 此方法运行在独立的 goroutine 中。
//
// 参数：
//   - conn: 客户端连接
func (l *localNotifier) handleConn(conn net.Conn) {
	defer func() {
		// 连接断开时，从客户端列表移除并关闭连接
		l.mu.Lock()
		delete(l.clients, conn)
		l.mu.Unlock()
		conn.Close()
	}()

	// 持续读取，保持连接活跃，同时检测断开
	// 客户端不会发送数据，这里只是为了检测连接状态
	buf := make([]byte, 1024)
	for {
		if _, err := conn.Read(buf); err != nil {
			// 对方断开或出错，退出循环
			return
		}
	}
}

// Connect 作为客户端连接到 Leader 的命名管道，返回命令接收通道。
//
// 此方法由客户端（非 Leader 进程）调用，用于接收 Leader 广播的命令。
//
// 返回：
//   - <-chan string: 命令接收通道，当连接断开时通道关闭
//   - error: 连接失败的错误
func (l *localNotifier) Connect() (<-chan string, error) {
	// 连接到 Leader 的命名管道
	// npipe.Dial 是对 Windows API ConnectNamedPipe 的封装
	conn, err := npipe.Dial(l.pipeName)
	if err != nil {
		l.reportError(fmt.Errorf("命名管道 Dial 失败: %w", err))
		return nil, err
	}

	// 创建命令接收通道
	ch := make(chan string, 5)
	go func() {
		defer close(ch)
		defer conn.Close()

		// 使用 Scanner 逐行读取命令
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			ch <- scanner.Text()
		}
		// scanner 遇到错误或 EOF 时自动退出
	}()

	return ch, nil
}

// Broadcast 向所有已连接的客户端发送命令。
//
// 命名管道是可靠流，写入成功即表示数据已到达对端内核缓冲区。
// 如果某个客户端断开，写入失败会自动将其移出列表。
//
// 参数：
//   - cmd: 要发送的命令（通常为 CmdRotate）
//
// 返回：
//   - error: 始终返回 nil（错误已通过 errorHandler 报告）
func (l *localNotifier) Broadcast(cmd string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for conn := range l.clients {
		_, err := fmt.Fprintln(conn, cmd)
		if err != nil {
			// 发送失败，报告错误并移除失效连接
			l.reportError(fmt.Errorf("命名管道 写入失败: %w", err))
			delete(l.clients, conn)
		}
	}
	return nil
}

// Close 关闭通知器，停止监听并释放资源。
//
// 关闭 done 通道通知 Accept 循环退出，关闭 listener 释放端口。
//
// 返回：
//   - error: 关闭 listener 的错误
func (l *localNotifier) Close() error {
	close(l.done)
	if l.listener != nil {
		return l.listener.Close()
	}
	return nil
}

// reportError 向错误处理器报告错误。
//
// 参数：
//   - err: 要报告的错误
func (l *localNotifier) reportError(err error) {
	if l.errorHandler != nil {
		l.errorHandler(err)
	}
}
