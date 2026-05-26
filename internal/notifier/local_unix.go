//go:build !windows

// Package notifier 提供进程间通知的内部实现。
//
// 本文件实现基于 Unix 域 Socket 的本地 IPC 通知器，用于 Linux/macOS 系统。
package notifier

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sync"
)

// LocalNotifier 基于 Unix 域 Socket 的本地 IPC 通知器实现。
//
// 在 Linux/macOS 下，Leader 监听 Unix Socket 文件，客户端通过文件路径连接。
// 所有进程运行在同一台机器上，通信效率高，无网络开销。
type LocalNotifier struct {
	socketPath string

	listener net.Listener

	clients map[net.Conn]struct{}

	mu sync.Mutex

	done chan struct{}

	closeOnce sync.Once

	errorHandler func(error)
}

// NewLocalNotifier 创建一个本地 IPC 通知器。
func NewLocalNotifier(commPath string, errorHandler func(error)) *LocalNotifier {
	if errorHandler == nil {
		errorHandler = func(err error) {
			fmt.Fprintf(os.Stderr, "[Local] 错误: %v\n", err)
		}
	}
	return &LocalNotifier{
		socketPath:   commPath,
		clients:      make(map[net.Conn]struct{}),
		done:         make(chan struct{}),
		errorHandler: errorHandler,
	}
}

// Serve 启动 Unix Socket 监听，接受客户端连接。
func (l *LocalNotifier) Serve() error {
	// 删除可能残留的旧 socket 文件，确保 Listen 成功
	os.Remove(l.socketPath)

	listener, err := net.Listen("unix", l.socketPath)
	if err != nil {
		l.reportError(fmt.Errorf("Unix Socket Listen 失败: %w", err))
		return err
	}
	l.listener = listener

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-l.done:
				return nil
			default:
				l.reportError(fmt.Errorf("Unix Socket Accept 失败: %w", err))
				continue
			}
		}

		l.mu.Lock()
		l.clients[conn] = struct{}{}
		l.mu.Unlock()

		go l.handleConn(conn)
	}
}

func (l *LocalNotifier) handleConn(conn net.Conn) {
	defer func() {
		l.mu.Lock()
		delete(l.clients, conn)
		l.mu.Unlock()
		conn.Close()
	}()

	buf := make([]byte, 1024)
	for {
		if _, err := conn.Read(buf); err != nil {
			return
		}
	}
}

// Connect 作为客户端连接到 Leader 的 Socket，返回命令接收通道。
func (l *LocalNotifier) Connect() (<-chan string, error) {
	conn, err := net.Dial("unix", l.socketPath)
	if err != nil {
		l.reportError(fmt.Errorf("Unix Socket Dial 失败: %w", err))
		return nil, err
	}

	ch := make(chan string, 5)
	go func() {
		defer close(ch)
		defer conn.Close()

		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			ch <- scanner.Text()
		}
	}()

	return ch, nil
}

// Broadcast 向所有已连接的客户端发送命令。
func (l *LocalNotifier) Broadcast(cmd string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for conn := range l.clients {
		_, err := fmt.Fprintln(conn, cmd)
		if err != nil {
			l.reportError(fmt.Errorf("Unix Socket 写入失败: %w", err))
			conn.Close()
			delete(l.clients, conn)
		}
	}
	return nil
}

// Close 关闭通知器，停止监听并释放资源。
// 可安全多次调用。
func (l *LocalNotifier) Close() error {
	l.closeOnce.Do(func() {
		close(l.done)
		l.mu.Lock()
		for conn := range l.clients {
			conn.Close()
		}
		l.mu.Unlock()
		if l.listener != nil {
			l.listener.Close()
		}
	})
	return nil
}

func (l *LocalNotifier) reportError(err error) {
	if l.errorHandler != nil {
		l.errorHandler(err)
	}
}
