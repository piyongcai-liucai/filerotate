//go:build windows

// Package notifier 提供进程间通知的内部实现。
//
// 本文件实现基于 Windows 命名管道的本地 IPC 通知器，用于 Windows 系统。
package notifier

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"gopkg.in/natefinch/npipe.v2"
)

// LocalNotifier 基于 Windows 命名管道的本地 IPC 通知器实现。
type LocalNotifier struct {
	pipeName     string
	listener     net.Listener
	clients      map[net.Conn]struct{}
	mu           sync.Mutex
	done         chan struct{}
	closeOnce    sync.Once
	errorHandler func(error)
}

// NewLocalNotifier 创建一个本地 IPC 通知器。
func NewLocalNotifier(commPath string, errorHandler func(error)) *LocalNotifier {
	if errorHandler == nil {
		errorHandler = func(err error) {
			fmt.Fprintf(os.Stderr, "[Local] 错误: %v\n", err)
		}
	}

	pipeName := `\\.\pipe\` + commPath
	return &LocalNotifier{
		pipeName:     pipeName,
		clients:      make(map[net.Conn]struct{}),
		done:         make(chan struct{}),
		errorHandler: errorHandler,
	}
}

// Serve 启动命名管道监听，接受客户端连接。
// 如果遇到 Access is denied，会等待 100ms 后重试一次，以应对管道释放延迟。
func (l *LocalNotifier) Serve() error {
	var listener net.Listener
	var err error

	for retry := 0; retry < 2; retry++ {
		listener, err = npipe.Listen(l.pipeName)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "denied") || strings.Contains(err.Error(), "Access is denied") {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return err
	}
	if err != nil {
		return fmt.Errorf("%w: %w", ErrLeaderExists, err)
	}
	l.mu.Lock()
	l.listener = listener
	l.mu.Unlock()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-l.done:
				return nil
			default:
				l.reportError(fmt.Errorf("命名管道 Accept 失败: %w", err))
				continue
			}
		}
		l.mu.Lock()
		l.clients[conn] = struct{}{}
		l.mu.Unlock()
	}
}

// Connect 作为客户端连接到 Leader 的命名管道，设置 1 秒超时。
func (l *LocalNotifier) Connect() (<-chan string, error) {
	conn, err := npipe.DialTimeout(l.pipeName, time.Second)
	if err != nil {
		l.reportError(fmt.Errorf("命名管道 Dial 失败: %w", err))
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
		if err := scanner.Err(); err != nil {
			l.reportError(fmt.Errorf("命名管道 读取失败: %w", err))
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
			l.reportError(fmt.Errorf("命名管道 写入失败: %w", err))
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
