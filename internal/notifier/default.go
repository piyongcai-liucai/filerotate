// Package notifier 提供进程间轮转通知的内部实现。
//
// 本文件实现 DefaultNotifier：基于 goroutine 轮询的本地通知器，
// 每隔固定时间检查文件是否需要轮转（大小、inode 变化）。
package notifier

import (
	"os"
	"sync"
	"time"
)

// DefaultNotifier 基于本地轮询的通知器，无需进程间通信。
type DefaultNotifier struct {
	ch   chan string
	done chan struct{}
	wg   sync.WaitGroup

	filePath string
	maxSize  *int64
	interval time.Duration

	mu           sync.Mutex
	lastFileInfo os.FileInfo
	signalSent   bool // 已发送 ROTATE，在 Write 处理完成前不再重复发送

	errorHandler func(error)
}

// NewDefault 创建一个基于轮询的默认通知器。
// interval 为轮询间隔，maxSize 为触发轮转的文件大小阈值（字节），通过指针与调用方共享。
func NewDefault(filePath string, maxSize *int64, interval time.Duration, errorHandler func(error)) *DefaultNotifier {
	return &DefaultNotifier{
		ch:           make(chan string, 1),
		done:         make(chan struct{}),
		filePath:     filePath,
		maxSize:      maxSize,
		interval:     interval,
		errorHandler: errorHandler,
	}
}

// Serve 启动轮询 goroutine。
func (n *DefaultNotifier) Serve() error {
	fi, err := os.Stat(n.filePath)
	if err == nil {
		n.lastFileInfo = fi
	}

	n.wg.Add(1)
	go n.poll()
	return nil
}

func (n *DefaultNotifier) poll() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.interval)
	defer ticker.Stop()

	for {
		select {
		case <-n.done:
			return
		case <-ticker.C:
			n.checkAndSignal()
		}
	}
}

func (n *DefaultNotifier) checkAndSignal() {
	n.mu.Lock()
	defer n.mu.Unlock()

	fi, err := os.Stat(n.filePath)
	if err != nil {
		return
	}

	// 若文件已被轮转（inode 变化），说明上次信号已被处理，恢复检测
	if n.signalSent && n.lastFileInfo != nil && !os.SameFile(n.lastFileInfo, fi) {
		n.signalSent = false
	}

	n.lastFileInfo = fi

	// 已发送过信号且尚未被处理，不再重复发送
	if n.signalSent {
		return
	}

	// 检查是否需要轮转
	if *n.maxSize > 0 && fi.Size() >= *n.maxSize {
		select {
		case n.ch <- "ROTATE":
			n.signalSent = true
		default:
		}
	}
}

// Reset 重置信号状态，Write 完成轮转处理后调用，允许 notifier 继续检测。
func (n *DefaultNotifier) Reset() {
	n.mu.Lock()
	n.signalSent = false
	n.mu.Unlock()
}

// Connect 返回接收命令的通道。
func (n *DefaultNotifier) Connect() (<-chan string, error) {
	return n.ch, nil
}

// Broadcast 向本地通道发送命令。
func (n *DefaultNotifier) Broadcast(cmd string) error {
	select {
	case n.ch <- cmd:
	default:
	}
	return nil
}

// Close 停止轮询 goroutine 并关闭命令通道。
func (n *DefaultNotifier) Close() error {
	close(n.done)
	n.wg.Wait()
	close(n.ch)
	return nil
}
