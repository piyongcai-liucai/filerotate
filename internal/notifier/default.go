// Package notifier 提供进程间轮转通知的内部实现。
//
// 本文件实现 DefaultNotifier：基于 goroutine 轮询的本地通知器，
// 每隔固定时间检查文件是否需要轮转（大小、inode 变化）。
package notifier

import (
	"errors"
	"os"
	"sync"
	"time"
)

// ErrLeaderExists 表示 Leader 已存在，Serve() 因管道/Socket 被占用而无法启动。
// 这是正常竞态，非 Leader 进程应静默降级为客户端。
var ErrLeaderExists = errors.New("leader already exists")

// DefaultNotifier 基于本地轮询的通知器，无需进程间通信。
type DefaultNotifier struct {
	ch   chan string
	done chan struct{}
	wg   sync.WaitGroup

	filePath string
	maxSize  *int64 // 轮转大小阈值（字节），0 表示永不以大小触发
	interval time.Duration

	mu           sync.Mutex
	lastFileInfo os.FileInfo
	signalSent   bool // 已发送 ROTATE，在 Writer 处理完成前不再重复发送

	errorHandler func(error)
}

// NewDefault 创建一个基于轮询的默认通知器。
// interval 为轮询间隔，maxSize 为触发轮转的文件大小阈值（字节），0 表示永不以大小触发轮转。指针与调用方共享。
func NewDefault(filePath string, maxSize *int64, interval time.Duration, errorHandler func(error)) (*DefaultNotifier, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	return &DefaultNotifier{
		ch:           make(chan string, 1),
		done:         make(chan struct{}),
		filePath:     filePath,
		maxSize:      maxSize,
		interval:     interval,
		lastFileInfo: fi,
		errorHandler: errorHandler,
	}, nil
}

// Serve 启动轮询 goroutine。
func (n *DefaultNotifier) Serve() error {
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

	if n.signalSent {
		return
	}

	fi, err := os.Stat(n.filePath)
	if err != nil {
		n.reportError(err)
		return
	}

	// 文件被外部轮转（inode 不同）或大小超阈值 → 通知 writer 处理
	if !os.SameFile(n.lastFileInfo, fi) ||
		(*n.maxSize > 0 && fi.Size() >= *n.maxSize) {
		select {
		case n.ch <- "ROTATE":
			n.signalSent = true
		default:
		}
	}
	n.lastFileInfo = fi
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

// reportError 向错误处理器报告错误。
func (n *DefaultNotifier) reportError(err error) {
	if n.errorHandler != nil {
		n.errorHandler(err)
	}
}

// Close 停止轮询 goroutine 并关闭命令通道。
func (n *DefaultNotifier) Close() error {
	close(n.done)
	n.wg.Wait()
	close(n.ch)
	return nil
}
