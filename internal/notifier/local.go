// Package notifier 提供进程间轮转通知的内部实现。
//
// 本文件实现 LocalNotifier：基于 goroutine 轮询的本地通知器，
// 每隔固定时间检查文件是否需要轮转（大小、inode 变化）。
package notifier

import (
	"os"
	"sync"
	"time"
)

// LocalNotifier 基于文件轮询的本地通知器，用于单机多进程场景。
// 每个实例独立轮询文件状态（大小 + inode），发现变化时发送 ROTATE 到本地通道。
type LocalNotifier struct {
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

// NewLocal 创建一个基于文件轮询的本地通知器。
// maxSize 为触发轮转的文件大小阈值（字节），0 表示永不以大小触发。指针与调用方共享。
func NewLocal(filePath string, maxSize *int64, interval time.Duration, errorHandler func(error)) (*LocalNotifier, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	return &LocalNotifier{
		ch:           make(chan string, 1),
		done:         make(chan struct{}),
		filePath:     filePath,
		maxSize:      maxSize,
		interval:     interval,
		lastFileInfo: fi,
		errorHandler: errorHandler,
	}, nil
}

// Serve 启动轮询 goroutine。每个进程都启动自己的轮询。
func (n *LocalNotifier) Serve() error {
	n.wg.Add(1)
	go n.poll()
	return nil
}

func (n *LocalNotifier) poll() {
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

func (n *LocalNotifier) checkAndSignal() {
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

// UpdateFileInfo 更新缓存的文件信息。
// 自身轮转后调用，避免下次 poll 将自身轮转导致的 inode 变化误判为外部轮转。
func (n *LocalNotifier) UpdateFileInfo(fi os.FileInfo) {
	n.mu.Lock()
	n.lastFileInfo = fi
	n.mu.Unlock()
}

// Reset 重置信号状态，Writer 完成轮转处理后调用，允许 notifier 继续检测。
func (n *LocalNotifier) Reset() {
	n.mu.Lock()
	n.signalSent = false
	n.mu.Unlock()
}

// Connect 返回接收命令的通道。
func (n *LocalNotifier) Connect() (<-chan string, error) {
	return n.ch, nil
}

// Broadcast 向本地通道发送命令。
func (n *LocalNotifier) Broadcast(cmd string) error {
	select {
	case n.ch <- cmd:
	default:
	}
	return nil
}

// reportError 向错误处理器报告错误。
func (n *LocalNotifier) reportError(err error) {
	if n.errorHandler != nil {
		n.errorHandler(err)
	}
}

// Close 停止轮询 goroutine 并关闭命令通道。
func (n *LocalNotifier) Close() error {
	close(n.done)
	n.wg.Wait()
	close(n.ch)
	return nil
}
