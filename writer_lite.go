// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件实现 Lite 模式 Writer 的构造函数，复用标准 Writer，
// 使用内置轮询通知器替代进程间通信。
package filerotate

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// LiteConfig Lite 模式配置。
//
// Lite 模式无需进程间 IPC，每个进程通过内置的 polling goroutine
// 独立检查文件大小和状态，通过分布式锁协调轮转。
type LiteConfig struct {
	// FilePath 文件路径。所有进程使用相同路径。
	FilePath string

	// MaxSizeMB 触发轮转的文件总大小（MB）。所有进程共享同一阈值。
	MaxSizeMB int

	// MaxAgeDays 备份保留天数，0 为永久。
	MaxAgeDays int

	// PollInterval 轮询间隔，默认 1 秒。
	PollInterval time.Duration

	// ErrorHandler 错误处理回调。若为 nil，默认打印到 stderr。
	ErrorHandler func(error)
}

// NewLite 创建一个 Lite 模式 Writer。
//
// Lite 模式复用标准 Writer 结构，使用内置轮询通知器替代 Leader 选举和 IPC。
// 每个进程独立检查文件状态（大小、inode 变化），通过分布式锁协调轮转操作。
func NewLite(cfg LiteConfig) (*Writer, error) {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 1 * time.Second
	}
	if cfg.ErrorHandler == nil {
		cfg.ErrorHandler = func(err error) {
			fmt.Fprintf(os.Stderr, "filerotate: %v\n", err)
		}
	}

	w := &Writer{
		filePath:      cfg.FilePath,
		lockPath:      cfg.FilePath + ".lock",
		maxSize:       int64(cfg.MaxSizeMB) * 1024 * 1024,
		maxAgeDays:    cfg.MaxAgeDays,
		checkInterval: cfg.PollInterval,
		rotateCh:      make(chan struct{}, 1),
		errorHandler:  cfg.ErrorHandler,
		lite:          true,
		done:          make(chan struct{}),
	}

	// 确保日志文件和锁文件的目录存在
	if err := os.MkdirAll(filepath.Dir(cfg.FilePath), 0o755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	// 打开日志文件
	var err error
	w.file, err = openFileAppend(w.filePath)
	if err != nil {
		return nil, err
	}

	// 创建轮转分布式锁
	w.rotateLocker, err = NewFileLocker(w.lockPath)
	if err != nil {
		w.file.Close()
		return nil, fmt.Errorf("create rotate locker: %w", err)
	}

	// 创建内置轮询通知器
	w.notifier = newDefaultNotifier(w.filePath, &w.maxSize, cfg.PollInterval, cfg.ErrorHandler)

	// 启动通知器轮询 goroutine
	go func() {
		if err := w.notifier.Serve(); err != nil {
			w.reportError(err)
		}
	}()

	// 等待通知器就绪后连接
	var cmdCh <-chan string
	for i := 0; i < 10; i++ {
		select {
		case <-w.done:
			return nil, fmt.Errorf("closed before ready")
		case <-time.After(50 * time.Millisecond):
		}
		cmdCh, err = w.notifier.Connect()
		if err == nil {
			break
		}
	}
	if err != nil {
		w.file.Close()
		w.rotateLocker.Unlock()
		return nil, fmt.Errorf("connect to notifier: %w", err)
	}

	// 启动命令处理协程，监听来自 Notifier 的轮转命令
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.handleCommands(cmdCh)
	}()

	return w, nil
}
