// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件实现标准版 Writer，通过 Leader 选举和进程间通知协调多进程文件轮转。
package filerotate

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Config 标准版 Writer 配置。
type Config struct {
	// FilePath 文件路径。所有并发进程必须使用相同的路径。
	FilePath string

	// MaxSizeMB 触发轮转的总文件大小（MB）。Leader 定期检查文件实际大小。
	MaxSizeMB int

	// MaxAgeDays 备份文件保留天数。0 表示永不删除。轮转时自动清理过期备份。
	MaxAgeDays int

	// CheckInterval Leader 检查文件大小的间隔。默认 5 秒。
	CheckInterval time.Duration

	// LockerFactory 自定义锁工厂函数。若为 nil，使用默认文件锁。
	LockerFactory func(lockPath string) (Locker, error)

	// NotifierFactory 自定义通知器工厂函数。
	// 若为 nil，使用平台默认本地 IPC（Unix Socket 或 Windows 命名管道）。
	NotifierFactory func(commPath string, errorHandler func(error)) (Notifier, error)

	// ErrorHandler 当内部 goroutine 发生错误时调用。如果为 nil，错误将打印到 stderr。
	ErrorHandler func(error)
}

// Writer 是多进程安全的文件写入器（标准版）。
//
// 内部通过 Leader 选举和进程间通知协调所有进程。
// 所有进程平等竞争 Leader，Leader 负责监控文件大小并广播轮转命令。
//
// Writer 实现了 io.WriteCloser，可直接与 log.Logger、fmt.Fprint 等配合使用。
type Writer struct {
	mu             sync.Mutex    // 保护并发写入和内部状态
	file           *os.File      // 当前文件句柄，使用 O_APPEND 模式保证多进程安全写入
	filePath       string        // 文件路径，所有进程必须相同
	lockPath       string        // 轮转锁文件路径（Lite 版使用，标准版保留用于兼容）
	leaderLockPath string        // Leader 选举锁路径，用于竞选 Leader
	maxSize        int64         // 轮转大小阈值（字节），由 MaxSizeMB 转换而来
	maxAgeDays     int           // 备份保留天数，轮转时自动清理过期备份
	checkInterval  time.Duration // Leader 检查文件大小的间隔
	notifier       Notifier      // 进程间通知器，Leader 通过它广播命令
	leaderLocker   Locker        // Leader 选举锁，用于竞选 Leader
	rotateCh       chan struct{} // 接收轮转通知的通道，收到信号后重开文件
	errorHandler   func(error)   // 错误处理回调，用于异步报告内部错误

	done      chan struct{} // 关闭时通知所有后台 goroutine 退出
	wg        sync.WaitGroup // 等待后台 goroutine 完全退出
	closeOnce sync.Once     // 确保 Close 只执行一次
}

// New 创建一个标准版 Writer，自动加入协调组。
func New(cfg Config) (*Writer, error) {
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = 5 * time.Second
	}
	if cfg.LockerFactory == nil {
		cfg.LockerFactory = func(p string) (Locker, error) { return NewFileLocker(p) }
	}
	if cfg.ErrorHandler == nil {
		cfg.ErrorHandler = func(err error) {
			fmt.Fprintf(os.Stderr, "filerotate: %v\n", err)
		}
	}

	w := &Writer{
		filePath:       cfg.FilePath,
		lockPath:       cfg.FilePath + ".lock",
		leaderLockPath: cfg.FilePath + ".leader.lock",
		maxSize:        int64(cfg.MaxSizeMB) * 1024 * 1024,
		maxAgeDays:     cfg.MaxAgeDays,
		checkInterval:  cfg.CheckInterval,
		rotateCh:       make(chan struct{}, 1),
		errorHandler:   cfg.ErrorHandler,
		done:           make(chan struct{}),
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

	// 创建 Leader 选举锁
	w.leaderLocker, err = cfg.LockerFactory(w.leaderLockPath)
	if err != nil {
		return nil, fmt.Errorf("create leader locker: %w", err)
	}

	// 使用文件路径的哈希值生成唯一的命名管道名称（Windows）或 Socket 文件名（Unix）
	// 确保同一日志文件的所有进程使用相同的通信标识，不同日志文件使用不同的标识
	hash := sha256.Sum256([]byte(w.filePath))
	commPath := fmt.Sprintf("filerotate_%x", hash[:8])

	// 创建通知器
	if cfg.NotifierFactory != nil {
		w.notifier, err = cfg.NotifierFactory(commPath, cfg.ErrorHandler)
	} else {
		w.notifier = NewLocalNotifier(commPath, cfg.ErrorHandler)
	}
	if err != nil {
		return nil, fmt.Errorf("create notifier: %w", err)
	}

	// 尝试成为 Leader，否则作为客户端连接现有 Leader
	if w.tryBecomeLeader() {
		w.wg.Add(1)
		go w.runLeader()
	} else {
		w.wg.Add(1)
		go w.connectToLeader()
	}
	return w, nil
}

// Write 实现 io.Writer。写入前检查是否有轮转信号，若有则先重开文件。
// 若本轮写入期间发生轮转，数据进入备份文件而非新文件，但数据不会丢失，
// 下次写入时自动重开文件。
func (w *Writer) Write(p []byte) (n int, err error) {
	w.mu.Lock()

	if w.file == nil {
		w.mu.Unlock()
		return 0, errFileNotOpen
	}

	// 写入前检查轮转信号
	select {
	case <-w.rotateCh:
		if err := w.reopenFile(); err != nil {
			w.mu.Unlock()
			return 0, err
		}
	default:
	}

	n, err = w.file.Write(p)
	w.mu.Unlock()
	return n, err
}

// Close 释放资源，关闭文件、通知器和 Leader 锁。
// 不等待后台 goroutine 完全退出，因为 Windows 命名管道 I/O 无法被可靠中断。
func (w *Writer) Close() error {
	var err error
	w.closeOnce.Do(func() {
		close(w.done)

		if w.notifier != nil {
			w.notifier.Close()
		}

		w.mu.Lock()
		defer w.mu.Unlock()

		if w.leaderLocker != nil {
			w.leaderLocker.Unlock()
		}
		if w.file != nil {
			err = w.file.Close()
		}
	})
	return err
}

func (w *Writer) reportError(err error) {
	if w.errorHandler != nil {
		w.errorHandler(err)
	}
}

func (w *Writer) tryBecomeLeader() bool {
	ok, err := w.leaderLocker.TryLock()
	if err != nil {
		w.reportError(fmt.Errorf("尝试获取Leader锁失败: %w", err))
		return false
	}
	return ok
}

// connectToLeader 客户端主循环：连接 Leader，监听命令，断线后自动重试或竞选 Leader。
//
// 当与 Leader 的连接断开时（例如 Leader 崩溃），客户端会尝试重新连接。
// 如果连接失败，客户端会尝试成为新 Leader。
// 注意：在尝试成为 Leader 之前，必须释放可能持有的旧锁，防止文件锁重入导致反复失败。
func (w *Writer) connectToLeader() {
	defer w.wg.Done()

	for {
		select {
		case <-w.done:
			return
		default:
		}

		cmdCh, err := w.notifier.Connect()
		if err != nil {
			w.reportError(fmt.Errorf("客户端连接Leader失败: %w", err))

			select {
			case <-w.done:
				return
			case <-time.After(1 * time.Second):
			}

			// 尝试成为 Leader（可能原 Leader 已退出）
			// 先释放可能持有的锁，避免因为本进程曾是 Leader 导致 tryBecomeLeader 立即成功但 Serve 仍失败的死循环
			w.leaderLocker.Unlock()
			if w.tryBecomeLeader() {
				w.wg.Add(1)
				go w.runLeader()
				return
			}
			continue
		}
		w.handleCommands(cmdCh)

		select {
		case <-w.done:
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (w *Writer) handleCommands(ch <-chan string) {
	for cmd := range ch {
		if cmd == CmdRotate {
			select {
			case w.rotateCh <- struct{}{}:
			default:
			}
		}
	}
}

// doRotation 执行文件轮转：关闭当前文件、重命名为备份、创建新文件、重开。
// 即使 doFileRotation 部分失败（如 rename 后 create 失败），也会尝试 reopenFile，
// 因为 openFileAppend 使用 O_CREATE 会自动创建不存在的文件。
func (w *Writer) doRotation() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		w.file.Close()
		w.file = nil
	}
	if err := doFileRotation(w.filePath, w.maxAgeDays); err != nil {
		w.reportError(fmt.Errorf("轮转失败: %w", err))
		// 即使轮转部分失败也尝试重开文件，避免文件句柄为空
		if reopenErr := w.reopenFile(); reopenErr != nil {
			return reopenErr
		}
		return err
	}
	return w.reopenFile()
}

func (w *Writer) reopenFile() error {
	f, err := openFileAppend(w.filePath)
	if err != nil {
		return err
	}
	oldFile := w.file
	w.file = f
	if oldFile != nil {
		oldFile.Close()
	}
	return nil
}
