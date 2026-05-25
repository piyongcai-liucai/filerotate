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
//
// 标准版通过 Leader 选举和 Notifier 通知机制协调所有进程。
// 适用于进程数多、写入速率差异大的场景。
type Config struct {
	// FilePath 文件路径。所有并发进程必须使用相同的路径。
	FilePath string

	// MaxSizeMB 触发轮转的总文件大小（MB）。Leader 定期检查文件实际大小。
	MaxSizeMB int

	// MaxAgeDays 备份文件保留天数。0 表示永不删除。轮转时自动清理过期备份。
	MaxAgeDays int

	// CheckInterval Leader 检查文件大小的间隔。默认 5 秒。
	// 间隔越短，轮转越及时，但系统调用频率越高。
	CheckInterval time.Duration

	// LockerFactory 自定义锁工厂函数。若为 nil，使用默认文件锁。
	// 锁用于 Leader 选举，保证同一时间只有一个进程成为 Leader。
	LockerFactory func(lockPath string) (Locker, error)

	// NotifierFactory 自定义通知器工厂函数。
	// 若为 nil，使用平台默认本地 IPC（Unix Socket 或 Windows 命名管道）。
	// 可通过此函数注入 NATS、JetStream、Valkey 等外部消息系统。
	NotifierFactory func(commPath string, errorHandler func(error)) (Notifier, error)

	// ErrorHandler 当内部 goroutine 发生错误时调用。如果为 nil，错误将打印到 stderr。
	// 用于异步报告 Leader、客户端连接、轮转等操作中的错误。
	ErrorHandler func(error)
}

// Writer 是多进程安全的文件写入器（标准版）。
//
// 内部通过 Leader 选举和进程间通知协调所有进程。
// 所有进程平等竞争 Leader，Leader 负责监控文件大小并广播轮转命令。
//
// Writer 实现了 io.WriteCloser，可直接与 log.Logger、fmt.Fprint 等配合使用。
//
// 生命周期：
//  1. 创建 Writer 时，自动打开日志文件
//  2. 尝试成为 Leader 或连接现有 Leader
//  3. Leader 定期检查文件大小，超限时执行轮转并广播命令
//  4. 所有进程收到命令后重开文件
//  5. 调用 Close() 释放资源
type Writer struct {
	// mu 保护并发写入和内部状态
	mu sync.Mutex

	// file 当前文件句柄，使用 O_APPEND 模式保证多进程安全写入
	file *os.File

	// filePath 文件路径，所有进程必须相同
	filePath string

	// lockPath 轮转锁文件路径（Lite 版使用，标准版保留用于兼容）
	lockPath string

	// leaderLockPath Leader 选举锁路径，用于竞选 Leader
	leaderLockPath string

	// maxSize 轮转大小阈值（字节），由 MaxSizeMB 转换而来
	maxSize int64

	// maxAgeDays 备份保留天数，轮转时自动清理过期备份
	maxAgeDays int

	// checkInterval Leader 检查文件大小的间隔
	checkInterval time.Duration

	// notifier 进程间通知器，Leader 通过它广播命令
	notifier Notifier

	// leaderLocker Leader 选举锁，用于竞选 Leader
	leaderLocker Locker

	// rotateCh 接收轮转通知的通道，收到信号后重开文件
	rotateCh chan struct{}

	// errorHandler 错误处理回调，用于异步报告内部错误
	errorHandler func(error)
}

// New 创建一个标准版 Writer，自动加入协调组。
//
// 进程启动后会自动尝试成为 Leader 或连接到现有 Leader。
// 如果 NotifierFactory 为 nil，使用平台默认本地 IPC。
// 如果 LockerFactory 为 nil，使用默认文件锁。
// 如果日志文件所在目录不存在，会自动创建。
//
// 参数:
//   - cfg: 标准版配置
//
// 返回:
//   - *Writer: Writer 实例
//   - error: 创建失败的错误
func New(cfg Config) (*Writer, error) {
	// 设置默认值
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = 5 * time.Second
	}
	if cfg.LockerFactory == nil {
		cfg.LockerFactory = func(p string) (Locker, error) { return NewFileLocker(p), nil }
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
	}

	// 确保日志文件和锁文件的目录存在
	if err := os.MkdirAll(filepath.Dir(cfg.FilePath), 0o755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	// 打开日志文件，使用 O_APPEND 模式保证多进程安全写入
	var err error
	w.file, err = openFileAppend(w.filePath)
	if err != nil {
		return nil, err
	}

	// 创建 Leader 选举锁
	// 所有进程使用相同的锁路径（<FilePath>.leader.lock）竞选 Leader
	w.leaderLocker, err = cfg.LockerFactory(w.leaderLockPath)
	if err != nil {
		return nil, fmt.Errorf("create leader locker: %w", err)
	}

	// 使用文件路径的哈希值生成唯一的命名管道名称
	// 避免多实例在同一台机器上运行时管道名称冲突
	hash := sha256.Sum256([]byte(w.filePath))
	commPath := fmt.Sprintf("filerotate_%x", hash[:8])

	// 创建通知器
	if cfg.NotifierFactory != nil {
		w.notifier, err = cfg.NotifierFactory(commPath, cfg.ErrorHandler)
	} else {
		// 使用平台默认的本地 IPC 通知器
		w.notifier = NewLocalNotifier(commPath, cfg.ErrorHandler)
	}
	if err != nil {
		return nil, fmt.Errorf("create notifier: %w", err)
	}

	// 尝试成为 Leader，否则作为客户端连接现有 Leader
	if w.tryBecomeLeader() {
		go w.runLeader()
	} else {
		go w.connectToLeader()
	}
	return w, nil
}

// Write 实现 io.Writer。写入前会检查是否有轮转信号，若有则先重开文件。
//
// 写入是线程安全的，多个 goroutine 可并发调用。
// 如果当前进程收到轮转信号（来自 Leader 的 ROTATE 命令），会先重开文件再写入。
//
// 参数:
//   - p: 要写入的数据
//
// 返回:
//   - n: 实际写入的字节数
//   - err: 错误信息
func (w *Writer) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return 0, fmt.Errorf("filerotate: file not open")
	}

	// 非阻塞检查轮转信号
	// 如果收到信号，先重开文件再写入
	select {
	case <-w.rotateCh:
		if err := w.reopenFile(); err != nil {
			return 0, err
		}
	default:
		// 没有轮转信号，直接写入
	}

	return w.file.Write(p)
}

// Close 释放资源，关闭文件、通知器和 Leader 锁。
//
// 调用 Close 后，Writer 不应再使用。
//
// 返回:
//   - error: 关闭失败的错误
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 释放 Leader 锁
	if w.leaderLocker != nil {
		w.leaderLocker.Unlock()
	}

	// 关闭通知器
	if w.notifier != nil {
		w.notifier.Close()
	}

	// 关闭日志文件
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// reportError 向 ErrorHandler 报告错误。
//
// 如果 ErrorHandler 为 nil，错误将被忽略。
//
// 参数:
//   - err: 要报告的错误
func (w *Writer) reportError(err error) {
	if w.errorHandler != nil {
		w.errorHandler(err)
	}
}

// tryBecomeLeader 尝试获取 Leader 锁。
//
// 使用非阻塞的 TryLock 避免多个进程同时阻塞在锁上。
//
// 返回:
//   - bool: 是否成功成为 Leader
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
func (w *Writer) connectToLeader() {
	for {
		cmdCh, err := w.notifier.Connect()
		if err != nil {
			w.reportError(fmt.Errorf("客户端连接Leader失败: %w", err))
			time.Sleep(1 * time.Second)

			// 尝试成为 Leader（可能原 Leader 已退出）
			if w.tryBecomeLeader() {
				go w.runLeader()
				return
			}
			continue
		}

		// 成功连接，处理命令直到连接断开
		w.handleCommands(cmdCh)
		time.Sleep(500 * time.Millisecond)
	}
}

// handleCommands 监听命令通道，收到 CmdRotate 时发送信号给 Write。
//
// 参数:
//   - ch: 命令接收通道
func (w *Writer) handleCommands(ch <-chan string) {
	for cmd := range ch {
		if cmd == CmdRotate {
			// 发送轮转信号给 Write 方法
			// 使用非阻塞发送，避免重复信号
			select {
			case w.rotateCh <- struct{}{}:
			default:
			}
		}
	}
}

// doRotation 执行实际轮转：重命名当前文件、创建新文件、清理旧备份。
//
// 仅由 Leader 调用，调用者应持有锁。
//
// 返回:
//   - error: 轮转失败的错误
func (w *Writer) doRotation() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 关闭当前文件句柄
	if w.file != nil {
		w.file.Close()
	}

	// 执行文件系统操作：重命名、创建新文件、清理旧备份
	if err := doFileRotation(w.filePath, w.maxAgeDays); err != nil {
		w.reportError(fmt.Errorf("轮转失败: %w", err))
		return err
	}

	// 重新打开新文件
	return w.reopenFile()
}

// reopenFile 关闭当前句柄，重新打开文件（用于切换到新文件）。
//
// 返回:
//   - error: 打开文件失败的错误
func (w *Writer) reopenFile() error {
	oldFile := w.file
	w.file = nil
	f, err := openFileAppend(w.filePath)
	if err != nil {
		return err
	}
	w.file = f
	if oldFile != nil {
		oldFile.Close()
	}
	return nil
}
