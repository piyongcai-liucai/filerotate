// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件实现标准版 Writer，通过 Leader 选举和进程间通知协调多进程文件轮转。
package filerotate

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// errFileNotOpen 表示文件未打开时尝试写入。
var errFileNotOpen = errors.New("filerotate: file not open")

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
	// 若工厂返回错误，Writer 创建将失败。
	LockerFactory func(lockPath string) (Locker, error)

	// NotifierFactory 自定义通知器工厂函数。若为 nil，使用基于文件轮询的默认通知器。
	// 跨主机场景（NFS + IPC）时可注入 NATS / JetStream / Valkey 等通知器。
	NotifierFactory func(errorHandler func(error)) (Notifier, error)

	// ErrorHandler 当内部 goroutine 发生错误时调用。如果为 nil，错误将打印到 stderr。
	ErrorHandler func(error)
}

// Writer 是多进程安全的文件写入器。
//
// 标准模式：通过 Leader 选举和进程间通知协调所有进程。
// 所有进程平等竞争 Leader，Leader 负责监控文件大小并广播轮转命令。
//
// 单机（Local）模式：无 Leader 选举，无进程间 IPC。每个进程通过内置的
// polling goroutine 独立检查文件状态，通过分布式锁协调轮转操作。
//
// Writer 实现了 io.WriteCloser，可直接与 log.Logger、fmt.Fprint 等配合使用。
type Writer struct {
	mu             sync.Mutex    // 保护 doRotation 文件替换和 Close，两种模式共用
	file           *os.File      // 当前文件句柄，使用 O_APPEND 模式保证多进程安全写入
	filePath       string        // 文件路径，所有进程必须相同
	lockPath       string        // 轮转锁文件路径
	leaderLockPath string        // Leader 选举锁路径，用于竞选 Leader
	maxSize        int64         // 轮转大小阈值（字节），由 MaxSizeMB 转换而来
	maxAgeDays     int           // 备份保留天数，轮转时自动清理过期备份
	checkInterval  time.Duration // Leader 检查文件大小的间隔
	notifier       Notifier      // 进程间通知器，Leader 通过它广播命令
	leaderLocker   Locker        // Leader 选举锁，用于竞选 Leader
	rotateCh       chan struct{} // 接收轮转通知的通道，收到信号后重开文件
	errorHandler   func(error)   // 错误处理回调，用于异步报告内部错误

	local        bool   // 是否为单机（Local）模式（无 Leader 选举，无 IPC）
	rotateLocker Locker // 单机模式轮转分布式锁，多进程协调

	lastLeaderFileInfo os.FileInfo // Leader 用于检测外部轮转（inode 变化），仅 ticker goroutine 访问

	done      chan struct{}  // 关闭时通知所有后台 goroutine 退出
	wg        sync.WaitGroup // 等待后台 goroutine 完全退出
	closeOnce sync.Once      // 确保 Close 只执行一次
}

// New 创建一个 Writer。
//
// 根据 NotifierFactory 自动选择模式：
//   - NotifierFactory == nil：单机模式，使用内置 LocalNotifier 轮询 + 分布式锁协调多进程。
//     所有进程平等，无 Leader 选举。
//   - NotifierFactory != nil：标准模式，使用自定义 IPC 通知器 + Leader 选举协调多进程。
func New(cfg Config) (*Writer, error) {
	if cfg.ErrorHandler == nil {
		cfg.ErrorHandler = func(err error) {
			fmt.Fprintf(os.Stderr, "filerotate: %v\n", err)
		}
	}

	if cfg.NotifierFactory == nil {
		return newLocal(cfg)
	}
	if cfg.LockerFactory == nil {
		return nil, errors.New("标准模式必须设置 LockerFactory，跨主机请使用 NewValkeyLocker")
	}
	return newStandard(cfg)
}


// newStandard 创建标准模式 Writer（IPC 通知 + Leader 选举）。
func newStandard(cfg Config) (*Writer, error) {
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = 5 * time.Second
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

	if err := os.MkdirAll(filepath.Dir(cfg.FilePath), 0o755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	var err error
	w.file, err = openFileAppend(w.filePath)
	if err != nil {
		return nil, err
	}

	w.leaderLocker, err = cfg.LockerFactory(w.leaderLockPath)
	if err != nil {
		w.file.Close()
		return nil, fmt.Errorf("create leader locker: %w", err)
	}

	w.rotateLocker, err = cfg.LockerFactory(w.lockPath)
	if err != nil {
		w.file.Close()
		w.leaderLocker.Unlock()
		return nil, fmt.Errorf("create rotate locker: %w", err)
	}

	var notifierErr error
	w.notifier, notifierErr = cfg.NotifierFactory(cfg.ErrorHandler)
	if notifierErr != nil {
		w.file.Close()
		w.leaderLocker.Unlock()
		w.rotateLocker.Unlock()
		return nil, fmt.Errorf("create notifier: %w", notifierErr)
	}

	if err := w.notifier.Serve(); err != nil {
		w.file.Close()
		w.leaderLocker.Unlock()
		w.rotateLocker.Unlock()
		return nil, fmt.Errorf("serve notifier: %w", err)
	}

	if w.tryBecomeLeader() {
		w.wg.Add(1)
		go w.runLeader()
	} else {
		w.wg.Add(1)
		go w.connectToLeader()
	}
	return w, nil
}

// Write 实现 io.Writer。写入前检查是否有轮转信号，若有则先处理轮转。
// Write 本身不加锁，仅在轮转操作中加锁（标准模式用 mu，单机模式用分布式锁）。
// 若本轮写入期间发生轮转，数据进入备份文件而非新文件，但数据不会丢失，
// 下次写入时自动重开文件。
func (w *Writer) Write(p []byte) (n int, err error) {
	// 写入前检查轮转信号：无论 Standard/Local，统一重开文件。
	// 轮转由 Leader 在 ticker 中执行，Write() 只负责在新文件上继续写入。
	select {
	case <-w.rotateCh:
		if err := w.reopenFile(); err != nil {
			w.reportError(err)
			w.requeueRotate()
		} else {
			w.resetNotifier()
		}
	default:
	}

	f := w.file
	if f == nil {
		return 0, errFileNotOpen
	}
	return f.Write(p)
}

// resetNotifier 通知 notifier 本轮处理已完成，可继续检测。
func (w *Writer) resetNotifier() {
	if r, ok := w.notifier.(interface{ Reset() }); ok {
		r.Reset()
	}
}

// requeueRotate 将轮转信号放回 rotateCh，用于轮转失败时的重试。
func (w *Writer) requeueRotate() {
	select {
	case w.rotateCh <- struct{}{}:
	default:
	}
}

// Close 释放资源，关闭文件、通知器和锁。
func (w *Writer) Close() error {
	var err error
	w.closeOnce.Do(func() {
		close(w.done)

		if w.notifier != nil {
			w.notifier.Close()
		}

		// 单机模式：LocalNotifier 的 Serve 在 Close 时能正常退出，Wait 安全。
		// 标准模式：IPC notifier 的 Serve 可能仍在 Accept 上阻塞，不 Wait。
		if w.local {
			w.wg.Wait()
		}
		if w.rotateLocker != nil {
			w.rotateLocker.Unlock()
		}

		w.mu.Lock()
		if w.file != nil {
			err = w.file.Close()
		}
		w.mu.Unlock()
	})
	return err
}

func (w *Writer) reportError(err error) {
	if w.errorHandler != nil {
		w.errorHandler(err)
	}
}

func (w *Writer) broadcastRotate() {
	if err := w.notifier.Broadcast(CmdRotate); err != nil {
		w.reportError(fmt.Errorf("广播 ROTATE 失败: %w", err))
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

// connectToLeader 客户端主循环：监听来自 notifier 的命令，并在 Leader 退出后竞选接管。
func (w *Writer) connectToLeader() {
	defer w.wg.Done()

	cmdCh, err := w.notifier.Connect()
	if err != nil {
		w.reportError(fmt.Errorf("客户端连接失败: %w", err))
		return
	}
	w.handleCommands(cmdCh)

	// 通道关闭（notifier 正在关闭）→ 尝试竞选新 Leader
	select {
	case <-w.done:
		return
	case <-time.After(500 * time.Millisecond):
	}

	if w.tryBecomeLeader() {
		w.wg.Add(1)
		go w.runLeader()
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

// doRotation 执行文件轮转：重命名、重开。
// 即使 doFileRotation 部分失败也会尝试 reopenFile——openFileAppend 的 O_CREATE 会自动创建文件。
func (w *Writer) doRotation() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := doFileRotation(w.filePath, w.maxAgeDays); err != nil {
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
