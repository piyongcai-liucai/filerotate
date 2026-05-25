// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件实现 Lite 版 Writer，极简设计，无进程间通信，仅依赖分布式锁和本地计数。
package filerotate

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LiteConfig Lite 版配置。
//
// Lite 版不需要 Notifier，所有进程通过分布式锁协调轮转。
// 适用于进程数较少、写入速率均匀的场景。
type LiteConfig struct {
	// FilePath 文件路径。所有进程使用相同路径。
	FilePath string

	// PerProcSizeMB 每个进程写入多少 MB 后触发轮转。
	// 实际文件大小 ≈ PerProcSizeMB × 进程数。
	// 例如 4 个进程，希望总文件约 100 MB 时轮转，则设为 25。
	PerProcSizeMB int

	// MaxAgeDays 备份保留天数，0 为永久。
	MaxAgeDays int

	// MaxWriteInterval 最大写入间隔，超过时检查文件是否被轮转，默认 5 秒。
	// 用于解决慢进程长期不触发轮转的问题。
	MaxWriteInterval time.Duration

	// LockerFactory 自定义锁工厂函数。若为 nil，使用默认文件锁。
	LockerFactory func(lockPath string) (Locker, error)
}

// LiteWriter 轻量版多进程安全文件写入器。
//
// 不依赖进程间通信，仅通过分布式锁和本地计数实现轮转协调。
// 跨平台，适用于进程数较少、写入速率均匀的场景。
//
// 每个进程独立维护本地写入计数 localWritten。
// 当 localWritten 达到 perProcSize 时，尝试通过文件锁选举执行轮转。
//
// 为了解决慢进程（写入很少的进程）长期不触发轮转的问题，
// 引入了时间间隔检查：如果距离上次写入时间超过 MaxWriteInterval，
// 会自动检查文件是否已被其他进程轮转。
type LiteWriter struct {
	// mu 保护并发写入和内部状态
	mu sync.Mutex

	// file 当前文件句柄
	file *os.File

	// filePath 文件路径
	filePath string

	// perProcSize 每个进程的轮转阈值（字节）
	perProcSize int64

	// maxAgeDays 备份保留天数
	maxAgeDays int

	// maxWriteInterval 最大写入间隔，用于感知外部轮转
	maxWriteInterval time.Duration

	// rotateLocker 轮转锁，用于选举唯一轮转执行者
	rotateLocker Locker

	// localWritten 本进程累计写入字节数（自上次轮转后）
	localWritten int64

	// lastWriteTime 上次写入时间，用于时间间隔检查
	lastWriteTime time.Time
}

// NewLiteWriter 创建 LiteWriter 并打开文件。
//
// 如果日志文件所在目录不存在，会自动创建。
//
// 参数:
//   - cfg: Lite 版配置
//
// 返回:
//   - *LiteWriter: LiteWriter 实例
//   - error: 错误信息
func NewLiteWriter(cfg LiteConfig) (*LiteWriter, error) {
	// 设置默认值
	if cfg.MaxWriteInterval == 0 {
		cfg.MaxWriteInterval = 5 * time.Second
	}
	if cfg.LockerFactory == nil {
		cfg.LockerFactory = func(p string) (Locker, error) { return NewFileLocker(p), nil }
	}

	// 确保日志文件和锁文件的目录存在
	if err := os.MkdirAll(filepath.Dir(cfg.FilePath), 0o755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	// 创建轮转锁
	// 锁文件路径为 <FilePath>.lock，所有进程使用相同的锁路径
	locker, err := cfg.LockerFactory(cfg.FilePath + ".lock")
	if err != nil {
		return nil, fmt.Errorf("create locker: %w", err)
	}

	w := &LiteWriter{
		filePath:         cfg.FilePath,
		perProcSize:      int64(cfg.PerProcSizeMB) * 1024 * 1024,
		maxAgeDays:       cfg.MaxAgeDays,
		maxWriteInterval: cfg.MaxWriteInterval,
		rotateLocker:     locker,
		lastWriteTime:    time.Now(),
	}

	// 打开日志文件
	w.file, err = openFileAppend(w.filePath)
	if err != nil {
		return nil, err
	}
	return w, nil
}

// Write 实现 io.Writer。触发条件：
//  1. 本地写入量达到 perProcSize
//  2. 距离上次写入时间超过 MaxWriteInterval（可能已被其他进程轮转）
//
// 写入是线程安全的。如果文件已被其他进程轮转（通过文件锁检测），
// 本进程会自动重开新文件并继续写入。
//
// 参数:
//   - p: 要写入的数据
//
// 返回:
//   - n: 实际写入的字节数
//   - err: 错误信息
func (w *LiteWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return 0, fmt.Errorf("filerotate: file not open")
	}

	// 时间间隔检查：如果很久没写，文件可能已被轮转，需要检查
	if time.Since(w.lastWriteTime) > w.maxWriteInterval {
		if err := w.rotateIfNeeded(); err != nil {
			return 0, err
		}
	}

	// 大小阈值检查
	if w.localWritten >= w.perProcSize {
		if err := w.rotateIfNeeded(); err != nil {
			return 0, err
		}
	}

	n, err = w.file.Write(p)
	w.localWritten += int64(n)
	w.lastWriteTime = time.Now()
	return n, err
}

// Close 关闭文件并释放锁资源。
//
// 返回:
//   - error: 关闭失败的错误
func (w *LiteWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.rotateLocker != nil {
		w.rotateLocker.Unlock()
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// rotateIfNeeded 通过分布式锁选举唯一轮转执行者。
//
// 流程:
//  1. 尝试获取文件锁（非阻塞）
//  2. 若未获取到锁，等待其他进程完成轮转
//  3. 若获取到锁，二次确认文件是否已被轮转
//  4. 若需要轮转，执行重命名和创建新文件
//
// 返回:
//   - error: 轮转过程中的错误
func (w *LiteWriter) rotateIfNeeded() error {
	// 尝试获取轮转锁（非阻塞）
	ok, err := w.rotateLocker.TryLock()
	if err != nil {
		return fmt.Errorf("try lock: %w", err)
	}
	if !ok {
		// 锁被占用，说明其他进程正在轮转
		// 等待轮转完成并重开新文件
		return w.waitAndReopen()
	}
	defer w.rotateLocker.Unlock()

	// 二次确认：可能在我们等待锁时，文件已被其他进程轮转
	fi, err := os.Stat(w.filePath)
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	// 如果文件很小（远小于阈值），说明已被轮转过，只需重开新文件
	if fi.Size() < w.perProcSize/2 {
		return w.reopenFile()
	}

	// 确实需要轮转，执行重命名+创建
	return w.doRotation()
}

// doRotation 执行实际的轮转操作：重命名 → 创建新文件 → 清理备份 → 重开。
//
// 返回:
//   - error: 轮转失败的错误
func (w *LiteWriter) doRotation() error {
	if w.file != nil {
		w.file.Close()
	}
	if err := doFileRotation(w.filePath, w.maxAgeDays); err != nil {
		return err
	}
	return w.reopenFile()
}

// reopenFile 关闭当前文件句柄，重新打开文件，并重置计数。
//
// 返回:
//   - error: 打开文件失败的错误
func (w *LiteWriter) reopenFile() error {
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
	w.localWritten = 0
	w.lastWriteTime = time.Now()
	return nil
}

// waitAndReopen 等待其他进程完成轮转（文件变小），然后重开新文件。
//
// 通过轮询文件大小检测轮转是否完成，最多等待 5 秒。
//
// 返回:
//   - error: 超时或其他错误
func (w *LiteWriter) waitAndReopen() error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		fi, err := os.Stat(w.filePath)
		if err == nil && fi.Size() < w.perProcSize/2 {
			return w.reopenFile()
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("filerotate: timeout waiting for rotation")
}
