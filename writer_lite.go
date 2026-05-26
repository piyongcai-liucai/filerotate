// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件实现 Lite 版 Writer，极简设计，无进程间通信，仅依赖分布式锁和本地计数。
package filerotate

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// errLockContention 表示轮转锁被其他进程持有，本进程应稍后重试。
var errLockContention = errors.New("lock contention")

// errFileNotOpen 表示文件未打开时尝试写入。
var errFileNotOpen = errors.New("filerotate: file not open")

// LiteConfig Lite 版配置。
//
// Lite 版不需要 Notifier，所有进程通过分布式锁协调轮转。
// 适用于进程数较少、写入速率均匀的场景。
type LiteConfig struct {
	// FilePath 文件路径。所有进程使用相同路径。
	FilePath string

	// PerProcSizeMB 每个进程写入多少 MB 后触发轮转。
	// 例如：预估 4 个进程，希望总文件约 100 MB 时轮转，则设为 25。
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
// 为了解决慢进程长期不触发轮转的问题，引入了时间间隔检查：
// 如果距离上次写入时间超过 MaxWriteInterval，会自动检查文件是否已被轮转。
//
// 轮转时的二次确认采用文件唯一标识（inode/文件索引）比较，
// 而非文件大小，避免高并发下误判。
type LiteWriter struct {
	mu               sync.Mutex
	file             *os.File
	filePath         string
	perProcSize      int64
	maxAgeDays       int
	maxWriteInterval time.Duration
	rotateLocker     Locker
	localWritten     int64
	lastWriteTime    time.Time   // 用于时间间隔检查
	initialFileInfo  os.FileInfo // 打开文件时的标识，用于判断文件是否被替换
}

// NewLiteWriter 创建 LiteWriter 并打开文件。
//
// 如果日志文件所在目录不存在，会自动创建。
func NewLiteWriter(cfg LiteConfig) (*LiteWriter, error) {
	if cfg.MaxWriteInterval == 0 {
		cfg.MaxWriteInterval = 5 * time.Second
	}
	if cfg.LockerFactory == nil {
		cfg.LockerFactory = func(p string) (Locker, error) { return NewFileLocker(p) }
	}

	// 确保日志文件和锁文件的目录存在
	if err := os.MkdirAll(filepath.Dir(cfg.FilePath), 0o755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	// 创建轮转锁
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

	// 记录初始文件标识，用于后续可靠判断文件是否已被轮转
	w.initialFileInfo, err = w.file.Stat()
	if err != nil {
		w.file.Close()
		return nil, err
	}

	return w, nil
}

// Write 实现 io.Writer。
//
// 优先写入数据，然后判断是否需要轮转检查。如果触发轮转但锁被占用，
// 不会阻塞等待，也不会重置检查时间，确保下一次写入能立即重试。
func (w *LiteWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()

	if w.file == nil {
		w.mu.Unlock()
		return 0, errFileNotOpen
	}

	// 先写入数据，保证日志不丢失且不被轮转锁阻塞
	n, err = w.file.Write(p)
	w.localWritten += int64(n)
	if err != nil {
		w.mu.Unlock()
		return n, err
	}

	// 判断是否需要触发轮转检查
	needCheck := w.localWritten >= w.perProcSize ||
		time.Since(w.lastWriteTime) > w.maxWriteInterval

	if needCheck {
		rotateErr := w.rotateIfNeeded()
		if rotateErr == errLockContention {
			w.mu.Unlock()
			return n, nil
		}
		if rotateErr != nil {
			w.mu.Unlock()
			return n, rotateErr
		}
		// 轮转成功或文件已被其他进程轮转，lastWriteTime 已在 reopenFile 中更新
	} else {
		w.lastWriteTime = time.Now()
	}

	w.mu.Unlock()
	return n, nil
}

// Close 关闭文件并释放锁。
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

// rotateIfNeeded 尝试获取轮转锁，并根据情况执行轮转或重开文件。
//
// 返回 errLockContention 表示锁竞争失败，调用者应稍后重试。
func (w *LiteWriter) rotateIfNeeded() error {
	// 非阻塞尝试获取锁
	ok, err := w.rotateLocker.TryLock()
	if err != nil {
		return fmt.Errorf("try lock: %w", err)
	}
	if !ok {
		// 锁被其他进程持有，不等待，直接返回特定错误
		return errLockContention
	}
	defer w.rotateLocker.Unlock()

	// 二次确认：通过文件标识判断是否已被其他进程轮转
	currentInfo, err := os.Stat(w.filePath)
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	// 如果文件标识不同（inode/索引变化），说明文件已被替换，只需重开文件
	if !os.SameFile(w.initialFileInfo, currentInfo) {
		return w.reopenFile()
	}

	// 还是旧文件，需要执行轮转
	return w.doRotation()
}

// doRotation 执行实际的轮转操作：重命名 → 创建新文件 → 清理备份 → 重开。
// 即使 doFileRotation 部分失败也会尝试 reopenFile，
// 因为 openFileAppend 使用 O_CREATE 会自动创建不存在的文件。
func (w *LiteWriter) doRotation() error {
	if w.file != nil {
		w.file.Close()
	}
	if err := doFileRotation(w.filePath, w.maxAgeDays); err != nil {
		// 即使轮转部分失败也尝试重开文件
		if reopenErr := w.reopenFile(); reopenErr != nil {
			return reopenErr
		}
		return err
	}
	return w.reopenFile()
}

// reopenFile 关闭当前文件句柄，重新打开文件，并更新初始文件标识，重置计数。
func (w *LiteWriter) reopenFile() error {
	oldFile := w.file
	w.file = nil
	f, err := openFileAppend(w.filePath)
	if err != nil {
		return err
	}
	w.file = f

	// 更新初始文件信息，以便后续判断文件是否被轮转
	newInfo, err := f.Stat()
	if err != nil {
		f.Close()
		w.file = nil
		return err
	}
	w.initialFileInfo = newInfo

	if oldFile != nil {
		oldFile.Close()
	}

	// 重置计数和时间戳
	w.localWritten = 0
	w.lastWriteTime = time.Now()
	return nil
}
