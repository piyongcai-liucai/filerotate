// Package filerotate 提供多进程安全的文件轮转功能。
package filerotate

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

// ErrFileNotOpen 表示文件未打开时尝试写入。
var ErrFileNotOpen = errors.New("filerotate: file not open")

// Config Writer 配置。
type Config struct {
	FilePath      string        // 文件路径，所有并发进程必须相同
	MaxSizeMB     int           // 触发轮转的文件大小（MB），0 表示永不轮转
	MaxAgeDays    int           // 备份文件保留天数，0 表示永不删除
	CheckInterval time.Duration // 文件检查间隔，默认 1s
	ErrorHandler  func(error)   // 内部 goroutine 错误回调，默认打印 stderr
}

// Writer 是多进程安全的文件写入器。
//
// 通过内置 polling goroutine 定期检查文件状态（大小 + inode），
// 超阈值时通过文件锁竞争轮转权。Write() 热路径不加锁，
// 依赖 O_APPEND 内核级原子追加。支持本地文件系统和 NFS。
type Writer struct {
	mu            sync.Mutex // 保护 doRotation 文件替换和 Close
	file          *os.File   // 当前文件句柄，O_APPEND 模式
	filePath      string
	lockPath      string
	maxSize       int64
	maxAgeDays    int
	checkInterval time.Duration
	rotateCh      chan struct{} // 通知 Write() 重开文件
	errorHandler  func(error)

	rotateLocker *flock.Flock // 文件锁，多进程协调

	pollMu       sync.Mutex  // 保护轮询状态
	lastFileInfo os.FileInfo // 上次 stat 结果，用于检测 inode 变化
	pollActive   bool        // 等待 Write() 处理当前轮转中

	done chan struct{}
	wg   sync.WaitGroup
}

// New 创建一个 Writer。
//
// 多进程安全：所有进程通过文件锁协调轮转，O_APPEND 保证写入安全。
// NFS 可用：rename 原子性 + os.Open 二次确认防止并发轮转。
func New(cfg Config) (*Writer, error) {
	if cfg.ErrorHandler == nil {
		cfg.ErrorHandler = func(err error) {
			fmt.Fprintf(os.Stderr, "filerotate: %v\n", err)
		}
	}
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = time.Second
	}

	w := &Writer{
		filePath:      cfg.FilePath,
		lockPath:      cfg.FilePath + ".lock",
		maxSize:       int64(cfg.MaxSizeMB) * 1024 * 1024,
		maxAgeDays:    cfg.MaxAgeDays,
		checkInterval: cfg.CheckInterval,
		rotateCh:      make(chan struct{}, 1),
		errorHandler:  cfg.ErrorHandler,
		done:          make(chan struct{}),
	}

	if err := os.MkdirAll(filepath.Dir(cfg.FilePath), 0o755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	var err error
	w.file, err = openFileAppend(w.filePath)
	if err != nil {
		return nil, err
	}

	w.rotateLocker = flock.New(w.lockPath)

	fi, err := w.file.Stat()
	if err != nil {
		w.file.Close()
		return nil, fmt.Errorf("stat file: %w", err)
	}
	w.lastFileInfo = fi

	w.wg.Add(1)
	go w.poll()
	return w, nil
}

// Write 实现 io.Writer。写入前检查轮转信号，若本轮写入期间发生轮转，
// 数据进入备份文件而非新文件，但不会丢失，下次写入时自动重开文件。
func (w *Writer) Write(p []byte) (n int, err error) {
	select {
	case <-w.rotateCh:
		if err := w.reopenFile(); err != nil {
			w.reportError(err)
			w.requeueRotate()
		} else {
			w.pollMu.Lock()
			w.pollActive = false
			if fi, err := w.file.Stat(); err == nil {
				w.lastFileInfo = fi
			}
			w.pollMu.Unlock()
		}
	default:
	}

	f := w.file
	if f == nil {
		return 0, ErrFileNotOpen
	}
	return f.Write(p)
}

// Close 释放资源，关闭文件和锁。
func (w *Writer) Close() error {
	close(w.done)
	w.wg.Wait()

	if w.rotateLocker != nil {
		w.rotateLocker.Unlock()
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// poll 定期检查文件状态，变化时触发轮转。
func (w *Writer) poll() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.done:
			return
		case <-ticker.C:
			w.checkAndRotate()
		}
	}
}

// checkAndRotate 检查文件状态，按需触发轮转。
//
// 先尝试获取文件锁（非阻塞），拿到锁后在锁保护下检查文件大小。
// flock 仅对本机进程有效，NFS 跨主机时可能出现多个主机同时判断超阈值并执行
// rename——先到者 rename 成功后后来者可能 rename 前者刚创建的空文件，产生空备份，
// 但不会丢失数据。轮转不频繁，pollMu 和 rotateLocker 均使用 defer 释放。
func (w *Writer) checkAndRotate() {
	w.pollMu.Lock()
	defer w.pollMu.Unlock()

	if w.pollActive {
		return
	}

	ok, lockErr := w.rotateLocker.TryLock()
	if lockErr != nil {
		w.reportError(fmt.Errorf("获取轮转锁失败: %w", lockErr))
		return
	}
	if !ok {
		return
	}
	defer w.rotateLocker.Unlock()

	f, err := os.Open(w.filePath)
	if err != nil {
		w.reportError(err)
		return
	}
	fi, err := f.Stat()
	f.Close()
	if err != nil {
		w.reportError(err)
		return
	}

	if !os.SameFile(w.lastFileInfo, fi) || (w.maxSize > 0 && fi.Size() >= w.maxSize) {
		w.pollActive = true
		w.lastFileInfo = fi

		if w.maxSize > 0 && fi.Size() >= w.maxSize {
			if err := w.doRotation(); err != nil {
				w.reportError(fmt.Errorf("轮转执行失败: %w", err))
			}
		}
		select {
		case w.rotateCh <- struct{}{}:
		default:
		}
	} else {
		w.lastFileInfo = fi
	}
}

// doRotation 执行文件轮转：重命名、重开。
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

func (w *Writer) reportError(err error) {
	if w.errorHandler != nil {
		w.errorHandler(err)
	}
}

func (w *Writer) requeueRotate() {
	select {
	case w.rotateCh <- struct{}{}:
	default:
	}
}
