// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件实现单机（Local）模式的自轮转循环和便捷构造函数，
// 使用内置轮询通知器 + 文件锁协调多进程。
package filerotate

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// NewLocal 创建一个单机模式 Writer。
//
// 与 New() 不同，NewLocal 强制使用内置 LocalNotifier（文件轮询）+ LocalLocker（文件锁），
// 忽略 Config 中的 LockerFactory 和 NotifierFactory。
//
// 单机模式下每个进程自给自足：
//   - 独立轮询文件状态（大小 + inode）
//   - 检测到超阈值时通过文件锁竞争轮转权
//   - 检测到 inode 变化（其他进程已完成轮转）时自动重开文件
func NewLocal(cfg Config) (*Writer, error) {
	cfg.NotifierFactory = nil
	cfg.LockerFactory = nil
	return New(cfg)
}

// newLocal 创建单机模式 Writer。
func newLocal(cfg Config) (*Writer, error) {
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = 1 * time.Second
	}

	w := &Writer{
		filePath:      cfg.FilePath,
		lockPath:      cfg.FilePath + ".lock",
		maxSize:       int64(cfg.MaxSizeMB) * 1024 * 1024,
		maxAgeDays:    cfg.MaxAgeDays,
		checkInterval: cfg.CheckInterval,
		rotateCh:      make(chan struct{}, 1),
		errorHandler:  cfg.ErrorHandler,
		local:         true,
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

	w.rotateLocker, err = NewLocalLocker(w.lockPath)
	if err != nil {
		w.file.Close()
		return nil, fmt.Errorf("create rotate locker: %w", err)
	}

	w.notifier, err = newLocalNotifier(w.filePath, &w.maxSize, cfg.CheckInterval, cfg.ErrorHandler)
	if err != nil {
		w.file.Close()
		w.rotateLocker.Unlock()
		return nil, fmt.Errorf("create notifier: %w", err)
	}

	if err := w.notifier.Serve(); err != nil {
		w.file.Close()
		w.rotateLocker.Unlock()
		return nil, fmt.Errorf("serve notifier: %w", err)
	}

	cmdCh, err := w.notifier.Connect()
	if err != nil {
		w.file.Close()
		w.rotateLocker.Unlock()
		return nil, fmt.Errorf("connect notifier: %w", err)
	}

	w.wg.Add(1)
	go w.runLocalLoop(cmdCh)
	return w, nil
}

// runLocalLoop 单机模式主循环：接收本地轮询通知，执行轮转或重开。
//
// LocalNotifier 检测到 size 超阈值或 inode 变化时发送 ROTATE。
// runLocalLoop 收到 ROTATE 后：
//   - 若文件大小超阈值：抢锁 → 轮转 → 解锁 → 通知 Write() 重开
//   - 若仅 inode 变化（其他进程已轮转）：通知 Write() 重开
func (w *Writer) runLocalLoop(cmdCh <-chan string) {
	defer w.wg.Done()

	for cmd := range cmdCh {
		if cmd != CmdRotate || w.maxSize < 1 {
			continue
		}
		w.tryRotate()
		select {
		case w.rotateCh <- struct{}{}:
		default:
		}
	}
}

// tryRotate 抢锁 → Stat → 按需轮转。返回 true 表示已执行轮转。
func (w *Writer) tryRotate() bool {
	ok, lockErr := w.rotateLocker.TryLock()
	if lockErr != nil {
		w.reportError(fmt.Errorf("获取轮转锁失败: %w", lockErr))
		return false
	}
	if !ok {
		return false
	}
	defer w.rotateLocker.Unlock()

	fi, err := os.Stat(w.filePath)
	if err != nil {
		w.reportError(fmt.Errorf("检查文件状态失败: %w", err))
		return false
	}
	if fi.Size() < w.maxSize {
		return false
	}
	if err := w.doRotation(); err != nil {
		w.reportError(fmt.Errorf("轮转执行失败: %w", err))
		return false
	}
	w.updateNotifierFileInfo()
	return true
}

// updateNotifierFileInfo 轮转后更新 notifier 中的文件信息，
// 避免下次 poll 将自身轮转导致的 inode 变化误判为外部轮转。
func (w *Writer) updateNotifierFileInfo() {
	if n, ok := w.notifier.(interface{ UpdateFileInfo(os.FileInfo) }); ok {
		if fi, err := os.Stat(w.filePath); err == nil {
			n.UpdateFileInfo(fi)
		}
	}
}
