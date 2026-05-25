// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件实现基于文件锁的本地互斥锁。
package filerotate

import (
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
)

// FileLocker 基于文件锁的本地互斥锁，适用于单机多进程。
// 使用 flock 系统调用，跨平台（Linux/macOS/Windows）。
type FileLocker struct {
	lock     *flock.Flock // 底层文件锁
	lockPath string       // 锁文件路径
}

// NewFileLocker 创建一个文件锁。
// 如果锁文件所在目录不存在，会自动创建。
//
// 参数:
//   - lockPath: 锁文件路径，所有进程必须使用相同的路径。
//
// 返回:
//   - *FileLocker: 文件锁实例
func NewFileLocker(lockPath string) *FileLocker {
	// 自动创建锁文件所在目录
	dir := filepath.Dir(lockPath)
	os.MkdirAll(dir, 0o755)

	return &FileLocker{
		lock:     flock.New(lockPath),
		lockPath: lockPath,
	}
}

// TryLock 非阻塞尝试获取锁。
//
// 返回:
//   - bool: 是否成功获取锁
//   - error: 错误信息
func (f *FileLocker) TryLock() (bool, error) {
	return f.lock.TryLock()
}

// Unlock 释放锁。
//
// 返回:
//   - error: 错误信息
func (f *FileLocker) Unlock() error {
	return f.lock.Unlock()
}
