// Package locker 提供分布式锁的内部实现。
//
// 本文件实现基于文件系统的本地互斥锁（FileLocker），
// 使用 flock 系统调用，跨平台支持 Linux、macOS 和 Windows。
package locker

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
)

// FileLocker 基于文件锁的本地互斥锁，适用于单机多进程。
//
// 内部使用 flock 库，在 Linux/macOS 上调用 flock 系统调用，
// 在 Windows 上使用 LockFileEx，保证同一台机器上多个进程之间的互斥访问。
//
// 锁文件路径由调用者指定，所有参与竞争的进程必须使用相同的路径。
type FileLocker struct {
	lock     *flock.Flock // 底层文件锁对象
	lockPath string       // 锁文件路径（用于日志或调试）
}

// NewFileLocker 创建一个文件锁实例。
//
// 如果锁文件所在的目录不存在，会自动创建（包括所有父目录）。
// 锁文件本身会在 TryLock 时自动创建。
//
// 参数:
//   - lockPath: 锁文件路径，所有参与竞争的进程必须使用相同的路径。
//
// 返回:
//   - *FileLocker: 文件锁实例，已准备好使用。
func NewFileLocker(lockPath string) (*FileLocker, error) {
	dir := filepath.Dir(lockPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create lock directory %s: %w", dir, err)
	}

	return &FileLocker{
		lock:     flock.New(lockPath),
		lockPath: lockPath,
	}, nil
}

// TryLock 非阻塞尝试获取锁。
//
// 如果锁当前未被其他进程持有，则立即获取锁并返回 true。
// 如果锁已被其他进程持有，则立即返回 false，不会阻塞等待。
//
// 返回:
//   - bool: 是否成功获取锁
//   - error: 获取锁过程中的错误（如权限不足、磁盘空间满等）
func (f *FileLocker) TryLock() (bool, error) {
	return f.lock.TryLock()
}

// Unlock 释放锁。
//
// 释放当前持有的锁，允许其他进程获取。
// 如果锁未被持有或已释放，调用此方法是安全的，不会产生错误。
//
// 返回:
//   - error: 释放锁过程中的错误
func (f *FileLocker) Unlock() error {
	return f.lock.Unlock()
}
