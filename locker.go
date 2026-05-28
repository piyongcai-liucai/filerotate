// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件定义 Locker 接口及工厂函数，用于创建分布式锁。
// 所有锁实现（文件锁、Valkey 锁）均通过此接口对外暴露，
// 具体实现位于 internal/locker 包中，外部不可见。
package filerotate

import (
	"github.com/valkey-io/valkey-go/valkeylock"

	"github.com/piyongcai-liucai/filerotate/internal/locker"
)

// Locker 分布式锁接口，用于保护轮转操作。
// 单机模式内置文件锁，无需关心此接口。
// 仅在跨主机场景下才需要提供自定义实现（如 NewValkeyLocker）。
type Locker interface {
	// TryLock 非阻塞尝试获取锁，返回是否成功。
	// 成功获取锁后，调用者应确保在操作完成后调用 Unlock。
	TryLock() (bool, error)

	// Unlock 释放锁。
	Unlock() error
}

// newLocalLocker 创建一个基于文件的本地互斥锁。
// lockPath 为锁文件路径，所有参与竞争的进程必须使用相同的路径。
// 如果锁文件所在目录不存在，会自动创建。
func newLocalLocker(lockPath string) (Locker, error) {
	return locker.NewLocalLocker(lockPath)
}

// NewValkeyLocker 创建一个基于 Valkey 的分布式锁。
//
// 使用 valkeylock 包的 Redlock 算法，支持多节点多数派共识、锁自动续期和客户端缓存失效通知。
// 用户可以通过 option 完全控制锁的行为，例如设置客户端配置、多数派数量、锁有效期等。
//
// 参数:
//   - option: valkeylock.LockerOption 配置，用于底层锁的创建。
//   - key: 锁键名称，所有竞争同一资源的进程必须使用相同的 key。
//
// 示例:
//
//	locker, err := filerotate.NewValkeyLocker(
//	    valkeylock.LockerOption{
//	        ClientOption: valkey.ClientOption{InitAddress: []string{"localhost:6379"}},
//	        KeyMajority:  1,
//	        KeyValidity:   30 * time.Second,
//	    },
//	    "my-lock-key",
//	)
func NewValkeyLocker(option valkeylock.LockerOption, key string) (Locker, error) {
	return locker.NewValkeyLocker(option, key)
}
