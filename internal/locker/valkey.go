// Package locker 提供分布式锁的内部实现。
//
// 本文件实现基于 Valkey (Redis) 的分布式锁（ValkeyLocker），
// 使用 valkeylock 包的 Redlock 算法，支持多节点多数派共识和锁自动续期。
package locker

import (
	"context"
	"strings"

	"github.com/valkey-io/valkey-go/valkeylock"
)

// ValkeyLocker 基于 valkeylock 包的分布式锁实现。
//
// 内部使用 Redlock 算法，在多个 Valkey 节点上尝试获取锁，
// 只有在多数节点上成功才算获取成功，保证了高可用性。
//
// 支持以下特性：
//   - 多数派共识：锁需要在多数节点上创建成功才算获取成功
//   - 锁自动续期：持有锁期间，后台自动延长锁的有效期
//   - 客户端缓存失效通知：客户端可快速感知锁的释放
//
// 适用于跨主机共享存储（如 NFS）或需要高可用的场景。
type ValkeyLocker struct {
	// locker 是 valkeylock 库返回的锁接口实例
	locker valkeylock.Locker

	// key 锁键名称，在创建时指定，用于后续 TryLock
	key string

	// cancel 释放锁的回调函数，调用后锁自动释放
	cancel context.CancelFunc

	// ctx 持有锁的上下文，用于判断锁是否仍有效
	ctx context.Context
}

// NewValkeyLocker 使用 valkeylock.LockerOption 和指定的 key 创建一个 Valkey 分布式锁。
//
// 参数:
//   - option: valkeylock.LockerOption 配置，用于底层锁的创建。
//   - key: 锁键名称，所有竞争同一资源的进程必须使用相同的 key。
//
// 返回:
//   - *ValkeyLocker: 锁实例
//   - error: 创建失败的错误
func NewValkeyLocker(option valkeylock.LockerOption, key string) (*ValkeyLocker, error) {
	locker, err := valkeylock.NewLocker(option)
	if err != nil {
		return nil, err
	}
	return &ValkeyLocker{
		locker: locker,
		key:    key,
	}, nil
}

// TryLock 非阻塞尝试获取锁，立即返回是否成功。
//
// 如果成功获取锁，内部会启动自动续期机制，确保锁在持有期间不会过期。
// 自动续期由 valkeylock 库管理，无需手动干预。
//
// 返回:
//   - bool: true 表示获取成功，false 表示锁被其他进程持有
//   - error: 仅当发生网络故障等严重错误时返回非 nil；普通锁竞争时返回 (false, nil)
func (v *ValkeyLocker) TryLock() (bool, error) {
	// 防止同一个实例重复加锁
	if v.cancel != nil {
		return false, nil
	}

	// 调用底层库的非阻塞获取
	ctx, cancel, err := v.locker.TryWithContext(context.Background(), v.key)
	if err != nil {
		// valkeylock 在锁被他人持有时会返回类似 "not locked: key ... is held by others" 的错误，
		// 而不是返回 (nil, nil)。这里需要将其转换为正常的“未获取到锁”状态。
		if strings.Contains(err.Error(), "held by others") {
			return false, nil
		}
		// 其他错误（如网络中断）视为严重错误
		return false, err
	}
	// 按照 valkeylock 文档，ctx == nil 也表示锁已被占用（尽管新版通常返回错误）
	if ctx == nil {
		return false, nil
	}
	// 获取成功，保存上下文和取消函数用于后续解锁
	v.ctx = ctx
	v.cancel = cancel
	return true, nil
}

// Unlock 释放锁。
//
// 调用锁的取消函数，valkeylock 库会自动处理 Redlock 的释放逻辑。
// 可以安全地多次调用，重复调用不会产生副作用。
//
// 返回:
//   - error: 释放过程中的错误（通常为 nil）
func (v *ValkeyLocker) Unlock() error {
	if v.cancel != nil {
		v.cancel()
		v.cancel = nil
		v.ctx = nil
	}
	return nil
}

// Close 释放锁并关闭底层连接资源。
//
// 如果不再需要此锁实例，建议调用 Close 来彻底清理资源。
// 调用 Close 后，此锁实例不应再使用。
//
// 返回:
//   - error: 关闭过程中的错误
func (v *ValkeyLocker) Close() error {
	v.Unlock()
	v.locker.Close()
	return nil
}
