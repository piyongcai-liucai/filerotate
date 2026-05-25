// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件实现基于 Valkey (Redis) 的分布式锁，使用 valkeylock 包的 Redlock 算法。
package filerotate

import (
	"context"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeylock"
)

// ValkeyLocker 基于 valkeylock 包的分布式锁实现。
//
// 内部使用 Redlock 算法，支持以下特性：
//   - 多节点多数派共识：锁需要在多数节点上成功创建才算获取成功
//   - 锁自动续期：持有锁期间，后台自动延长锁的有效期
//   - 客户端缓存失效通知：客户端可快速感知锁的释放
//
// 适合跨主机共享存储（如 NFS）或需要高可用的场景。
// 在单机部署时，推荐使用默认的 FileLocker，性能更好且无外部依赖。
type ValkeyLocker struct {
	// locker 是 valkeylock 库返回的锁接口实例
	// NewLocker 返回的具体类型实现了 valkeylock.Locker 接口
	locker valkeylock.Locker

	// key 锁键名称，所有竞争同一资源的进程必须使用相同的 key
	key string

	// cancel 释放锁的回调函数，调用后锁自动释放
	// valkeylock 库会处理 Redlock 的释放逻辑
	cancel context.CancelFunc

	// ctx 持有锁的上下文，用于判断锁是否仍有效
	// 当 ctx 被取消时，表示锁已被释放或过期
	ctx context.Context
}

// NewValkeyLocker 创建一个 Valkey 分布式锁。
//
// 参数：
//   - clientOption: Valkey 客户端配置（连接地址、密码等）
//   - key: 锁名称，所有竞争同一资源的进程必须使用相同的 key
//   - keyMajority: 多数派数量。单节点设为 1，集群建议设为 N/2+1
//     （例如 3 节点集群设为 2，5 节点集群设为 3）
//   - keyValidity: 锁键有效期，过期后自动释放。
//     建议设置足够完成一次轮转操作的时间（如 30 秒）
//
// 示例：
//
//	locker, err := filerotate.NewValkeyLocker(
//	    valkey.ClientOption{InitAddress: []string{"localhost:6379"}},
//	    "my-lock-key",
//	    1,                // 单节点
//	    30*time.Second,   // 30 秒超时
//	)
func NewValkeyLocker(clientOption valkey.ClientOption, key string, keyMajority int32, keyValidity time.Duration) (*ValkeyLocker, error) {
	// 使用 valkeylock 库创建锁实例
	// LockerOption 配置锁的行为：
	//   - ClientOption: Valkey 连接配置
	//   - KeyMajority: 多数派数量
	//   - KeyValidity: 锁键有效期
	//   - NoLoopTracking: Valkey >= 7.0.5 建议启用，可提升性能
	locker, err := valkeylock.NewLocker(valkeylock.LockerOption{
		ClientOption:   clientOption,
		KeyMajority:    keyMajority,
		KeyValidity:    keyValidity,
		NoLoopTracking: true,
	})
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
// 返回：
//   - bool: true 表示获取成功，false 表示锁被其他进程持有
//   - error: 网络错误或其他异常
func (v *ValkeyLocker) TryLock() (bool, error) {
	// TryWithContext 尝试获取锁
	// 返回值：
	//   - ctx: 持有锁的上下文，nil 表示未获取到锁
	//   - cancel: 释放锁的回调函数
	//   - err: 错误信息
	ctx, cancel, err := v.locker.TryWithContext(context.Background(), v.key)
	if err != nil {
		return false, err
	}
	// ctx 为 nil 表示未获取到锁（锁被其他进程持有）
	if ctx == nil {
		return false, nil
	}
	// 保存上下文和取消函数，用于后续释放锁
	v.ctx = ctx
	v.cancel = cancel
	return true, nil
}

// Unlock 释放锁。
//
// 调用锁的取消函数，valkeylock 库会自动处理 Redlock 的释放逻辑。
// 可以安全地多次调用，重复调用不会产生副作用。
//
// 返回：
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
// 返回：
//   - error: 关闭过程中的错误
func (v *ValkeyLocker) Close() error {
	v.Unlock()
	v.locker.Close()
	return nil
}
