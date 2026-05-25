package filerotate

// Locker 分布式锁接口，用于保护轮转操作。
//
// 在单机部署时，默认的文件锁（FileLocker）已足够。
// 仅在跨主机共享文件系统（如 NFS）且需要协调轮转时，才需要实现此接口。
type Locker interface {
	// TryLock 非阻塞尝试获取锁，返回是否成功。
	TryLock() (bool, error)
	// Unlock 释放锁。
	Unlock() error
}
