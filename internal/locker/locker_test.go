// Package locker 提供分布式锁的单元测试。
// 测试覆盖文件锁和 Valkey 锁的互斥与释放逻辑。
// 当外部服务（Valkey）不可用时，相关测试会自动跳过。
package locker

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeylock"
)

// ---------- 辅助函数 ----------

// connectValkeyOrSkip 尝试连接本地 Valkey/Redis 服务，失败则跳过当前测试。
// 通过发送 PING 命令验证连接可用性。
func connectValkeyOrSkip(t *testing.T) valkey.Client {
	t.Helper()
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{"localhost:6379"},
	})
	if err != nil {
		t.Skipf("Valkey/Redis not available: %v", err)
	}
	if err := client.Do(context.Background(), client.B().Ping().Build()).Error(); err != nil {
		t.Skipf("Valkey/Redis ping failed: %v", err)
	}
	return client
}

// ---------- FileLocker 测试 ----------

// TestFileLocker 验证基于文件锁的本地互斥锁。
// 1. 获取锁后，第二个实例无法获取。
// 2. 释放锁后，第二个实例可以获取。
func TestFileLocker(t *testing.T) {
	path := t.TempDir() + "/test.lock"
	l, err := NewFileLocker(path)
	if err != nil {
		t.Fatal(err)
	}

	ok, err := l.TryLock()
	if err != nil || !ok {
		t.Fatal("TryLock failed")
	}

	l2, err := NewFileLocker(path)
	if err != nil {
		t.Fatal(err)
	}
	ok, err = l2.TryLock()
	if err != nil || ok {
		t.Fatal("second TryLock should fail")
	}

	l.Unlock()
	ok, err = l2.TryLock()
	if err != nil || !ok {
		t.Fatal("TryLock should succeed after unlock")
	}
	l2.Unlock()
}

// TestFileLockerDirCreation 验证锁文件目录不存在时自动创建。
func TestFileLockerDirCreation(t *testing.T) {
	dir := t.TempDir() + "/nonexistent/subdir"
	lockPath := dir + "/test.lock"

	l, err := NewFileLocker(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := l.TryLock()
	if err != nil || !ok {
		t.Fatalf("TryLock failed: %v", err)
	}
	l.Unlock()

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Fatal("directory should have been created")
	}
}

// ---------- ValkeyLocker 测试 ----------

// TestValkeyLocker 验证基于 Valkey 的分布式锁。
// 测试互斥性：第一个锁获取后，第二个无法获取；释放后第二个可获取。
func TestValkeyLocker(t *testing.T) {
	client := connectValkeyOrSkip(t)
	defer client.Close()

	key := "test-lock-" + t.Name()

	// 创建第一个锁实例
	locker, err := NewValkeyLocker(
		valkeylock.LockerOption{
			ClientOption:   valkey.ClientOption{InitAddress: []string{"localhost:6379"}},
			KeyMajority:    1,
			KeyValidity:    30 * time.Second, // 足够长的有效期避免过期干扰
			NoLoopTracking: true,
		},
		key,
	)
	if err != nil {
		t.Fatalf("create locker: %v", err)
	}
	defer locker.Close()

	// 第一次获取锁应成功
	ok, err := locker.TryLock()
	if err != nil || !ok {
		t.Fatalf("first TryLock failed: ok=%v, err=%v", ok, err)
	}

	// 创建第二个锁实例（同一 key）
	locker2, err := NewValkeyLocker(
		valkeylock.LockerOption{
			ClientOption:   valkey.ClientOption{InitAddress: []string{"localhost:6379"}},
			KeyMajority:    1,
			KeyValidity:    30 * time.Second,
			NoLoopTracking: true,
		},
		key,
	)
	if err != nil {
		t.Fatalf("create second locker: %v", err)
	}
	defer locker2.Close()

	// 第二个锁尝试获取，预期失败
	ok, err = locker2.TryLock()
	if err != nil {
		// 如果 err 不是锁竞争导致，而是网络等严重错误，则测试失败
		t.Fatalf("second TryLock unexpected error: %v", err)
	}
	if ok {
		t.Fatal("second TryLock should fail because lock is still held")
	}

	// 释放第一个锁
	locker.Unlock()

	// 第二个锁现在应该可以获取
	ok, err = locker2.TryLock()
	if err != nil || !ok {
		t.Fatalf("TryLock should succeed after unlock: ok=%v, err=%v", ok, err)
	}
	locker2.Unlock()
}

// TestValkeyLockerDoubleTryLock 验证同一实例重复 TryLock 不会重复获取锁。
func TestValkeyLockerDoubleTryLock(t *testing.T) {
	client := connectValkeyOrSkip(t)
	defer client.Close()

	key := "test-lock-double-" + t.Name()

	locker, err := NewValkeyLocker(
		valkeylock.LockerOption{
			ClientOption:   valkey.ClientOption{InitAddress: []string{"localhost:6379"}},
			KeyMajority:    1,
			KeyValidity:    30 * time.Second,
			NoLoopTracking: true,
		},
		key,
	)
	if err != nil {
		t.Fatalf("create locker: %v", err)
	}
	defer locker.Close()

	ok, err := locker.TryLock()
	if err != nil || !ok {
		t.Fatalf("first TryLock failed: ok=%v, err=%v", ok, err)
	}

	// 同一实例重复 TryLock 应返回 false（不 panic，不重复加锁）
	ok, err = locker.TryLock()
	if err != nil || ok {
		t.Fatalf("second TryLock on same instance should return false, got ok=%v, err=%v", ok, err)
	}

	locker.Unlock()
}

// TestValkeyLockerCloseReleasesLock 验证 Close 后另一个实例可以获取锁。
func TestValkeyLockerCloseReleasesLock(t *testing.T) {
	client := connectValkeyOrSkip(t)
	defer client.Close()

	key := "test-lock-close-" + t.Name()

	locker, err := NewValkeyLocker(
		valkeylock.LockerOption{
			ClientOption:   valkey.ClientOption{InitAddress: []string{"localhost:6379"}},
			KeyMajority:    1,
			KeyValidity:    30 * time.Second,
			NoLoopTracking: true,
		},
		key,
	)
	if err != nil {
		t.Fatalf("create locker: %v", err)
	}

	ok, err := locker.TryLock()
	if err != nil || !ok {
		t.Fatalf("TryLock failed: ok=%v, err=%v", ok, err)
	}

	// Close 应释放锁
	if err := locker.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 另一个实例现在应可以获取锁
	locker2, err := NewValkeyLocker(
		valkeylock.LockerOption{
			ClientOption:   valkey.ClientOption{InitAddress: []string{"localhost:6379"}},
			KeyMajority:    1,
			KeyValidity:    30 * time.Second,
			NoLoopTracking: true,
		},
		key,
	)
	if err != nil {
		t.Fatalf("create second locker: %v", err)
	}
	defer locker2.Close()

	ok, err = locker2.TryLock()
	if err != nil || !ok {
		t.Fatalf("TryLock should succeed after Close: ok=%v, err=%v", ok, err)
	}
	locker2.Unlock()
}
