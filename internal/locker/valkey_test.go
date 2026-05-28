// Package locker 提供 Valkey 分布式锁的单元测试。
package locker

import (
	"context"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeylock"
)

// connectValkeyOrSkip 尝试连接本地 Valkey/Redis 服务，失败则跳过当前测试。
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

// ---------- ValkeyLocker 测试 ----------

// TestValkeyLocker 验证基于 Valkey 的分布式锁。
func TestValkeyLocker(t *testing.T) {
	client := connectValkeyOrSkip(t)
	defer client.Close()

	key := "test-lock-" + t.Name()

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
	if err != nil {
		t.Fatalf("second TryLock unexpected error: %v", err)
	}
	if ok {
		t.Fatal("second TryLock should fail because lock is still held")
	}

	locker.Unlock()

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

	if err := locker.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

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
