package filerotate

import (
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
)

func TestFileLocker(t *testing.T) {
	path := t.TempDir() + "/test.lock"
	l := NewFileLocker(path)
	ok, err := l.TryLock()
	if err != nil || !ok {
		t.Fatal("TryLock failed")
	}
	l2 := NewFileLocker(path)
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

func TestValkeyLocker(t *testing.T) {
	// 如果 Valkey 不可用，跳过测试
	locker, err := NewValkeyLocker(
		valkey.ClientOption{InitAddress: []string{"localhost:6379"}},
		"test-lock-"+t.Name(),
		1,
		10*time.Second,
	)
	if err != nil {
		t.Skipf("Valkey not available: %v", err)
	}
	defer locker.Close()

	ok, err := locker.TryLock()
	if err != nil || !ok {
		t.Fatal("TryLock failed")
	}

	// 第二个锁实例无法获取同一把锁
	locker2, _ := NewValkeyLocker(
		valkey.ClientOption{InitAddress: []string{"localhost:6379"}},
		"test-lock-"+t.Name(),
		1,
		10*time.Second,
	)
	defer locker2.Close()
	ok, err = locker2.TryLock()
	if err != nil || ok {
		t.Fatal("second TryLock should fail")
	}

	// 释放后再获取应成功
	locker.Unlock()
	ok, err = locker2.TryLock()
	if err != nil || !ok {
		t.Fatal("TryLock should succeed after unlock")
	}
	locker2.Unlock()
}
