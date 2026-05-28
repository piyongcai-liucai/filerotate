// Package locker 提供本地文件锁的单元测试。
package locker

import (
	"os"
	"path/filepath"
	"testing"
)

// ---------- 辅助函数 ----------

const logDir = "../../example/log"

func ensureLogDir(t *testing.T) {
	t.Helper()
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("create log dir: %v", err)
	}
}

// ---------- LocalLocker 测试 ----------

// TestLocalLocker 验证基于文件锁的本地互斥锁。
// 1. 获取锁后，第二个实例无法获取。
// 2. 释放锁后，第二个实例可以获取。
func TestLocalLocker(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, t.Name()+".lock")
	defer os.Remove(path)

	l, err := NewLocalLocker(path)
	if err != nil {
		t.Fatal(err)
	}

	ok, err := l.TryLock()
	if err != nil || !ok {
		t.Fatal("TryLock failed")
	}

	l2, err := NewLocalLocker(path)
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

// TestLocalLockerDirCreation 验证锁文件目录不存在时自动创建。
func TestLocalLockerDirCreation(t *testing.T) {
	ensureLogDir(t)
	dir := filepath.Join(logDir, t.Name(), "subdir")
	lockPath := filepath.Join(dir, "test.lock")
	defer os.RemoveAll(filepath.Join(logDir, t.Name()))

	l, err := NewLocalLocker(lockPath)
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

// ---------- LocalLocker 错误路径测试 ----------

func TestLocalLocker_CreateError(t *testing.T) {
	ensureLogDir(t)
	// 创建一个普通文件作为路径阻断器，
	// MkdirAll 会因为该文件名已存在（不是目录）而失败
	blocker := filepath.Join(logDir, t.Name()+"_blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(blocker)

	_, err := NewLocalLocker(filepath.Join(blocker, "test.lock"))
	if err == nil {
		t.Fatal("expected error creating lock file")
	}
}

func TestLocalLocker_TryLockOnDifferentInstance(t *testing.T) {
	ensureLogDir(t)
	p1 := filepath.Join(logDir, t.Name()+"_1.lock")
	p2 := filepath.Join(logDir, t.Name()+"_2.lock")
	defer os.Remove(p1)
	defer os.Remove(p2)

	l1, _ := NewLocalLocker(p1)
	l2, _ := NewLocalLocker(p2)

	ok, _ := l1.TryLock()
	if !ok {
		t.Fatal("first TryLock should succeed")
	}
	defer l1.Unlock()

	// 不同路径的锁应独立（非互斥）
	ok, _ = l2.TryLock()
	if !ok {
		t.Fatal("lock on different path should succeed independently")
	}
	l2.Unlock()
}
