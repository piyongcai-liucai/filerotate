// Valkey 示例：同时使用 Valkey Pub/Sub 通知器和 Valkey 分布式锁，
// 模拟 3 个进程并发写入，适用于跨主机 NFS 共享存储场景。
//
// 需要可访问的 Valkey/Redis 服务。运行多个实例可验证跨主机协调：
//
//	go run .          # 主机 1
//	go run .          # 主机 2
//	go run .          # 主机 3
//
// Leader 通过 Valkey Pub/Sub 广播轮转命令，分布式锁防止并发轮转冲突。
// 轮转 10 次后自动退出。
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeylock"

	"github.com/piyongcai-liucai/filerotate"
	"github.com/piyongcai-liucai/filerotate/example/common"
)

const (
	valkeyAddr = "localhost:6379"
	numProcs   = 3
	logFile    = "../log/app.valkey.log"
	maxSizeMB  = 1
	maxRotate  = 10
)

func main() {
	fmt.Printf("=== Valkey 通知器+锁多进程示例 ===\n")
	fmt.Printf("Valkey: %s | 进程数: %d | 轮转阈值: %d MB | 目标轮转次数: %d\n",
		valkeyAddr, numProcs, maxSizeMB, maxRotate)

	os.MkdirAll(filepath.Dir(logFile), 0o755)

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		common.MonitorRotations(logFile, maxRotate, done)
	}()

	for id := 0; id < numProcs; id++ {
		wg.Add(1)
		go func(procID int) {
			defer wg.Done()
			runValkeyProcess(procID, done)
		}(id)
	}

	wg.Wait()
	fmt.Println("所有进程已退出")
}

func runValkeyProcess(id int, done <-chan struct{}) {
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{valkeyAddr},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[进程 %d] 连接 Valkey 失败: %v\n", id, err)
		return
	}
	defer client.Close()

	writer, err := filerotate.New(filerotate.Config{
		FilePath:      logFile,
		MaxSizeMB:     maxSizeMB,
		MaxAgeDays:    7,
		CheckInterval: time.Second,
		LockerFactory: func(lockPath string) (filerotate.Locker, error) {
			return filerotate.NewValkeyLocker(
				valkeylock.LockerOption{
					ClientOption: valkey.ClientOption{InitAddress: []string{valkeyAddr}},
					KeyMajority:  1,
					KeyValidity:  30 * time.Second,
				},
				lockPath,
			)
		},
		NotifierFactory: func(errorHandler func(error)) (filerotate.Notifier, error) {
			return filerotate.NewValkeyNotifier(client, "filerotate.rotate", errorHandler), nil
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[进程 %d] 创建 Writer 失败: %v\n", id, err)
		return
	}
	defer writer.Close()

	pid := os.Getpid()
	for i := 0; ; i++ {
		select {
		case <-done:
			fmt.Printf("[进程 %d] 收到退出信号，已写入 %d 条日志\n", id, i)
			return
		default:
		}

		fmt.Fprintf(writer, "[进程 %d/%d PID %d] %s 第 %d 条日志 —— Valkey Pub/Sub + Redlock 示例\n",
			id, numProcs, pid, time.Now().Format(time.RFC3339), i)
		time.Sleep(time.Millisecond)
	}
}
