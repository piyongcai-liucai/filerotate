// Lite 版示例：模拟 3 个进程并发写入，无 IPC，通过内置轮询和分布式锁协调轮转。
//
// 运行多个实例可验证真正的跨进程协调：
//
//	go run .          # 终端 1
//	go run .          # 终端 2
//	go run .          # 终端 3
//
// 每个进程通过内置 goroutine 定期检查文件大小，超阈值后通过分布式锁竞争执行轮转。
// 总共轮转 10 次后自动退出。
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/piyongcai-liucai/filerotate"
)

const (
	numProcs  = 3
	logFile   = "../log/app.lite.log"
	maxSizeMB = 1
	maxRotate = 10
)

func main() {
	fmt.Printf("=== Lite 版多进程示例 ===\n")
	fmt.Printf("进程数: %d | 轮转阈值: %d MB | 目标轮转次数: %d\n", numProcs, maxSizeMB, maxRotate)

	os.MkdirAll(filepath.Dir(logFile), 0o755)

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		monitorRotations(logFile, maxRotate, done)
	}()

	for id := 0; id < numProcs; id++ {
		wg.Add(1)
		go func(procID int) {
			defer wg.Done()
			runLiteProcess(procID, done)
		}(id)
	}

	wg.Wait()
	fmt.Println("所有进程已退出")
}

func runLiteProcess(id int, done <-chan struct{}) {
	writer, err := filerotate.NewLite(filerotate.LiteConfig{
		FilePath:   logFile,
		MaxSizeMB:  maxSizeMB,
		MaxAgeDays: 7,
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

		fmt.Fprintf(writer, "[进程 %d/%d PID %d] %s 第 %d 条日志 —— Lite版内置轮询示例\n",
			id, numProcs, pid, time.Now().Format(time.RFC3339), i)
		time.Sleep(time.Millisecond)
	}
}

func monitorRotations(filePath string, max int, done chan<- struct{}) {
	base := filepath.Base(filePath)
	for {
		count := countBackups(filePath, base)
		fmt.Printf("\r[监控] 当前备份数: %d/%d", count, max)
		if count >= max {
			fmt.Printf("\n[监控] 已达到 %d 次轮转，通知进程退出\n", max)
			close(done)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func countBackups(filePath, base string) int {
	matches, _ := filepath.Glob(filePath + ".2*")
	n := 0
	for _, m := range matches {
		ext := strings.TrimPrefix(filepath.Base(m), base+".")
		if len(ext) == 25 && ext[8] == '_' && ext[15] == '.' {
			n++
		}
	}
	return n
}
