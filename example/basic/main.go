// 标准版示例：模拟 3 个进程并发写入，使用默认本地 IPC 和文件锁协调轮转。
//
// 运行多个实例可验证真正的跨进程协调：
//
//	go run .          # 终端 1
//	go run .          # 终端 2
//	go run .          # 终端 3
//
// 每个进程独立写入带有自己 PID 标识的日志，Leader 检测文件达到 1MB 后
// 通过本地 IPC 通知所有进程重开文件。轮转 10 次后自动退出。
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
	logFile   = "../log/app.log"
	maxSizeMB = 1
	maxRotate = 10
)

func main() {
	fmt.Printf("=== 标准版多进程示例 ===\n")
	fmt.Printf("进程数: %d | 轮转阈值: %d MB | 目标轮转次数: %d\n", numProcs, maxSizeMB, maxRotate)

	// 确保日志目录存在
	os.MkdirAll(filepath.Dir(logFile), 0o755)

	// 监控备份文件数，达到目标后通知所有进程退出
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		monitorRotations(logFile, maxRotate, done)
	}()

	// 启动 N 个模拟进程
	for id := 0; id < numProcs; id++ {
		wg.Add(1)
		go func(procID int) {
			defer wg.Done()
			runStandardProcess(procID, done)
		}(id)
	}

	wg.Wait()
	fmt.Println("所有进程已退出")
}

func runStandardProcess(id int, done <-chan struct{}) {
	writer, err := filerotate.New(filerotate.Config{
		FilePath:      logFile,
		MaxSizeMB:     maxSizeMB,
		MaxAgeDays:    7,
		CheckInterval: time.Second,
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

		fmt.Fprintf(writer, "[进程 %d/%d PID %d] %s 第 %d 条日志 —— 标准版本地IPC示例\n",
			id, numProcs, pid, time.Now().Format(time.RFC3339), i)
		time.Sleep(time.Millisecond)
	}
}

// monitorRotations 定期检查备份文件数量，达到上限后关闭 done channel。
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

// countBackups 统计符合轮转备份命名规范的文件数量。
func countBackups(filePath, base string) int {
	matches, _ := filepath.Glob(filePath + ".2*")
	n := 0
	for _, m := range matches {
		ext := strings.TrimPrefix(filepath.Base(m), base+".")
		// 备份格式: 20060102_150405.000000000 (25字符，纳秒精度)
		if len(ext) == 25 && ext[8] == '_' && ext[15] == '.' {
			n++
		}
	}
	return n
}
