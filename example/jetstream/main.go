// JetStream 示例：使用 NATS JetStream 临时消费者进行进程间通信，模拟 3 个进程并发写入。
//
// 需要可访问的 NATS 服务器（启用 JetStream）。运行多个实例可验证跨主机协调：
//
//	go run .          # 主机 1
//	go run .          # 主机 2
//	go run .          # 主机 3
//
// 使用临时消费者（Ephemeral Consumer），进程退出后自动清理，只接收新消息。
// 轮转 10 次后自动退出。
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/piyongcai-liucai/filerotate"
)

const (
	natsURL   = "nats://172.20.130.90:4222"
	numProcs  = 3
	logFile   = "../log/app.js.log"
	maxSizeMB = 1
	maxRotate = 10
)

func main() {
	fmt.Printf("=== JetStream 通知器多进程示例 ===\n")
	fmt.Printf("NATS: %s | 进程数: %d | 轮转阈值: %d MB | 目标轮转次数: %d\n",
		natsURL, numProcs, maxSizeMB, maxRotate)

	os.MkdirAll(filepath.Dir(logFile), 0o755)

	// 预先创建 Stream（多进程只需创建一次，重复创建会被忽略）
	nc, err := nats.Connect(natsURL)
	if err != nil {
		panic(err)
	}
	js, err := nc.JetStream()
	if err != nil {
		panic(err)
	}
	if err := ensureStream(js); err != nil {
		panic(err)
	}
	nc.Close()

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
			runJetStreamProcess(procID, done)
		}(id)
	}

	wg.Wait()
	fmt.Println("所有进程已退出")
}

func runJetStreamProcess(id int, done <-chan struct{}) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[进程 %d] 连接 NATS 失败: %v\n", id, err)
		return
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[进程 %d] 创建 JetStream 上下文失败: %v\n", id, err)
		return
	}

	writer, err := filerotate.New(filerotate.Config{
		FilePath:      logFile,
		MaxSizeMB:     maxSizeMB,
		MaxAgeDays:    7,
		CheckInterval: time.Second,
		NotifierFactory: func(commPath string, errorHandler func(error)) (filerotate.Notifier, error) {
			return filerotate.NewJetStreamNotifier(js, "filerotate.rotate", "FILEROTATE", errorHandler), nil
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

		fmt.Fprintf(writer, "[进程 %d/%d PID %d] %s 第 %d 条日志 —— JetStream 临时消费者示例\n",
			id, numProcs, pid, time.Now().Format(time.RFC3339), i)
		time.Sleep(time.Millisecond)
	}
}

func ensureStream(js nats.JetStreamContext) error {
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "FILEROTATE",
		Subjects:  []string{"filerotate.rotate"},
		Replicas:  1,
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		MaxAge:    24 * time.Hour,
		MaxMsgs:   1000,
		Discard:   nats.DiscardOld,
	})
	if err != nil && err != nats.ErrStreamNameAlreadyInUse {
		return err
	}
	return nil
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
