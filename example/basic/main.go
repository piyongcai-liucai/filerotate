// 标准版示例：使用默认的本地 IPC 通知器和文件锁。
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/piyongcai-liucai/filerotate"
)

func main() {
	// 创建标准版 Writer，所有进程使用相同的 FilePath 即可自动协调
	writer, err := filerotate.New(filerotate.Config{
		FilePath:      "../log/app.log", // 日志文件路径
		MaxSizeMB:     1,                // 10 MB 轮转阈值
		MaxAgeDays:    7,                // 备份保留 7 天
		CheckInterval: time.Second,      // Leader 每 5 秒检查一次文件大小
		ErrorHandler: func(err error) {
			log.Printf("filerotate 内部错误: %v", err)
			// 可以在这里发送告警、记录到文件等
		},
	})
	if err != nil {
		panic(err)
	}
	defer writer.Close()

	// 直接作为 io.Writer 使用
	for i := 0; ; i++ {
		fmt.Fprintf(writer, "[PID %d] %s log entry %d\n",
			os.Getpid(), time.Now().Format(time.RFC3339), i)
		time.Sleep(time.Millisecond)
	}
}
