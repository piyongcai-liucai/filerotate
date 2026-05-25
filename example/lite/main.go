// Lite 版示例：极简设计，无进程间通信，仅依赖文件锁和本地计数。
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/piyongcai-liucai/filerotate"
)

func main() {
	// 创建 Lite 版 Writer
	writer, err := filerotate.NewLiteWriter(filerotate.LiteConfig{
		FilePath:         "../log/app.log", // 日志文件路径
		PerProcSizeMB:    1,                // 每个进程写入 25 MB 后触发轮转
		MaxAgeDays:       7,                // 备份保留 7 天
		MaxWriteInterval: time.Second,
	})
	if err != nil {
		panic(err)
	}
	defer writer.Close()

	for i := 0; ; i++ {
		fmt.Fprintf(writer, "[PID %d] %s log entry %d\n",
			os.Getpid(), time.Now().Format(time.RFC3339), i)
		time.Sleep(time.Millisecond)
	}
}
