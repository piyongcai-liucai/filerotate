// 使用 NATS 核心通知器的标准版示例。
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/piyongcai-liucai/filerotate"
)

func main() {
	// 连接 NATS
	nc, err := nats.Connect("nats://172.20.130.90:4222")
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	// 创建 Writer，注入 NATS 通知器
	writer, err := filerotate.New(filerotate.Config{
		FilePath:      "../log/app.log",
		MaxSizeMB:     1,
		MaxAgeDays:    7,
		CheckInterval: 100 * time.Millisecond,
		NotifierFactory: func(commPath string, errorHandler func(error)) (filerotate.Notifier, error) {
			return filerotate.NewNATSNotifier(nc, "filerotate.rotate", errorHandler), nil
		},
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
