// 使用 JetStream 通知器的标准版示例。
// 需要提前创建 JetStream Stream，示例中包含创建逻辑。
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/piyongcai-liucai/filerotate"
)

func main() {
	// 连接 NATS，获取 JetStream 上下文
	nc, err := nats.Connect("nats://172.20.130.90:4222")
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		panic(err)
	}

	// 创建 Stream（仅需执行一次，生产环境建议由运维预先创建）
	if err := ensureStream(js); err != nil {
		panic(err)
	}

	// 创建 Writer，注入 JetStream 通知器
	writer, err := filerotate.New(filerotate.Config{
		FilePath:      "../log/app.log",
		MaxSizeMB:     1,
		MaxAgeDays:    7,
		CheckInterval: time.Second,
		NotifierFactory: func(commPath string, errorHandler func(error)) (filerotate.Notifier, error) {
			return filerotate.NewJetStreamNotifier(js, "filerotate.rotate", "FILEROTATE", errorHandler), nil
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

// ensureStream 创建 JetStream Stream，可配置集群参数。
func ensureStream(js nats.JetStreamContext) error {
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "FILEROTATE",
		Subjects:  []string{"filerotate.rotate"},
		Replicas:  1, // 单节点，集群可按需调整
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
