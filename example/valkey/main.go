// 使用 Valkey 锁和 Valkey 通知器的标准版示例。
// 适用于跨主机 NFS 共享存储场景。
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/piyongcai-liucai/filerotate"
)

func main() {
	// 创建 Valkey 客户端
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{"localhost:6379"},
	})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// 创建 Valkey 分布式锁
	valkeyLocker, err := filerotate.NewValkeyLocker(
		valkey.ClientOption{InitAddress: []string{"localhost:6379"}},
		"filerotate-lock", // 锁名称
		1,                 // 单节点 KeyMajority=1
		30*time.Second,    // 锁键有效期
	)
	if err != nil {
		panic(err)
	}
	defer valkeyLocker.Close()

	// 创建 Writer，注入 Valkey 锁和通知器
	writer, err := filerotate.New(filerotate.Config{
		FilePath:   "../log/app.log", // NFS 共享路径
		MaxSizeMB:  1,
		MaxAgeDays: 7,
		LockerFactory: func(lockPath string) (filerotate.Locker, error) {
			return valkeyLocker, nil
		},
		CheckInterval: time.Second,
		NotifierFactory: func(commPath string, errorHandler func(error)) (filerotate.Notifier, error) {
			return filerotate.NewValkeyNotifier(client, "filerotate.rotate", errorHandler), nil
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
