// Package common 提供示例程序共享的辅助函数。
package common

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// MonitorRotations 定期检查备份文件数量，达到上限后关闭 done channel。
func MonitorRotations(filePath string, max int, done chan<- struct{}) {
	base := filepath.Base(filePath)
	for {
		count := CountBackups(filePath, base)
		fmt.Printf("\r[监控] 当前备份数: %d/%d", count, max)
		if count >= max {
			fmt.Printf("\n[监控] 已达到 %d 次轮转，通知进程退出\n", max)
			close(done)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// CountBackups 统计符合轮转备份命名规范的文件数量。
func CountBackups(filePath, base string) int {
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
