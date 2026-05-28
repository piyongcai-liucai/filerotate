// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件实现 Leader 选举后的主循环逻辑：监控文件大小、触发轮转。
package filerotate

import (
	"fmt"
	"os"
	"time"
)

// runLeader Leader 主循环：监控文件大小、执行轮转、广播通知。
//
// Serve 已在 New() 中调用，这里只需 Connect 并启动 ticker。
// 通过 w.done 通道接收退出信号，Close 时安全退出。
func (w *Writer) runLeader() {
	defer w.wg.Done()
	defer w.leaderLocker.Unlock()

	cmdCh, err := w.notifier.Connect()
	if err != nil {
		w.reportError(fmt.Errorf("Leader 连接失败: %w", err))
		return
	}

	// 启动命令处理协程，监听来自 Notifier 的命令
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.handleCommands(cmdCh)
	}()

	// 记录当前文件信息，用于检测外部轮转（inode 变化）
	fi, err := os.Stat(w.filePath)
	if err != nil {
		w.reportError(fmt.Errorf("Leader 初始化文件状态检查失败: %w", err))
		return
	}
	w.lastLeaderFileInfo = fi

	// 定期检查文件大小和外部轮转
	ticker := time.NewTicker(w.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.done:
			return
		case <-ticker.C:
			fi, err := os.Stat(w.filePath)
			if err != nil {
				w.reportError(fmt.Errorf("检查文件状态失败: %w", err))
				continue
			}

			if !os.SameFile(w.lastLeaderFileInfo, fi) {
				if err := w.reopenFile(); err != nil {
					w.reportError(fmt.Errorf("外部轮转后重开文件失败: %w", err))
					continue
				}
				w.broadcastRotate()
			} else if w.maxSize > 0 && fi.Size() >= w.maxSize {
				if w.tryRotate() {
					w.broadcastRotate()
					if newFi, err := os.Stat(w.filePath); err == nil {
						fi = newFi
					}
				}
			}

			w.lastLeaderFileInfo = fi
		}
	}
}
