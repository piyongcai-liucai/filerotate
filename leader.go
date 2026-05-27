// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件实现 Leader 选举后的主循环逻辑：启动通知器服务、监控文件大小、触发轮转。
package filerotate

import (
	"fmt"
	"os"
	"time"
)

// runLeader Leader 主循环：启动通知器服务、监控文件大小、触发轮转。
//
// Leader 负责:
//   - 启动 Notifier 服务端，等待其他进程（客户端）连接
//   - 自己也作为客户端连接 Notifier，以便接收自己广播的轮转命令
//   - 定期检查文件大小
//   - 当文件大小超过阈值时，执行轮转并广播 ROTATE 命令通知所有进程
//
// 此方法以 goroutine 方式运行，通常在 Writer.New 中被调用。
// 通过 w.done 通道接收退出信号，Close 时安全退出。
func (w *Writer) runLeader() {
	defer w.wg.Done()
	defer w.leaderLocker.Unlock()

	// 启动通知器服务端（如 Unix Socket 监听、命名管道监听）
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if err := w.notifier.Serve(); err != nil {
			w.reportError(fmt.Errorf("Leader Serve 失败: %w", err))
		}
	}()

	// 等待服务端就绪，用重试机制代替固定延迟
	var cmdCh <-chan string
	var err error
	for i := 0; i < 10; i++ {
		select {
		case <-w.done:
			return
		case <-time.After(50 * time.Millisecond):
		}
		cmdCh, err = w.notifier.Connect()
		if err == nil {
			break
		}
	}
	if err != nil {
		w.reportError(fmt.Errorf("Leader 连接自己失败: %w", err))
		return
	}

	// 启动命令处理协程，监听来自 Notifier 的命令
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.handleCommands(cmdCh)
	}()

	// 定期检查文件大小
	ticker := time.NewTicker(w.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.done:
			return
		case <-ticker.C:
			fi, err := os.Stat(w.filePath)
			if err != nil {
				w.reportError(fmt.Errorf("检查文件大小失败: %w", err))
				continue
			}

			if w.maxSize > 0 && fi.Size() >= w.maxSize {
				if err := w.doRotation(); err != nil {
					w.reportError(fmt.Errorf("轮转执行失败: %w", err))
					continue
				}

				if err := w.notifier.Broadcast(CmdRotate); err != nil {
					w.reportError(fmt.Errorf("广播 ROTATE 失败: %w", err))
				}
			}
		}
	}
}
