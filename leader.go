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
// 如果 Leader 崩溃，其他进程会检测到并重新选举新 Leader。
func (w *Writer) runLeader() {
	// 启动通知器服务端（如 Unix Socket 监听、NATS 订阅等）
	// 服务端运行在独立的 goroutine 中，持续接受客户端连接
	go func() {
		if err := w.notifier.Serve(); err != nil {
			// 通知器服务端启动失败，报告错误
			w.reportError(fmt.Errorf("Leader Serve 失败: %w", err))
		}
	}()
	// 等待服务端就绪，确保客户端能成功连接
	time.Sleep(200 * time.Millisecond)

	// Leader 自己也作为客户端连接 Notifier
	// 这样当 Leader 执行轮转后广播 ROTATE 命令时，自己也能收到并重开文件
	cmdCh, err := w.notifier.Connect()
	if err != nil {
		w.reportError(fmt.Errorf("Leader 连接自己失败: %w", err))
		return
	}
	// 启动命令处理协程，监听来自 Notifier 的命令
	go w.handleCommands(cmdCh)

	// 定期检查文件大小
	ticker := time.NewTicker(w.checkInterval)
	defer ticker.Stop()
	for range ticker.C {
		// 获取当前文件信息
		fi, err := os.Stat(w.filePath)
		if err != nil {
			// 文件可能不存在或无法访问，报告错误后继续下一次检查
			w.reportError(fmt.Errorf("检查文件大小失败: %w", err))
			continue
		}

		// 判断文件大小是否达到轮转阈值
		if fi.Size() >= w.maxSize {
			// 执行轮转操作：重命名当前文件、创建新文件、清理旧备份
			if err := w.doRotation(); err != nil {
				w.reportError(fmt.Errorf("轮转执行失败: %w", err))
				continue
			}

			// 广播 ROTATE 命令，通知所有进程（包括自己）重开文件
			if err := w.notifier.Broadcast(CmdRotate); err != nil {
				w.reportError(fmt.Errorf("广播 ROTATE 失败: %w", err))
			}
		}
	}
}
