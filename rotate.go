// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件包含文件轮转的核心操作：重命名、创建新文件、清理过期备份。
package filerotate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// doFileRotation 执行文件重命名、创建新文件、清理旧备份。
// 调用者应确保已持有锁，避免并发轮转。
//
// 参数:
//   - filePath: 要轮转的文件路径
//   - maxAgeDays: 备份文件保留天数，0 表示不清理
//
// 返回 nil 表示成功。
func doFileRotation(filePath string, maxAgeDays int) error {
	// 生成带时间戳的备份文件名，精确到秒，避免重名
	backupName := filePath + "." + time.Now().Format("20060102_150405")
	if err := os.Rename(filePath, backupName); err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	// 创建新的空文件，供后续写入
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("create new: %w", err)
	}
	f.Close()

	// 如果配置了保留天数，清理过期备份
	if maxAgeDays > 0 {
		cleanOldBackups(filePath, maxAgeDays)
	}
	return nil
}

// cleanOldBackups 删除超过 maxAgeDays 天的备份文件。
//
// 备份文件命名格式：<base>.<timestamp>，其中 timestamp 格式为 "20060102_150405"。
// 同时检查文件修改时间和文件名中的时间戳，任一过期则删除。
//
// 参数:
//   - filePath: 原始文件路径
//   - maxAgeDays: 最大保留天数
func cleanOldBackups(filePath string, maxAgeDays int) {
	dir := filepath.Dir(filePath)
	base := filepath.Base(filePath)

	// 匹配所有该文件的备份
	matches, _ := filepath.Glob(filepath.Join(dir, base+".*"))
	cutoff := time.Now().AddDate(0, 0, -maxAgeDays)

	for _, path := range matches {
		// 跳过当前文件、锁文件、Socket 文件
		if path == filePath ||
			strings.HasSuffix(path, ".lock") ||
			strings.HasSuffix(path, ".sock") {
			continue
		}

		// 提取文件名中的时间戳部分
		ext := strings.TrimPrefix(filepath.Base(path), base+".")
		if len(ext) != 15 || ext[8] != '_' {
			continue
		}
		t, err := time.Parse("20060102_150405", ext)
		if err != nil {
			continue
		}

		fi, err := os.Stat(path)
		if err != nil {
			continue
		}

		// 文件修改时间或文件名时间任一过期，则删除
		if fi.ModTime().Before(cutoff) || t.Before(cutoff) {
			os.Remove(path)
		}
	}
}

// openFileAppend 以追加模式打开或创建文件。
// 如果文件所在目录不存在，会自动创建目录（包括所有父目录）。
// 使用 O_CREATE|O_APPEND|O_WRONLY 保证多进程安全写入。
//
// 参数:
//   - filePath: 文件路径
//
// 返回:
//   - *os.File: 文件句柄
//   - error: 错误信息
func openFileAppend(filePath string) (*os.File, error) {
	// 自动创建目录（如果不存在）
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("创建目录失败 %s: %w", dir, err)
	}

	return os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
}
