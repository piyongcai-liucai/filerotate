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
// openFileAppend 使用 O_CREATE，会在下次写入时自动创建文件，因此此处无需单独 os.Create。
//
// 参数:
//   - filePath: 要轮转的文件路径
//   - maxAgeDays: 备份文件保留天数，0 表示不清理
//
// 返回 nil 表示成功。
func doFileRotation(filePath string, maxAgeDays int) error {
	// 使用纳秒级精度的时间戳，避免同一秒内多次轮转导致备份文件名冲突
	now := time.Now()
	backupName := filePath + "." + now.Format("20060102_150405.000000000")

	// Windows 下文件关闭后句柄可能延迟释放，重试 3 次以应对
	var err error
	for i := 0; i < 3; i++ {
		err = os.Rename(filePath, backupName)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	// 如果配置了保留天数，清理过期备份
	if maxAgeDays > 0 {
		cleanOldBackups(filePath, maxAgeDays)
	}
	return nil
}

// cleanOldBackups 删除超过 maxAgeDays 天的备份文件。
//
// 备份文件命名格式：<base>.<timestamp>，timestamp 格式为 "20060102_150405"（旧格式）
// 或 "20060102_150405.000000000"（新格式，纳秒精度）。
// 同时检查文件修改时间和文件名中的时间戳，任一过期则删除。
func cleanOldBackups(filePath string, maxAgeDays int) {
	dir := filepath.Dir(filePath)
	base := filepath.Base(filePath)

	matches, err := filepath.Glob(filepath.Join(dir, base+".*"))
	if err != nil {
		return
	}
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
		ts := parseTimestamp(ext)
		if ts.IsZero() {
			continue
		}

		fi, err := os.Stat(path)
		if err != nil {
			continue
		}

		// 文件修改时间或文件名时间任一过期，则删除
		if fi.ModTime().Before(cutoff) || ts.Before(cutoff) {
			os.Remove(path)
		}
	}
}

// parseTimestamp 从备份文件扩展名中解析时间戳。
// 支持两种格式：
//   - 旧格式: "20060102_150405" (15字符)
//   - 新格式: "20060102_150405.000000000" (25字符，纳秒精度)
//
// 返回零值 time.Time 表示解析失败。
func parseTimestamp(ext string) time.Time {
	switch len(ext) {
	case 25: // 新格式：纳秒精度
		if ext[8] != '_' || ext[15] != '.' {
			return time.Time{}
		}
		t, err := time.Parse("20060102_150405.000000000", ext)
		if err != nil {
			return time.Time{}
		}
		return t
	case 15: // 旧格式：秒精度
		if ext[8] != '_' {
			return time.Time{}
		}
		t, err := time.Parse("20060102_150405", ext)
		if err != nil {
			return time.Time{}
		}
		return t
	default:
		return time.Time{}
	}
}

// openFileAppend 定义在平台特定文件中：
//
//	open_file_windows.go - Windows: 使用 FILE_SHARE_DELETE 允许重命名
//	open_file_unix.go    - Unix:    使用 O_CREATE|O_APPEND|O_WRONLY
