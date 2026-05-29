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

// doFileRotation 执行文件重命名和过期备份清理。
//
// rename 前二次确认文件大小，避免 NFS 跨主机场景下对已被其他主机轮转后的
// 新文件（空文件）再次 rename，产生空备份。
//
// 参数:
//   - filePath: 要轮转的文件路径
//   - maxSize: 轮转阈值（字节），用于二次确认
//   - maxAgeDays: 备份文件保留天数，0 表示不清理
//
// 返回 nil 表示成功或无需轮转（文件已被其他主机处理）。
func doFileRotation(filePath string, maxSize int64, maxAgeDays int) error {
	for i := range 3 {
		if err := tryRotate(filePath, maxSize); err == nil {
			break
		} else if i == 2 {
			return fmt.Errorf("rotation: %w", err)
		}

		time.Sleep(10 * time.Millisecond)
	}

	if maxAgeDays > 0 {
		cleanOldBackups(filePath, maxAgeDays)
	}
	return nil
}

// tryRotate 尝试执行一次文件轮转。
// 返回 nil 表示成功或无需轮转（文件已被其他主机处理）。
func tryRotate(filePath string, maxSize int64) error {
	f, err := os.Open(filePath)
	if os.IsNotExist(err) {
		return nil // 已被其他主机 rename
	} else if err != nil {
		return err
	}

	fi, err := f.Stat()
	f.Close()

	if err != nil {
		return err
	} else if maxSize > 0 && fi.Size() < maxSize {
		return nil // 已是其他主机创建的新文件
	}

	backupName := filePath + "." + time.Now().Format("20060102_150405.000000000")
	return os.Rename(filePath, backupName)
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
