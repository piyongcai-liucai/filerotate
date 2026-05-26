//go:build !windows

// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件实现 Unix 平台的文件打开。
package filerotate

import (
	"os"
)

// openFileAppend 以追加模式打开或创建文件。
// 使用 O_CREATE|O_APPEND|O_WRONLY 保证多进程安全写入。
// 调用者应确保文件所在目录已存在。
func openFileAppend(filePath string) (*os.File, error) {
	return os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
}
