//go:build windows

// Package filerotate 提供多进程安全的文件轮转功能。
//
// 本文件实现 Windows 平台的文件打开，使用 FILE_SHARE_DELETE
// 允许其他进程在文件打开期间执行重命名操作。
package filerotate

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// openFileAppend 以追加模式打开或创建文件。
// 使用 FILE_SHARE_DELETE 共享模式，允许其他进程在文件打开期间重命名该文件。
func openFileAppend(filePath string) (*os.File, error) {
	path, err := windows.UTF16PtrFromString(filePath)
	if err != nil {
		return nil, err
	}

	handle, err := windows.CreateFile(
		path,
		windows.FILE_APPEND_DATA|windows.SYNCHRONIZE, // 原子追加写入
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_ALWAYS,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("windows.CreateFile %s: %w", filePath, err)
	}

	return os.NewFile(uintptr(handle), filePath), nil
}
