# filerotate

[![Go Reference](https://pkg.go.dev/badge/github.com/piyongcai-liucai/filerotate.svg)](https://pkg.go.dev/github.com/piyongcai-liucai/filerotate)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**多进程安全的文件轮转库**，零外部依赖，开箱即用。

每个进程通过内置 polling goroutine 独立检查文件状态，通过文件锁（`flock`）协调轮转。适用于本地文件系统和 NFS 同目录挂载。

## 特性

- **多进程安全** – 文件锁协调轮转 + O_APPEND 内核级原子写入
- **按大小轮转** – 文件达到设定大小后自动重命名为带纳秒级时间戳的备份文件
- **自动清理** – 可配置备份文件保留天数，过期自动删除
- **零日志丢失** – 轮转期间其他进程的写入完整保留在备份文件中，下次写入自动重开文件
- **NFS 可用** – `os.Open` 二次确认绕过 NFS 属性缓存，`rename` 原子性保证并发安全
- **Windows 支持** – 使用 `FILE_SHARE_DELETE` 打开文件，允许其他进程在持有句柄时执行重命名

## 快速开始

### 安装

```bash
go get github.com/piyongcai-liucai/filerotate
```

### 使用

```go
writer, err := filerotate.New(filerotate.Config{
    FilePath:   "./app.log",
    MaxSizeMB:  100,
    MaxAgeDays: 7,
})
if err != nil {
    log.Fatal(err)
}
defer writer.Close()

log.SetOutput(writer)
```

## 配置说明

```go
type Config struct {
    FilePath      string        // 文件路径，所有并发进程必须相同
    MaxSizeMB     int           // 触发轮转的文件大小（MB），0 表示永不轮转
    MaxAgeDays    int           // 备份文件保留天数，0 为永久
    CheckInterval time.Duration // 文件检查间隔，默认 1s
    ErrorHandler  func(error)   // 内部 goroutine 错误回调，默认打印 stderr
}
```

## NFS 支持

NFS（同目录挂载）上可用，有三点需要注意：

1. **`os.Open` + `f.Stat()` 替代 `os.Stat`** — 利用 NFS close-to-open 一致性强制从服务端获取最新属性，避免属性缓存（3-60s）导致检测延迟
2. **`rename` 原子性** — NFS 上 `rename` 是原子操作，极端时序下多个主机同时 rename 也不会损坏数据
3. **flock 仅保证本机互斥** — 跨主机无效，无法用于多主机之间的轮转协调
4. **可能出现空备份文件** — 多主机可能同时判定超阈值并 rename，先到者成功后后来者可能 rename 前者刚创建的空文件，产生空备份，**数据不丢失**

## 项目结构

```
filerotate/
├── writer.go            # Writer、Config、New、Write、Close、poll、checkAndRotate
├── rotate.go            # 文件重命名、备份清理、时间戳解析
├── open_file_unix.go    # Unix: O_CREATE|O_APPEND|O_WRONLY
├── open_file_windows.go # Windows: CreateFile + FILE_SHARE_DELETE
├── *_test.go            # 单元测试 + 多进程集成测试
└── log/                 # 测试日志输出目录
```

## 工作原理

```
进程启动 → New()
  ├── 打开文件（O_APPEND），获取初始 FileInfo
  ├── 创建文件锁（flock）
  ├── 启动 poll goroutine
  └── Write() 热路径:
       ├── select rotateCh（非阻塞检查）
       │   └── 有信号 → reopenFile()
       └── f.Write(p)（直接写入，无锁）

poll → checkAndRotate()
  ├── TryLock()（非阻塞抢锁，没抢到说明别人在轮转，跳过）
  ├── 抢到锁 → os.Open + f.Stat（绕过 NFS 属性缓存）
  ├── 大小超阈值 → doFileRotation()（rename + 清理过期备份，3次重试）
  ├── inode 变化（别人轮转了）→ 发信号通知 Write() 重开文件
  └── 发信号到 rotateCh → Write() 收到后 reopenFile()
```

## 轮转文件命名

备份文件使用纳秒级精度时间戳，避免高频轮转时文件名冲突：

```
app.log.20240501_143025.123456789
```