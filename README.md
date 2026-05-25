# filerotate

[![Go Reference](https://pkg.go.dev/badge/github.com/yourname/filerotate.svg)](https://pkg.go.dev/github.com/yourname/filerotate)
[![Go Report Card](https://goreportcard.com/badge/github.com/yourname/filerotate)](https://goreportcard.com/report/github.com/yourname/filerotate)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**多进程安全的文件轮转库**，提供两种实现以应对不同场景：

- **标准版** (`Writer`) – 自动发现所有写入进程，通过 Leader 选举与可插拔的跨平台通知机制实现精确协调。
- **Lite 版** (`LiteWriter`) – 极简设计，仅依赖本地写入计数和分布式锁选举，无进程间通信，通过时间间隔检查自动感知外部轮转。

## ✨ 特性

### 两种版本共有

- **多进程安全** – 轮转时通过**分布式锁**（文件锁、Valkey）保证唯一执行者。
- **按大小轮转** – 文件达到设定大小后自动重命名为带时间戳的备份文件并创建新文件，备份文件名精确到秒。
- **自动清理** – 可配置备份文件保留天数，过期自动删除。
- **零日志丢失** – 轮转期间其他进程的写入会完整保留在备份文件中。
- **高性能写入** – 常态写入零额外系统调用，锁竞争仅在轮转瞬间发生。

### 标准版额外优势

- **全自动进程发现** – 无需手动配置进程数，Leader 锁自动协调所有进程。
- **写入速率差异无关** – 慢进程通过通知保持同步。
- **Leader 死亡自动恢复** – 原 Leader 崩溃后其他进程自动重新选举。
- **可插拔通知与锁** – 提供 `Notifier` 和 `Locker` 接口，内置多种实现。

### Lite 版特点

- **无进程间通信** – 极低开销，仅依赖分布式锁和内存计数。
- **时间间隔检查** – 如果距离上次写入时间过长，自动检查文件是否已被轮转，避免慢进程长期持有旧文件句柄。
- **进程数需要预估** – 轮转阈值基于单个进程写入量，但时间检查可弥补慢进程的滞后。

## 🔧 平台支持

| 版本 | Linux | macOS | Windows | 说明 |
|------|-------|-------|---------|------|
| **标准版** (`Writer`) | ✅ | ✅ | ✅ | 默认使用 `NewLocalNotifier`，内部自动选择 Unix Socket 或 Windows 命名管道 |
| **Lite 版** (`LiteWriter`) | ✅ | ✅ | ✅ | 仅依赖跨平台文件锁 (`gofrs/flock`)，无平台特定代码 |

## 📦 内置组件

### 通知器（Notifier）

| 实现 | 构造函数 | 广播 | 持久化 | 说明 |
|------|----------|------|--------|------|
| 本地 IPC | `NewLocalNotifier(commPath)` | ✅ | ❌ | 默认，Unix Socket / 命名管道 |
| NATS 核心 | `NewNATSNotifier(conn, subject)` | ✅ | ❌ | 需要 NATS 服务 |
| JetStream | `NewJetStreamNotifier(js, subject)` | ✅ | ✅ | 临时消费者，Stream 外部创建 |
| Valkey | `NewValkeyNotifier(client, channel)` | ✅ | ❌ | 需要 Valkey/Redis |

### 分布式锁（Locker）—— 可选扩展

文件轮转本身是**本地文件系统操作**，在单机多进程部署时，**默认的文件锁（`NewFileLocker`）已经足够**，你无需关心 `Locker` 接口。`Locker` 抽象是为以下特殊场景设计的：

- **NFS 共享存储**：当多个主机挂载同一个 NFS 目录，且部署在不同主机上的应用实例都向该目录下的同一个日志文件写入时，文件锁可能不可靠（部分 NFS 实现不支持 `flock`）。此时可以使用 **Valkey 锁** 来跨主机协调轮转。
- **单元测试**：可以注入自定义实现来模拟锁行为，无需真实文件系统。

| 实现 | 构造函数 | 特色 | 适用场景 |
|------|----------|------|----------|
| 文件锁 | `NewFileLocker(lockPath)` | 本地互斥，默认 | 单机多进程 |
| Valkey 锁 | `NewValkeyLocker(clientOption, key, keyMajority, keyValidity)` | Redlock 算法，自动续期，多节点多数派 | 跨主机（如 NFS）、已有 Valkey |

> **重要**：如果你只在**单台机器**上运行多个进程（例如 GoFiber 的 `prefork` 模式、容器副本共享本地卷），那么你完全不需要分布式锁，默认的文件锁就是最简单、最快速的方案。`Locker` 接口仅作为可选扩展提供，不会增加你的使用负担。

## 🚀 快速开始

### 安装

```bash
go get github.com/yourname/filerotate
```

### 标准版示例（默认文件锁 + 本地通知器）

```go
writer, err := filerotate.New(filerotate.Config{
    FilePath:      "./app.log",
    MaxSizeMB:     10,
    MaxAgeDays:    7,
    CheckInterval: 5 * time.Second,
})
```

### Lite 版示例（默认文件锁，默认最大写入间隔 5 秒）

```go
writer, err := filerotate.NewLiteWriter(filerotate.LiteConfig{
    FilePath:        "./app.log",
    PerProcSizeMB:   25,
    MaxAgeDays:      7,
})
```

### 使用 Valkey 锁和通知器（跨主机 NFS）

```go
valkeyLocker, _ := filerotate.NewValkeyLocker(
    valkey.ClientOption{InitAddress: []string{"localhost:6379"}},
    "filerotate-lock", 1, 30*time.Second,
)

writer, _ := filerotate.New(filerotate.Config{
    FilePath: "/nfs/logs/app.log",
    MaxSizeMB: 10,
    LockerFactory: func(lockPath string) (filerotate.Locker, error) {
        return valkeyLocker, nil
    },
    NotifierFactory: func(commPath string) (filerotate.Notifier, error) {
        return filerotate.NewValkeyNotifier(client, "filerotate.rotate"), nil
    },
})
```

## 📂 项目结构

```
filerotate/
├── README.md
├── LICENSE
├── go.mod
├── writer.go
├── leader.go
├── notifier.go
├── notifier_local_unix.go
├── notifier_local_windows.go
├── notifier_nats.go
├── notifier_jetstream.go
├── notifier_valkey.go
├── locker.go
├── locker_file.go
├── locker_valkey.go
├── rotate.go
├── lite_writer.go
├── writer_test.go
├── lite_writer_test.go
├── notifier_test.go
├── notifier_local_test.go
├── notifier_nats_test.go
├── notifier_jetstream_test.go
├── notifier_valkey_test.go
├── locker_test.go
└── example/
    ├── main.go
    ├── lite_main.go
    ├── nats_main.go
    ├── jetstream_main.go
    └── valkey_main.go
```

## 🔧 配置说明

### 标准版 Config

```go
type Config struct {
    FilePath        string
    MaxSizeMB       int
    MaxAgeDays      int
    CheckInterval   time.Duration                              // 默认 5 秒
    LockerFactory   func(lockPath string) (Locker, error)      // 默认文件锁
    NotifierFactory func(commPath string) (Notifier, error)    // 默认本地 IPC
}
```

### Lite 版 LiteConfig

```go
type LiteConfig struct {
    FilePath         string
    PerProcSizeMB    int
    MaxAgeDays       int
    MaxWriteInterval time.Duration                              // 最大写入间隔，默认 5 秒
    LockerFactory    func(lockPath string) (Locker, error)      // 默认文件锁
}
```

## ❓ 选型指南

| 场景 | 通知器 | 锁 |
|------|--------|-----|
| 单机多进程（默认） | `NewLocalNotifier` | `NewFileLocker` |
| 跨主机 NFS 共享 | `NewNATSNotifier` 或 `NewValkeyNotifier` | `NewValkeyLocker` |
| 已有 NATS 基础设施 | `NewNATSNotifier` | 任意锁（推荐文件锁） |
| 需要持久化通知 | `NewJetStreamNotifier` | 任意锁 |