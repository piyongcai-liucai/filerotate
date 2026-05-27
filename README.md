# filerotate

[![Go Reference](https://pkg.go.dev/badge/github.com/piyongcai-liucai/filerotate.svg)](https://pkg.go.dev/github.com/piyongcai-liucai/filerotate)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**多进程安全的文件轮转库**，统一的 `Writer` 结构支持两种模式：

- **标准模式** (`New()`) – 自动发现所有写入进程，通过 Leader 选举与可插拔的进程间通知机制实现精确协调。
- **Lite 模式** (`NewLite()`) – 无 IPC，每个进程通过内置 polling goroutine 独立检查文件状态，通过分布式锁协调轮转。

## 特性

### 两种模式共有

- **多进程安全** – 轮转时通过分布式锁（文件锁、Valkey Redlock）保证唯一执行者。
- **按大小轮转** – 文件达到设定大小后自动重命名为带纳秒级时间戳的备份文件。
- **自动清理** – 可配置备份文件保留天数，过期自动删除。
- **零日志丢失** – 轮转期间其他进程的写入会完整保留在备份文件中，下次写入自动重开文件。
- **Write() 无锁** – 热路径不加锁，依赖 `O_APPEND` 内核级原子追加，高并发下完全并行写入。
- **Windows 支持** – 使用 `FILE_SHARE_DELETE` 打开文件，允许其他进程在持有句柄时执行重命名。

### 标准模式额外优势

- **全自动进程发现** – 无需手动配置进程数，Leader 锁自动协调所有进程。
- **Leader 死亡自动恢复** – 原 Leader 崩溃后其他进程自动重新选举。
- **可插拔通知与锁** – 提供 `Notifier` 和 `Locker` 接口，内置多种实现。

### Lite 模式特点

- **无进程间通信** – 每个进程通过内置 polling goroutine 独立检测文件大小和 inode 变化。
- **统一阈值** – 所有进程共享同一 `MaxSizeMB` 阈值，无需预估每进程写入量。
- **信号去重** – 轮转失败时不会重复发送信号，直到当前处理完成后才允许下一轮检测。

## 平台支持

| 模式 | Linux | macOS | Windows | 说明 |
|------|-------|-------|---------|------|
| **标准模式** (`New()`) | ✅ | ✅ | ✅ | 默认 Unix Socket / Windows 命名管道 |
| **Lite 模式** (`NewLite()`) | ✅ | ✅ | ✅ | 仅依赖跨平台文件锁，无平台特定代码 |

## 快速开始

### 安装

```bash
go get github.com/piyongcai-liucai/filerotate
```

### 标准模式（Leader 选举 + 本地 IPC）

```go
writer, err := filerotate.New(filerotate.Config{
    FilePath:      "./app.log",
    MaxSizeMB:     10,
    MaxAgeDays:    7,
    CheckInterval: 5 * time.Second,
    ErrorHandler:  func(err error) { log.Printf("filerotate: %v", err) },
})
if err != nil {
    log.Fatal(err)
}
defer writer.Close()

// 直接用作 log.Logger 的输出
log.SetOutput(writer)
```

### Lite 模式（无 IPC，内置轮询 + 分布式锁）

```go
writer, err := filerotate.NewLite(filerotate.LiteConfig{
    FilePath:     "./app.log",
    MaxSizeMB:    100, // 文件总大小达到 100 MB 时触发轮转
    MaxAgeDays:   7,
    PollInterval: 1 * time.Second,
    ErrorHandler: func(err error) { log.Printf("filerotate: %v", err) },
})
if err != nil {
    log.Fatal(err)
}
defer writer.Close()

log.SetOutput(writer)
```

### 使用 Valkey 通知器和锁（跨主机 NFS）

```go
client, _ := valkey.NewClient(valkey.ClientOption{
    InitAddress: []string{"localhost:6379"},
})

locker, _ := filerotate.NewValkeyLocker(
    valkeylock.LockerOption{
        ClientOption: valkey.ClientOption{InitAddress: []string{"localhost:6379"}},
        KeyMajority:  1,
        KeyValidity:  30 * time.Second,
    },
    "filerotate-lock",
)

writer, _ := filerotate.New(filerotate.Config{
    FilePath:  "/nfs/logs/app.log",
    MaxSizeMB: 10,
    LockerFactory: func(lockPath string) (filerotate.Locker, error) {
        return locker, nil
    },
    NotifierFactory: func(commPath string, errorHandler func(error)) (filerotate.Notifier, error) {
        return filerotate.NewValkeyNotifier(client, "filerotate:rotate", errorHandler), nil
    },
})
```

## 配置说明

### 标准模式 Config

```go
type Config struct {
    FilePath        string                                          // 文件路径，所有进程必须相同
    MaxSizeMB       int                                             // 触发轮转的文件大小（MB），0 表示永不轮转
    MaxAgeDays      int                                             // 备份保留天数，0 为永久
    CheckInterval   time.Duration                                   // Leader 检查文件大小的间隔，默认 5s
    LockerFactory   func(lockPath string) (Locker, error)           // 自定义锁工厂，默认文件锁
    NotifierFactory func(commPath string, errorHandler func(error)) (Notifier, error) // 自定义通知器工厂，默认本地 IPC
    ErrorHandler    func(error)                                     // 内部 goroutine 错误回调，默认打印 stderr
}
```

### Lite 模式 LiteConfig

```go
type LiteConfig struct {
    FilePath     string        // 文件路径，所有进程必须相同
    MaxSizeMB    int           // 触发轮转的文件总大小（MB），0 表示永不轮转
    MaxAgeDays   int           // 备份保留天数，0 为永久
    PollInterval time.Duration // 轮询文件状态的间隔，默认 1s
    ErrorHandler func(error)   // 内部 goroutine 错误回调，默认打印 stderr
}
```

## 内置组件

### 通知器（Notifier）

| 实现 | 构造函数 | 适用场景 |
|------|----------|----------|
| 本地 IPC | `NewLocalNotifier(commPath, errorHandler)` | 默认，Unix Socket / Windows 命名管道 |
| NATS 核心 | `NewNATSNotifier(conn, subject, errorHandler)` | Pub/Sub，需要 NATS 服务 |
| JetStream | `NewJetStreamNotifier(js, subject, streamName, errorHandler)` | 临时消费者，Stream 外部创建，消息持久化 |
| Valkey | `NewValkeyNotifier(client, channel, errorHandler)` | Pub/Sub，需要 Valkey/Redis |

### 分布式锁（Locker）

| 实现 | 构造函数 | 说明 |
|------|----------|------|
| 文件锁 | `NewFileLocker(lockPath)` | 默认，基于 `gofrs/flock`，单机多进程 |
| Valkey 锁 | `NewValkeyLocker(option, key)` | Redlock 算法，自动续期，适合跨主机 |

> 如果你只在单台机器上运行多个进程，默认的文件锁就是最简单、最快速的方案。`Locker` 接口仅作为可选扩展提供。

## 选型指南

| 场景 | 模式 | 通知器 | 锁 |
|------|------|--------|-----|
| 单机多进程 | 标准模式 | `NewLocalNotifier` | `NewFileLocker` |
| 单机，极简 | Lite 模式 | 内置轮询 | `NewFileLocker` |
| 跨主机 NFS | 标准模式 | `NewValkeyNotifier` 或 `NewNATSNotifier` | `NewValkeyLocker` |
| 已有 NATS | 标准模式 | `NewNATSNotifier` | `NewFileLocker` |
| 需要持久化通知 | 标准模式 | `NewJetStreamNotifier` | 任意 |

## 项目结构

```
filerotate/
├── writer.go                # Writer 结构 + 标准模式 Config/New
├── writer_lite.go           # Lite 模式 LiteConfig/NewLite
├── leader.go                # Leader 选举、文件监控、轮转触发
├── rotate.go                # 文件重命名、备份清理、时间戳解析
├── open_file_unix.go        # Unix: O_CREATE|O_APPEND|O_WRONLY
├── open_file_windows.go     # Windows: CreateFile + FILE_SHARE_DELETE
├── locker.go                # Locker 接口 + 工厂函数
├── notifier.go              # Notifier 接口 + 工厂函数
├── integration_test.go      # 多进程集成测试
├── internal/
│   ├── locker/
│   │   ├── file.go          # 文件锁（flock）
│   │   └── valkey.go        # Valkey 锁（Redlock）
│   └── notifier/
│       ├── default.go       # 轮询通知器（Lite 模式内置）
│       ├── local_unix.go    # Unix Socket 通知器
│       ├── local_windows.go # Windows 命名管道通知器
│       ├── nats.go          # NATS Pub/Sub 通知器
│       ├── jetstream.go     # JetStream 临时消费者通知器
│       └── valkey.go        # Valkey Pub/Sub 通知器
└── example/
    ├── basic/               # 标准模式多进程示例（本地 IPC）
    ├── lite/                # Lite 模式多进程示例
    ├── nats/                # NATS 通知器示例
    ├── jetstream/           # JetStream 通知器示例
    └── valkey/              # Valkey 通知器 + 锁示例
```

## 工作原理

### 标准模式

```
进程启动 → 尝试获取 Leader 锁
  ├── 成功 → runLeader()
  │          ├── 启动 Notifier.Serve()（监听连接）
  │          ├── Connect() 自己，接收自己广播的命令
  │          └── 定时检查文件大小 → 超阈值 → doRotation() → Broadcast("ROTATE")
  └── 失败 → connectToLeader()
             ├── Connect() 连接到 Leader
             ├── handleCommands() 监听 ROTATE 命令
             └── 收到 ROTATE → 发送信号到 rotateCh → Write() 中 reopenFile()
```

### Lite 模式

```
进程启动 → NewLite()
  ├── 创建 DefaultNotifier（polling goroutine，定期 stat 文件）
  ├── 启动 handleCommands goroutine 监听轮转信号
  └── Write() 热路径:
       ├── select rotateCh（非阻塞检查）
       │   └── 有信号 → rotateIfNeededLite()
       │       ├── TryLock()（非阻塞抢锁）
       │       ├── 抢到锁 → 二次确认文件大小 → doRotationLite()
       │       └── 没抢到 → reopenFile()（别人已轮转，切到新文件）
       └── f.Write(p)（直接写入，无锁）
```

## 轮转文件命名

备份文件使用纳秒级精度时间戳，避免高频轮转时文件名冲突：

```
app.log.20240501_143025.123456789
```

旧格式（秒级精度）的备份文件同样支持识别和清理：

```
app.log.20240501_143025
```
