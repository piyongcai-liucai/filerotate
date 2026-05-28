# filerotate

[![Go Reference](https://pkg.go.dev/badge/github.com/piyongcai-liucai/filerotate.svg)](https://pkg.go.dev/github.com/piyongcai-liucai/filerotate)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**多进程安全的文件轮转库**，提供统一的 `Writer` 结构和两种工作模式：

- `New()` – 根据 `NotifierFactory` 自动选择模式：
  - `NotifierFactory` 为 nil（默认）→ **单机模式**，内置文件轮询 + 文件锁，开箱即用。
  - `NotifierFactory` 非 nil → **标准模式**，通过 Leader 选举与可插拔的进程间通知机制实现精确协调。
- `NewLocal()` – `New()` 的便捷封装，强制单机模式，忽略 `NotifierFactory`/`LockerFactory` 配置。

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

### 单机模式特点

- **无进程间通信** – 每个进程通过内置 polling goroutine 独立检测文件大小和 inode 变化。
- **统一阈值** – 所有进程共享同一 `MaxSizeMB` 阈值，无需预估每进程写入量。
- **信号去重** – 轮转失败时不会重复发送信号，直到当前处理完成后才允许下一轮检测。

## 平台支持

| 模式 | Linux | macOS | Windows | 说明 |
|------|-------|-------|---------|------|
| **单机模式** (`NewLocal()`) | ✅ | ✅ | ✅ | 默认，基于文件轮询 + 文件锁，无 Leader 选举 |
| **标准模式** (自定义通知器) | ✅ | ✅ | ✅ | 设置 NotifierFactory 后启用 Leader 选举 + 跨进程通知 |

## 快速开始

### 安装

```bash
go get github.com/piyongcai-liucai/filerotate
```

### 单机模式（默认，无进程间通信）

```go
// 方式一：New() 自动选择（NotifierFactory 为 nil → 单机模式）
writer, err := filerotate.New(filerotate.Config{
    FilePath:   "./app.log",
    MaxSizeMB:  100,
    MaxAgeDays: 7,
})
// 方式二：NewLocal 便捷函数（强制单机模式，忽略 LockerFactory/NotifierFactory）
writer, err := filerotate.NewLocal(filerotate.Config{
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
    NotifierFactory: func(errorHandler func(error)) (filerotate.Notifier, error) {
        return filerotate.NewValkeyNotifier(client, "filerotate:rotate", errorHandler), nil
    },
})
```

## 配置说明

### NotifierFactory 与模式选择

```go
type Config struct {
    FilePath        string                                          // 文件路径，所有进程必须相同
    MaxSizeMB       int                                             // 触发轮转的文件大小（MB），0 表示永不轮转
    MaxAgeDays      int                                             // 备份保留天数，0 为永久
    CheckInterval   time.Duration                                   // 检查间隔：默认 5s（标准），1s（单机）
    LockerFactory   func(lockPath string) (Locker, error)           // 自定义锁工厂，标准模式必填，单机模式忽略
    NotifierFactory func(errorHandler func(error)) (Notifier, error) // nil=单机模式，非nil=标准模式 + Leader选举
    ErrorHandler    func(error)                                     // 内部 goroutine 错误回调，默认打印 stderr
}
```

## 内置组件

### 通知器（Notifier）

| 实现 | 构造函数 | 适用场景 |
|------|----------|----------|
| 单机轮询 | 内置默认 | 基于文件轮询（inode + 大小检测），单机默认 |
| NATS 核心 | `NewNATSNotifier(conn, subject, errorHandler)` | Pub/Sub，需要 NATS 服务 |
| JetStream | `NewJetStreamNotifier(js, subject, errorHandler)` | 临时消费者，Stream 外部创建，消息持久化 |
| Valkey | `NewValkeyNotifier(client, channel, errorHandler)` | Pub/Sub，需要 Valkey/Redis |

### 分布式锁（Locker）

| 实现 | 构造函数 | 说明 |
|------|----------|------|
| 单机文件锁 | 内置 | 默认，基于 `gofrs/flock`，单机模式自动使用 |
| Valkey 锁 | `NewValkeyLocker(option, key)` | Redlock 算法，自动续期，适合跨主机 |

> 如果你只在单台机器上运行多个进程，默认的文件锁就是最简单、最快速的方案。`Locker` 接口仅作为可选扩展提供。

## 选型指南

| 场景 | 模式 | 通知器 | 锁 |
|------|------|--------|-----|
| 单机极简模式 | 单机模式 | 内置单机轮询 | 内置单机文件锁 |
| 网络文件系统 | 标准模式 | NATS / JetStream / Valkey | ValkeyLocker |

## 项目结构

```
filerotate/
├── writer.go                # Writer 结构 + 标准模式 Config/New
├── writer_local.go           # 单机模式便捷构造函数和自轮转循环
├── leader.go                # Leader 选举、文件监控、轮转触发
├── rotate.go                # 文件重命名、备份清理、时间戳解析
├── open_file_unix.go        # Unix: O_CREATE|O_APPEND|O_WRONLY
├── open_file_windows.go     # Windows: CreateFile + FILE_SHARE_DELETE
├── locker.go                # Locker 接口 + 工厂函数
├── notifier.go              # Notifier 接口 + 工厂函数
├── integration_test.go      # 多进程集成测试
├── internal/
│   ├── locker/
│   │   ├── local.go         # 单机文件锁（flock）
│   │   └── valkey.go        # Valkey 锁（Redlock）
│   └── notifier/
│       ├── local.go         # 单机轮询通知器（基于文件大小和 inode）
│       ├── nats.go          # NATS Pub/Sub 通知器
│       ├── jetstream.go     # JetStream 临时消费者通知器
│       └── valkey.go        # Valkey Pub/Sub 通知器
└── example/
    ├── local/               # 单机模式多进程示例
    ├── nats/                # NATS 通知器示例
    ├── jetstream/           # JetStream 通知器示例
    └── valkey/              # Valkey 通知器 + 锁示例
```

## 工作原理

### 标准模式

```
进程启动 → New()（NotifierFactory + LockerFactory 均非 nil）
  → 尝试获取 Leader 锁
  ├── 成功 → runLeader()
  │          ├── 启动 Notifier.Serve()（监听连接）
  │          ├── Connect() 自己，接收自己广播的命令
  │          └── 定时检查文件大小 → 超阈值 → doRotation() → Broadcast("ROTATE")
  └── 失败 → connectToLeader()
             ├── Connect() 连接到 Leader
             ├── handleCommands() 监听 ROTATE 命令
             └── 收到 ROTATE → 发送信号到 rotateCh → Write() 中 reopenFile()
```

### 单机模式

```
进程启动 → New() / NewLocal()
  ├── 创建 LocalNotifier（polling goroutine，定期 stat 文件）
  ├── 启动 runLocalLoop goroutine 监听轮转信号
  └── Write() 热路径:
       ├── select rotateCh（非阻塞检查）
       │   └── 有信号 → reopenFile()
       └── f.Write(p)（直接写入，无锁）

LocalNotifier 检测到 size 超阈值或 inode 变化 → 发送 ROTATE
  → runLocalLoop 收到 ROTATE → tryRotate()
      ├── TryLock()（非阻塞抢锁）
      ├── 抢到锁 → 二次确认文件大小 → doRotation()
      └── 没抢到（别人已轮转）→ 仅 reopenFile()
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
