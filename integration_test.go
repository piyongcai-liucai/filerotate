// integration_test.go
// 多进程集成测试，通过 os/exec 启动子进程验证跨进程协调。
// 使用 TestMain 模式：设置 FILEROTATE_CHILD=1 环境变量标识子进程，
// 子进程执行指定角色函数后退出，主进程验证文件内容。
//
// 运行方式：
//
//	go test -run 'TestMultiProc'        # 仅运行多进程集成测试
//	go test -short ./...                 # 跳过集成测试（含 -short 标志）
package filerotate

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// ========== TestMain：子进程路由 ==========

func TestMain(m *testing.M) {
	if os.Getenv("FILEROTATE_CHILD") == "1" {
		runChildProcess()
		return
	}
	os.Exit(m.Run())
}

func runChildProcess() {
	role := os.Getenv("FILEROTATE_ROLE")
	filePath := os.Getenv("FILEROTATE_FILE")

	var err error
	switch role {
	case "writer_basic":
		err = childWriterBasic(filePath)
	case "writer_rotate":
		err = childWriterRotate(filePath)
	case "writer_failover_leader":
		err = childWriterFailoverLeader(filePath)
	case "writer_failover_follower":
		err = childWriterFailoverFollower(filePath)
	case "lite_basic":
		err = childLiteBasic(filePath)
	case "lite_rotate":
		err = childLiteRotate(filePath)
	default:
		fmt.Fprintf(os.Stderr, "unknown child role: %s", role)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "child error: %v", err)
		os.Exit(1)
	}
	fmt.Println("OK")
	os.Exit(0)
}

// ========== 子进程辅助函数 ==========

// executable returns the path to the test binary, failing the test if unavailable.
func executable(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("获取可执行文件路径失败: %v", err)
	}
	return exe
}

// runChild starts a child process and waits for it. Returns its combined output.
func runChild(role, filePath string, timeout time.Duration, extraEnv ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("获取可执行文件路径: %w", err)
	}

	cmd := exec.CommandContext(ctx, exe)
	cmd.Env = append(os.Environ(),
		"FILEROTATE_CHILD=1",
		"FILEROTATE_ROLE="+role,
		"FILEROTATE_FILE="+filePath,
	)
	cmd.Env = append(cmd.Env, extraEnv...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("%v\n%s", err, string(output))
	}
	return string(output), nil
}

// runTwoChildren starts two child processes concurrently and waits for both.
func runTwoChildren(t *testing.T, role, path string, timeout time.Duration) {
	t.Helper()

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := runChild(role, path, timeout)
			if err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
}

// cleanupSocket removes the Unix socket file associated with a log file path.
func cleanupSocket(t *testing.T, filePath string) {
	t.Helper()
	// 计算与 Writer.New 中相同的 hash
	hash := sha256.Sum256([]byte(filePath))
	sockName := fmt.Sprintf("filerotate_%x", hash[:8])
	os.Remove(sockName)
}

// cleanupIntegration removes log files, backups, lock files, and socket files.
func cleanupIntegration(t *testing.T, path string) {
	t.Helper()
	os.Remove(path)
	backups, _ := filepath.Glob(path + ".2*")
	for _, b := range backups {
		os.Remove(b)
	}
	os.Remove(path + ".lock")
	os.Remove(path + ".leader.lock")
	cleanupSocket(t, path)
}

// countLinesInFile 统计文件中非空行数。
func countLinesInFile(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(data))
	if s == "" {
		return 0
	}
	return len(strings.Split(s, "\n"))
}

// ========== 子进程角色实现 ==========

// childWriterBasic 创建标准 Writer，写入 100 行 PID 标记后退出。
func childWriterBasic(filePath string) error {
	w, err := New(Config{
		FilePath:     filePath,
		MaxSizeMB:    100,
		MaxAgeDays:   0,
		ErrorHandler: func(_ error) {},
	})
	if err != nil {
		return err
	}
	defer w.Close()

	marker := fmt.Sprintf("[PID %d]\n", os.Getpid())
	for i := 0; i < 100; i++ {
		if _, err := w.Write([]byte(marker)); err != nil {
			return err
		}
	}
	return nil
}

// childWriterRotate 创建标准 Writer（小 maxSize），分批写入触发轮转。
func childWriterRotate(filePath string) error {
	w, err := New(Config{
		FilePath:      filePath,
		MaxSizeMB:     0,
		MaxAgeDays:    0,
		CheckInterval: 100 * time.Millisecond,
		ErrorHandler:  func(_ error) {},
	})
	if err != nil {
		return err
	}
	defer w.Close()

	w.maxSize = 2048 // 覆盖为小阈值，确保多进程写入触发轮转

	chunk := strings.Repeat("x", 256)
	// 分批写入，每批之间 sleep 给 Leader 时间检测文件大小
	for i := 0; i < 30; i++ {
		line := fmt.Sprintf("[%d] chunk %d: %s\n", os.Getpid(), i, chunk)
		if _, err := w.Write([]byte(line)); err != nil {
			return err
		}
		time.Sleep(80 * time.Millisecond)
	}
	return nil
}

// childWriterFailoverLeader 作为 Leader 写入标记后短暂存活，然后退出模拟崩溃。
func childWriterFailoverLeader(filePath string) error {
	w, err := New(Config{
		FilePath:     filePath,
		MaxSizeMB:    100,
		MaxAgeDays:   0,
		ErrorHandler: func(_ error) {},
	})
	if err != nil {
		return err
	}
	defer w.Close()

	marker := fmt.Sprintf("[LEADER PID %d]\n", os.Getpid())
	for i := 0; i < 5; i++ {
		if _, err := w.Write([]byte(marker)); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

// childWriterFailoverFollower 作为 Follower 写入较长时间，
// 覆盖 Leader 退出前后的时段，验证故障转移。
func childWriterFailoverFollower(filePath string) error {
	w, err := New(Config{
		FilePath:     filePath,
		MaxSizeMB:    100,
		MaxAgeDays:   0,
		ErrorHandler: func(_ error) {},
	})
	if err != nil {
		return err
	}
	defer w.Close()

	marker := fmt.Sprintf("[FOLLOWER PID %d]\n", os.Getpid())
	for i := 0; i < 40; i++ {
		if _, err := w.Write([]byte(marker)); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

// childLiteBasic 创建 LiteWriter，写入 100 行后退出。
func childLiteBasic(filePath string) error {
	w, err := NewLiteWriter(LiteConfig{
		FilePath:      filePath,
		PerProcSizeMB: 100,
		MaxAgeDays:    0,
	})
	if err != nil {
		return err
	}
	defer w.Close()

	marker := fmt.Sprintf("[PID %d]\n", os.Getpid())
	for i := 0; i < 100; i++ {
		if _, err := w.Write([]byte(marker)); err != nil {
			return err
		}
	}
	return nil
}

// childLiteRotate 创建 LiteWriter（小 perProcSize），写入触发轮转。
func childLiteRotate(filePath string) error {
	w, err := NewLiteWriter(LiteConfig{
		FilePath:      filePath,
		PerProcSizeMB: 0,
		MaxAgeDays:    0,
	})
	if err != nil {
		return err
	}
	defer w.Close()

	w.perProcSize = 2048

	chunk := strings.Repeat("z", 256)
	for i := 0; i < 30; i++ {
		line := fmt.Sprintf("[%d] chunk %d: %s\n", os.Getpid(), i, chunk)
		if _, err := w.Write([]byte(line)); err != nil {
			return err
		}
			time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// ========== 标准版 Writer 多进程测试 ==========

// TestMultiProcWriterBasic 验证 2 个进程可并发写入同一文件，数据不丢失。
func TestMultiProcWriterBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试 (short 模式)")
	}
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestMultiProcWriterBasic.log")
	defer cleanupIntegration(t, path)

	runTwoChildren(t, "writer_basic", path, 30*time.Second)

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 200 {
		t.Errorf("期望 200 行，实际 %d 行 (每个进程写 100 行)", len(lines))
	}
	for _, line := range lines {
		if !strings.HasPrefix(line, "[PID ") || !strings.HasSuffix(line, "]") {
			t.Errorf("意外行内容: %q", line)
		}
	}
}

// TestMultiProcWriterRotation 验证多进程写入可触发轮转，且数据完整保留。
func TestMultiProcWriterRotation(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试 (short 模式)")
	}
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestMultiProcWriterRotation.log")
	defer cleanupIntegration(t, path)

	runTwoChildren(t, "writer_rotate", path, 60*time.Second)

	// 验证存在备份文件
	base := filepath.Base(path)
	matches, _ := filepath.Glob(path + ".*")
	backupCount := 0
	for _, m := range matches {
		if isBackupFile(base, filepath.Base(m)) {
			backupCount++
		}
	}
	if backupCount == 0 {
		t.Fatal("轮转未发生，未找到备份文件")
	}
	t.Logf("找到 %d 个备份文件", backupCount)

	// 统计所有文件（当前 + 备份）中的总行数，验证数据不丢失
	totalLines := countLinesInFile(path)
	for _, m := range matches {
		if isBackupFile(base, filepath.Base(m)) {
			totalLines += countLinesInFile(m)
		}
	}
	if totalLines != 60 {
		t.Errorf("期望总共 60 行（2 进程 × 30 条），实际 %d 行", totalLines)
	}
}

// TestMultiProcWriterFailover 验证 Leader 退出后 Follower 可继续正常写入。
func TestMultiProcWriterFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试 (short 模式)")
	}
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestMultiProcWriterFailover.log")
	defer cleanupIntegration(t, path)

	// 先启动 Leader
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		out, err := runChild("writer_failover_leader", path, 15*time.Second)
		if err != nil {
			// 接受失败（Leader 正常退出不算错误，但 exec 可能报信号相关错误）
			t.Logf("Leader 输出: %s", out)
		}
	}()

	// 等待 Leader 初始化完成（大约需要 0.5 秒）
	time.Sleep(500 * time.Millisecond)

	// 启动 Follower（Leader 存活时连接，Leader 退出后继续写）
	followerDone := make(chan struct{})
	var followerOutput string
	var followerErr error
	go func() {
		defer close(followerDone)
		followerOutput, followerErr = runChild("writer_failover_follower", path, 30*time.Second)
	}()

	<-leaderDone
	<-followerDone

	if followerErr != nil {
		t.Errorf("Follower 失败: %v\n输出: %s", followerErr, followerOutput)
	}

	// 验证文件中有来自两个进程的数据
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), "[LEADER") {
		t.Error("文件中缺少 Leader 写入的数据")
	}
	if !strings.Contains(string(content), "[FOLLOWER") {
		t.Error("文件中缺少 Follower 写入的数据")
	}
}

// ========== LiteWriter 多进程测试 ==========

// TestMultiProcLiteWriterBasic 验证 2 个进程通过 LiteWriter 并发写入，无数据丢失。
func TestMultiProcLiteWriterBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试 (short 模式)")
	}
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestMultiProcLiteWriterBasic.log")
	defer cleanupIntegration(t, path)

	runTwoChildren(t, "lite_basic", path, 30*time.Second)

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 200 {
		t.Errorf("期望 200 行，实际 %d 行", len(lines))
	}
}

// TestMultiProcLiteWriterRotation 验证多进程 LiteWriter 各自触发轮转，分布式锁防冲突。
func TestMultiProcLiteWriterRotation(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试 (short 模式)")
	}
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestMultiProcLiteWriterRotation.log")
	defer cleanupIntegration(t, path)

	runTwoChildren(t, "lite_rotate", path, 60*time.Second)

	base := filepath.Base(path)
	matches, _ := filepath.Glob(path + ".*")
	backupCount := 0
	for _, m := range matches {
		if isBackupFile(base, filepath.Base(m)) {
			backupCount++
		}
	}
	if backupCount == 0 {
		t.Fatal("轮转未发生，未找到备份文件")
	}
	t.Logf("找到 %d 个备份文件", backupCount)

	// 统计所有文件（当前文件 + 备份文件）中的总行数，验证数据不丢失。
	// LiteWriter 无 IPC，进程可能将数据写入已被其他进程轮转的旧文件句柄，
	// 因此不能要求当前日志文件非空，数据在备份文件中也是正常的。
	totalLines := countLinesInFile(path)
	for _, m := range matches {
		if isBackupFile(base, filepath.Base(m)) {
			totalLines += countLinesInFile(m)
		}
	}
	if totalLines != 60 {
		t.Errorf("期望总共 60 行（2 进程 × 30 条），实际 %d 行", totalLines)
	}
}
