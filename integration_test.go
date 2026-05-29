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
	case "basic":
		err = childBasic(filePath)
	case "rotate":
		err = childRotate(filePath)
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

	for range 2 {
		wg.Go(func() {
			_, err := runChild(role, path, timeout)
			if err != nil {
				errCh <- err
			}
		})
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
}

// cleanupIntegration removes log files, backups, and lock files.
func cleanupIntegration(t *testing.T, path string) {
	t.Helper()
	os.Remove(path)
	backups, _ := filepath.Glob(path + ".2*")
	for _, b := range backups {
		os.Remove(b)
	}
	os.Remove(path + ".lock")
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

// childBasic 创建 Writer，写入 100 行后退出。
func childBasic(filePath string) error {
	w, err := New(Config{
		FilePath:   filePath,
		MaxSizeMB:  100,
		MaxAgeDays: 0,
	})
	if err != nil {
		return err
	}
	defer w.Close()

	marker := fmt.Sprintf("[PID %d]\n", os.Getpid())
	for range 100 {
		if _, err := w.Write([]byte(marker)); err != nil {
			return err
		}
	}
	return nil
}

// childRotate 创建 Writer（小 maxSize），写入触发轮转。
func childRotate(filePath string) error {
	w, err := New(Config{
		FilePath:      filePath,
		MaxSizeMB:     1,
		MaxAgeDays:    0,
		CheckInterval: 100 * time.Millisecond,
	})
	if err != nil {
		return err
	}
	defer w.Close()

	w.maxSize = 2048 // 测试用极小值，int64 在现代平台上原子读写

	chunk := strings.Repeat("z", 256)
	for i := range 30 {
		line := fmt.Sprintf("[%d] chunk %d: %s\n", os.Getpid(), i, chunk)
		if _, err := w.Write([]byte(line)); err != nil {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// ========== 多进程测试 ==========

// TestMultiProcWriter 验证 2 个进程并发写入，无数据丢失。
func TestMultiProcWriter(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试 (short 模式)")
	}
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestMultiProcWriter.log")
	defer cleanupIntegration(t, path)

	runTwoChildren(t, "basic", path, 30*time.Second)

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 200 {
		t.Errorf("期望 200 行，实际 %d 行", len(lines))
	}
}

// TestMultiProcWriterRotation 验证多进程各自触发轮转，文件锁防冲突。
func TestMultiProcWriterRotation(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试 (short 模式)")
	}
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestMultiProcWriterRotation.log")
	defer cleanupIntegration(t, path)

	runTwoChildren(t, "rotate", path, 60*time.Second)

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
	// 进程可能将数据写入已被其他进程轮转的旧文件句柄，
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

// TestMultiGoroutineRotation 多 goroutine 并发写入，验证轮转正常触发。
// 模拟原 example/main.go 的多进程场景。
func TestMultiGoroutineRotation(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试 (short 模式)")
	}
	ensureLogDir(t)
	path := filepath.Join(logDir, t.Name()+".log")
	defer cleanupIntegration(t, path)

	const (
		numProcs  = 3
		maxRotate = 3
		chunkSize = 512
	)

	done := make(chan struct{})
	var wg sync.WaitGroup

	wg.Go(func() {
		base := filepath.Base(path)
		for {
			matches, _ := filepath.Glob(path + ".2*")
			count := 0
			for _, m := range matches {
				if isBackupFile(base, filepath.Base(m)) {
					count++
				}
			}
			if count >= maxRotate {
				close(done)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	for id := range numProcs {
		wg.Go(func() {
			w, err := New(Config{
				FilePath:      path,
				MaxSizeMB:     1,
				MaxAgeDays:    0,
				CheckInterval: 100 * time.Millisecond,
			})
			if err != nil {
				t.Error(err)
				return
			}
			defer w.Close()
			w.maxSize = 2048

			chunk := strings.Repeat("x", chunkSize)
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				fmt.Fprintf(w, "[%d/%d] line %d: %s\n", id, numProcs, i, chunk)
				time.Sleep(time.Millisecond)
			}
		})
	}

	wg.Wait()

	base := filepath.Base(path)
	matches, _ := filepath.Glob(path + ".2*")
	count := 0
	for _, m := range matches {
		if isBackupFile(base, filepath.Base(m)) {
			count++
		}
	}
	if count < maxRotate {
		t.Errorf("expected >= %d backups, got %d", maxRotate, count)
	}
}
