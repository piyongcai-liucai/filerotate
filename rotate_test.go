// rotate_test.go
// rotate.go 中文件轮转核心函数的单元测试。
package filerotate

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseTimestampNewFormat(t *testing.T) {
	// 有效新格式: 25字符，纳秒精度
	ts := parseTimestamp("20060102_150405.000000000")
	if ts.IsZero() {
		t.Fatal("expected valid time for new format")
	}
	if ts.Year() != 2006 || ts.Month() != 1 || ts.Day() != 2 {
		t.Fatalf("date mismatch: got %s", ts)
	}
	if ts.Hour() != 15 || ts.Minute() != 4 || ts.Second() != 5 {
		t.Fatalf("time mismatch: got %s", ts)
	}
}

func TestParseTimestampOldFormat(t *testing.T) {
	// 有效旧格式: 15字符，秒精度
	ts := parseTimestamp("20060102_150405")
	if ts.IsZero() {
		t.Fatal("expected valid time for old format")
	}
	if ts.Year() != 2006 || ts.Month() != 1 || ts.Day() != 2 {
		t.Fatalf("date mismatch: got %s", ts)
	}
}

func TestParseTimestampNewFormatWrongUnderscore(t *testing.T) {
	// 25字符但下划线位置错误
	ts := parseTimestamp("20060102-150405.000000000")
	if !ts.IsZero() {
		t.Fatal("expected zero time when underscore is at wrong position")
	}
}

func TestParseTimestampNewFormatWrongDot(t *testing.T) {
	// 25字符但点位置错误
	ts := parseTimestamp("20060102_150405-000000000")
	if !ts.IsZero() {
		t.Fatal("expected zero time when dot is at wrong position")
	}
}

func TestParseTimestampOldFormatWrongUnderscore(t *testing.T) {
	// 15字符但下划线位置错误
	ts := parseTimestamp("20060102-150405")
	if !ts.IsZero() {
		t.Fatal("expected zero time when underscore is at wrong position")
	}
}

func TestParseTimestampInvalidLength(t *testing.T) {
	tests := []struct {
		name string
		ext  string
	}{
		{"too short 14", "2006010215040"},
		{"too long 16", "20060102_1504051"},
		{"too short 24", "20060102_150405.00000000"},
		{"too long 26", "20060102_150405.0000000001"},
		{"empty", ""},
		{"just underscore", "_______________"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if ts := parseTimestamp(tt.ext); !ts.IsZero() {
				t.Errorf("expected zero time for %q, got %s", tt.ext, ts)
			}
		})
	}
}

func TestParseTimestampInvalidDate(t *testing.T) {
	// 有效分隔符但日期值无效
	ts := parseTimestamp("20060132_150405")
	if !ts.IsZero() {
		t.Fatal("expected zero time for invalid date")
	}

	ts = parseTimestamp("20060102_250405.000000000")
	if !ts.IsZero() {
		t.Fatal("expected zero time for invalid time in new format")
	}
}

func TestParseTimestampRealTimestamp(t *testing.T) {
	// 验证纳秒精度被正确保留
	ts := parseTimestamp("20240102_030405.123456789")
	if ts.Year() != 2024 || ts.Month() != 1 || ts.Day() != 2 {
		t.Fatalf("date mismatch: got %s", ts)
	}
	if ts.Hour() != 3 || ts.Minute() != 4 || ts.Second() != 5 {
		t.Fatalf("time mismatch: got %s", ts)
	}
	if ts.Nanosecond() != 123456789 {
		t.Fatalf("nanosecond mismatch: got %d", ts.Nanosecond())
	}
}

func TestCleanOldBackupsSkipsNonBackupFiles(t *testing.T) {
	ensureLogDir(t)
	path := filepath.Join(logDir, "TestCleanOldBackups.log")
	defer cleanupLogs(t, path)

	// 创建非备份文件(无效时间戳格式)
	invalid, _ := os.Create(path + ".not_a_timestamp")
	invalid.Close()
	// 创建锁文件和 socket 文件
	lockF, _ := os.Create(path + ".lock")
	lockF.Close()
	sockF, _ := os.Create(path + ".sock")
	sockF.Close()

	// 创建旧备份文件
	oldTime := time.Now().AddDate(0, 0, -10)
	oldName := path + "." + oldTime.Format("20060102_150405")
	f, _ := os.Create(oldName)
	f.Close()
	os.Chtimes(oldName, oldTime, oldTime)

	cleanOldBackups(path, 7)

	// 无效格式、锁文件、socket 文件应保留
	if _, err := os.Stat(path + ".not_a_timestamp"); os.IsNotExist(err) {
		t.Error("non-backup file should not be deleted")
	}
	if _, err := os.Stat(path + ".lock"); os.IsNotExist(err) {
		t.Error("lock file should not be deleted")
	}
	if _, err := os.Stat(path + ".sock"); os.IsNotExist(err) {
		t.Error("socket file should not be deleted")
	}
	// 旧备份应被删除
	if _, err := os.Stat(oldName); !os.IsNotExist(err) {
		t.Error("old backup file should be deleted")
	}
}
