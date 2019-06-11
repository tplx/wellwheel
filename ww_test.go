// Copyright 2019 TempleX (temple3x@gmail.com).
// Copyright (c) 2014 Nate Finch
//
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package wellwheel

import (
	"container/heap"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestNewFile(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	fp := filepath.Join(dir, "ww_newfile.log")
	defer func() {
		os.Remove(dir)
		os.Remove(fp)
	}()

	_, err = New(&Config{
		OutputPath: fp,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewWithExistFile(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	_, err = New(&Config{
		OutputPath: f.Name(),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMakeLogDir(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	fp := filepath.Join(dir, "dir", "ww_newfile.log")
	defer func() {
		os.Remove(dir)
		os.Remove(fp)
	}()

	_, err = New(&Config{
		OutputPath: fp,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLogger_Write(t *testing.T) {
	outputPath := filepath.Join(os.TempDir(), "test_logger_write"+strconv.FormatInt(time.Now().UnixNano(), 10))
	var l *Logger
	defer func() {
		os.Remove(outputPath)
	}()

	bufSize := 10
	kb = 1
	l, err := New(&Config{
		OutputPath: outputPath, BufSize: int64(bufSize), Developed: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < bufSize; i++ {
		written, err := l.Write([]byte{'1'})
		if err != nil {
			t.Fatal(err)
		}
		if written != 1 {
			t.Fatal("written mismatch")
		}
		if l.dirtySize != int64(i+1) {
			t.Fatal("dirtySize mismatch")
		}
		if l.dirtyOffset != 0 {
			t.Fatal("dirtyOffset mismatch")
		}
	}

	stat, err := l.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != 0 {
		t.Fatal("mismatch fsize")
	}
}

func TestLogger_WriteAt(t *testing.T) {
	outputPath := filepath.Join(os.TempDir(), "test_logger_write"+strconv.FormatInt(time.Now().UnixNano(), 10))
	var l *Logger
	defer func() {
		os.Remove(outputPath)
	}()

	maxSize := 10
	mb = 1
	l, err := New(&Config{
		OutputPath: outputPath, MaxSize: int64(maxSize), Developed: true,
		EnablePWrite: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	stat, err := l.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != int64(maxSize) {
		t.Fatal("mismatch fsize", stat.Size())
	}

	p := make([]byte, 2)
	for i := 0; i < maxSize/2; i++ {
		written, err := l.WriteAt(p, int64(i*2))
		if err != nil {
			t.Fatal(err)
		}
		if written != 2 {
			t.Fatal("written mismatch")
		}
		if l.dirtySize != int64((i+1)*2) {
			t.Fatal("dirtySize mismatch")
		}
		if l.dirtyOffset != 0 {
			t.Fatal("dirtyOffset mismatch")
		}
	}

	stat, err = l.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != int64(maxSize) {
		t.Fatal("mismatch fsize")
	}
}

// write >= bytes_per_sync
func TestLogger_Write_Sync(t *testing.T) {
	outputPath := filepath.Join(os.TempDir(), "test_logger_write"+strconv.FormatInt(time.Now().UnixNano(), 10))
	var l *Logger
	defer func() {
		os.Remove(outputPath)
	}()

	bufSize := 10
	kb = 1
	l, err := New(&Config{
		OutputPath: outputPath, BufSize: int64(bufSize),
		BytesPerSync: 10,
		Developed:    true,
	})
	if err != nil {
		t.Fatal(err)
	}

	writeSize := bufSize + 1
	for i := 0; i < writeSize; i++ {
		written, err := l.Write([]byte{'1'})
		if err != nil {
			t.Fatal(err)
		}
		if written != 1 {
			t.Fatal("written mismatch")
		}
	}

	if l.dirtySize != 1 {
		t.Fatal("dirtySize mismatch")
	}
	if l.dirtyOffset != int64(bufSize) {
		t.Fatal("dirtyOffset mismatch")
	}

	stat, err := l.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != int64(bufSize) {
		t.Fatal("fsize mismatch")
	}
}

// writeAt >= bytes_per_sync
func TestLogger_WriteAt_Sync(t *testing.T) {
	outputPath := filepath.Join(os.TempDir(), "test_logger_write"+strconv.FormatInt(time.Now().UnixNano(), 10))
	var l *Logger
	defer func() {
		os.Remove(outputPath)
	}()

	kb = 1
	mb = 1
	maxSize := 10
	bytesPerSync := 8
	l, err := New(&Config{
		OutputPath:   outputPath,
		BytesPerSync: int64(bytesPerSync),
		Developed:    true,
		EnablePWrite: true,
		MaxSize:      int64(maxSize),
	})
	if err != nil {
		t.Fatal(err)
	}

	p := make([]byte, 2)
	for i := 0; i <= bytesPerSync/2; i++ {
		written, err := l.WriteAt(p, int64(i*2))
		if err != nil {
			t.Fatal(err)
		}
		if written != 2 {
			t.Fatal("written mismatch")
		}
	}

	if l.dirtySize != 2 {
		t.Fatal("dirtySize mismatch")
	}
	if l.dirtyOffset != int64(bytesPerSync) {
		t.Fatal("dirtyOffset mismatch")
	}

	stat, err := l.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != int64(maxSize) {
		t.Fatal("mismatch fsize", stat.Size())
	}
}

// trigger auto sync > interval
func TestLogger_Write_GT_Interval(t *testing.T) {
	outputPath := filepath.Join(os.TempDir(), "test_logger_write"+strconv.FormatInt(time.Now().UnixNano(), 10))
	var l *Logger
	defer func() {
		os.Remove(outputPath)
	}()

	l, err := New(&Config{
		OutputPath: outputPath, SyncInterval: 1,
		Developed: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	writeSize := 10
	for i := 0; i < writeSize; i++ {
		written, err := l.Write([]byte{'1'})
		if err != nil {
			t.Fatal(err)
		}
		if written != 1 {
			t.Fatal("written mismatch")
		}
	}
	time.Sleep(1200 * time.Millisecond)

	l.Mu.Lock()
	defer l.Mu.Unlock()
	if l.dirtySize != 0 {
		t.Fatal("dirtySize mismatch")
	}
	if l.dirtyOffset != int64(writeSize) {
		t.Fatal("dirtyOffset mismatch")
	}

	stat, err := l.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != int64(writeSize) {
		t.Fatal("fsize mismatch")
	}
}

// trigger auto sync > interval with WriteAt
func TestLogger_WriteAt_GT_Interval(t *testing.T) {
	outputPath := filepath.Join(os.TempDir(), "test_logger_write"+strconv.FormatInt(time.Now().UnixNano(), 10))
	var l *Logger
	defer func() {
		os.Remove(outputPath)
	}()

	kb = 1
	mb = 1
	maxSize := 10
	bytesPerSync := maxSize
	l, err := New(&Config{
		OutputPath:   outputPath,
		BytesPerSync: int64(bytesPerSync),
		Developed:    true,
		EnablePWrite: true,
		MaxSize:      int64(maxSize),
		SyncInterval: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	p := make([]byte, 2)
	for i := 0; i < 1; i++ {
		written, err := l.WriteAt(p, int64(i*2))
		if err != nil {
			t.Fatal(err)
		}
		if written != 2 {
			t.Fatal("written mismatch")
		}
	}

	time.Sleep(1200 * time.Millisecond)

	l.Mu.Lock()
	defer l.Mu.Unlock()
	if l.dirtySize != 0 {
		t.Fatal("dirtySize mismatch")
	}
	if l.dirtyOffset != 2 {
		t.Fatal("dirtyOffset mismatch")
	}

	stat, err := l.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != int64(maxSize) {
		t.Fatal("fsize mismatch")
	}
}

// force sync (written < bufSize)
func TestLogger_Sync(t *testing.T) {
	outputPath := filepath.Join(os.TempDir(), "test_logger_write"+strconv.FormatInt(time.Now().UnixNano(), 10))
	var l *Logger
	defer func() {
		os.Remove(outputPath)
	}()

	bufSize := 10
	kb = 1
	l, err := New(&Config{
		OutputPath: outputPath, BufSize: int64(bufSize), Developed: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	writeSize := bufSize - 1

	for i := 0; i < writeSize; i++ {
		written, err := l.Write([]byte{'1'})
		if err != nil {
			t.Fatal(err)
		}
		if written != 1 {
			t.Fatal("written mismatch")
		}
	}

	err = l.Sync()
	if err != nil {
		t.Fatal(err)
	}

	if l.dirtySize != 0 {
		t.Fatal("dirtySize mismatch")
	}
	if l.dirtyOffset != int64(writeSize) {
		t.Fatal("dirtyOffset mismatch")
	}

	stat, err := os.Stat(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != int64(writeSize) {
		t.Fatal("fsize mismatch")
	}
}

// force sync (written < bufSize) with WriteAt
func TestLogger_Sync_WriteAt(t *testing.T) {
	outputPath := filepath.Join(os.TempDir(), "test_logger_write"+strconv.FormatInt(time.Now().UnixNano(), 10))
	var l *Logger
	defer func() {
		os.Remove(outputPath)
	}()

	kb = 1
	mb = 1
	maxSize := 10
	bytesPerSync := maxSize
	l, err := New(&Config{
		OutputPath:   outputPath,
		BytesPerSync: int64(bytesPerSync),
		Developed:    true,
		EnablePWrite: true,
		MaxSize:      int64(maxSize),
	})
	if err != nil {
		t.Fatal(err)
	}

	p := make([]byte, 2)
	for i := 0; i < 1; i++ {
		written, err := l.WriteAt(p, int64(i*2))
		if err != nil {
			t.Fatal(err)
		}
		if written != 2 {
			t.Fatal("written mismatch")
		}
	}

	err = l.Sync()
	if err != nil {
		t.Fatal(err)
	}

	if l.dirtySize != 0 {
		t.Fatal("dirtySize mismatch")
	}
	if l.dirtyOffset != 2 {
		t.Fatal("dirtyOffset mismatch")
	}

	stat, err := l.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != int64(maxSize) {
		t.Fatal("fsize mismatch")
	}
}

func TestLogger_Rotate(t *testing.T) {
	outputPath := filepath.Join(os.TempDir(), "test_logger_write"+strconv.FormatInt(time.Now().UnixNano(), 10))
	var l *Logger
	defer func() {
		os.Remove(outputPath)
	}()

	newtPath := filepath.Join(os.TempDir(), "test_logger_write"+strconv.FormatInt(time.Now().UnixNano(), 10))
	defer func() {
		os.Remove(newtPath)
	}()

	mb = 1
	l, err := New(&Config{
		OutputPath: outputPath, MaxSize: mb, Developed: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = l.Write([]byte{'1'})
	if err != nil {
		t.Fatal(err)
	}

	_, err = l.Write([]byte{'1'})
	if err != nil {
		t.Fatal(err)
	}

	backup := heap.Pop(l.Backups).(BackupInfo)
	prefix, ext := l.prefixAndExt()
	tt, err := l.timeFromName(backup.Fp, prefix, ext)
	if err != nil {
		t.Fatal(err)
	}
	if tt != backup.ts {
		t.Fatal("backup time mismatch")
	}

	backupStat, err := os.Stat(backup.Fp)
	if err != nil {
		t.Fatal(err)
	}
	if backupStat.Size() != 2 {
		t.Fatal("old log file size mismatch", backupStat.Size())
	}

	if l.dirtySize != 0 {
		t.Fatal("dirtySize mismatch", l.dirtySize)
	}
	if l.dirtyOffset != 0 {
		t.Fatal("dirtyOffset mismatch")
	}
}

func TestLogger_Rotate_WriteAt(t *testing.T) {
	outputPath := filepath.Join(os.TempDir(), "test_logger_write"+strconv.FormatInt(time.Now().UnixNano(), 10))
	var l *Logger
	defer func() {
		os.Remove(outputPath)
	}()

	newtPath := filepath.Join(os.TempDir(), "test_logger_write"+strconv.FormatInt(time.Now().UnixNano(), 10))
	defer func() {
		os.Remove(newtPath)
	}()

	mb = 1
	l, err := New(&Config{
		OutputPath: outputPath, MaxSize: mb, Developed: true,
		EnablePWrite: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = l.WriteAt([]byte{'1'}, 0)
	if err != nil {
		t.Fatal(err)
	}

	_, err = l.WriteAt([]byte{'1'}, 1)
	if err != nil {
		t.Fatal(err)
	}

	backup := heap.Pop(l.Backups).(BackupInfo)
	prefix, ext := l.prefixAndExt()
	tt, err := l.timeFromName(backup.Fp, prefix, ext)
	if err != nil {
		t.Fatal(err)
	}
	if tt != backup.ts {
		t.Fatal("backup time mismatch")
	}

	backupStat, err := os.Stat(backup.Fp)
	if err != nil {
		t.Fatal(err)
	}
	if backupStat.Size() != 2 {
		t.Fatal("old log file size mismatch", backupStat.Size())
	}

	if l.dirtySize != 0 {
		t.Fatal("dirtySize mismatch", l.dirtySize)
	}
	if l.dirtyOffset != 0 {
		t.Fatal("dirtyOffset mismatch")
	}
}

func TestWriteSync(t *testing.T) {
	outputPath := filepath.Join(os.TempDir(), "test_logger_write"+strconv.FormatInt(time.Now().UnixNano(), 10))
	var l *Logger
	defer func() {
		os.Remove(outputPath)
	}()

	l, err := New(&Config{
		OutputPath: outputPath,
		Sync:       true,
		Developed:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	writeSize := 3

	for i := 0; i < writeSize; i++ {
		written, err := l.Write([]byte{'1'})
		if err != nil {
			t.Fatal(err)
		}
		if written != 1 {
			t.Fatal("written mismatch")
		}
	}

	if l.dirtySize != 0 {
		t.Fatal("dirtySize mismatch")
	}
	if l.dirtyOffset != int64(writeSize) {
		t.Fatal("dirtyOffset mismatch")
	}

	stat, err := os.Stat(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != int64(writeSize) {
		t.Fatal("fsize mismatch")
	}
}

func TestWriteSync_WriteAt(t *testing.T) {
	outputPath := filepath.Join(os.TempDir(), "test_logger_write"+strconv.FormatInt(time.Now().UnixNano(), 10))
	var l *Logger
	defer func() {
		os.Remove(outputPath)
	}()

	l, err := New(&Config{
		OutputPath:   outputPath,
		Sync:         true,
		Developed:    true,
		EnablePWrite: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	writeSize := 3

	for i := 0; i < writeSize; i++ {
		written, err := l.WriteAt([]byte{'1'}, int64(i))
		if err != nil {
			t.Fatal(err)
		}
		if written != 1 {
			t.Fatal("written mismatch")
		}
	}

	if l.dirtySize != 0 {
		t.Fatal("dirtySize mismatch")
	}
	if l.dirtyOffset != int64(writeSize) {
		t.Fatal("dirtyOffset mismatch")
	}

	stat, err := os.Stat(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != int64(l.maxSize) {
		t.Fatal("fsize mismatch")
	}
}

func TestLogger_BackupInfos(t *testing.T) {
	h := make(BackupInfos, 0, 3)
	now := time.Now()
	for i := 3; i > 0; i-- {
		now = now.Add(-time.Second)
		heap.Push(&h, BackupInfo{now.Unix(), strconv.FormatInt(int64(i), 10)})
	}
	backup := heap.Pop(&h).(BackupInfo)
	if backup.Fp != "1" {
		t.Fatal("backupInfos pop mismatch")
	}
}
