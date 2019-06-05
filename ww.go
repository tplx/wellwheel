// Copyright 2019 TempleX (temple3x@gmail.com).
// Copyright (c) 2014 Nate Finch
//
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package wellwheel

import (
	"bufio"
	"container/heap"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tplx/fnc"
)

type Logger struct {
	Mu sync.Mutex

	// log file status.
	outputPath string
	file       *os.File // output *os.File.
	size       int64    // output file size.
	// dirty page cache info.
	// ps: may already been flushed by kernel.
	dirtyOffset int64
	dirtySize   int64

	// user-space buffer for log writing.
	buf     *bufio.Writer
	bufSize int

	syncWrite bool

	// jobs of sync page cache, use chan for avoiding stall.
	syncJobs     chan syncJob
	bytesPerSync int64
	syncInterval time.Duration

	// rotation config.
	maxSize    int64
	Backups    *backupInfos // all backups information.
	maxBackups int
	localTime  bool
}

// Config of Logger.
type Config struct {
	OutputPath string `json:"output_path" toml:"output_path"` // Log file path.

	Sync bool `json:"sync" toml:"sync"` // Sync every writes or not.
	// User Space buffer size.
	// Unit is KB.
	BufSize int64 `json:"buf_size" toml:"buf_size"`
	// The Interval of flushing data from buffer&page cache to storage media.
	// Unit is second.
	SyncInterval int64 `json:"sync_interval" toml:"sync_interval"`
	BytesPerSync int64 `json:"bytes_per_sync" toml:"bytes_per_sync"` // After write BytesPerSync bytes call sync.
	// Maximum size of a log file before it gets rotated.
	// Unit is MB.
	MaxSize int64 `json:"max_size" toml:"max_size"`
	// Maximum number of backup log files to retain.
	MaxBackups int `json:"max_backups" toml:"max_backups"`
	// Timestamp in backup log file. Default is to use UTC time.
	LocalTime bool `json:"local_time" toml:"local_time"`
}

// Use variables for tests.
var (
	kb int64 = 1024
	mb int64 = 1024 * 1024
)

var (
	// >32KB couldn't improve performance significantly.
	defaultBufSize = 32 * kb // 32KB

	// RocksDB use 1MB too. :D
	defaultBytesPerSync = mb              // 1MB
	defaultSyncInterval = 5 * time.Second // 5second

	// We don't need to keep too many backups,
	// in practice, log shipper will collect the logs already.
	defaultMaxSize    = 256 * mb
	defaultMaxBackups = 8
)

func New(conf *Config) (l *Logger, err error) {

	l = new(Logger)
	err = l.init(conf)
	if err != nil {
		return
	}

	go l.doSyncJob() // sync log content async.

	go func() {
		ticker := time.NewTicker(l.syncInterval)
		for range ticker.C {
			l.Sync()
		}
	}()

	return
}

// Init Logger when creates a new Logger.
func (l *Logger) init(conf *Config) (err error) {
	err = l.parseConf(conf)
	if err != nil {
		return
	}

	l.listBackup()

	err = l.openExistOrNew()
	if err != nil {
		return
	}

	l.syncWrite = conf.Sync

	l.buf = bufio.NewWriterSize(l.file, l.bufSize)
	l.syncJobs = make(chan syncJob, 8)
	return
}

// Set Logger's args from Config.
func (l *Logger) parseConf(conf *Config) (err error) {

	if conf.OutputPath == "" {
		return errors.New("empty log file path")
	} else {
		l.outputPath = conf.OutputPath
	}

	if conf.BufSize <= 0 {
		l.bufSize = int(defaultBufSize)
	} else {
		l.bufSize = int(conf.BufSize * kb)
	}

	if conf.MaxSize <= 0 {
		l.maxSize = defaultMaxSize
	} else {
		l.maxSize = conf.MaxSize * mb
	}

	if conf.MaxBackups <= 0 {
		l.maxBackups = defaultMaxBackups
	} else {
		l.maxBackups = conf.MaxBackups
	}

	l.localTime = conf.LocalTime

	if conf.BytesPerSync <= 0 {
		l.bytesPerSync = defaultBytesPerSync
	} else {
		l.bytesPerSync = conf.BytesPerSync
	}

	if conf.SyncInterval <= 0 {
		l.syncInterval = defaultSyncInterval
	} else {
		l.syncInterval = time.Duration(conf.SyncInterval) * time.Second
	}

	return
}

// List all backup log files (in init process),
// and remove them if there are too many backups.
func (l *Logger) listBackup() {
	backups := make(backupInfos, 0, defaultMaxBackups*2)
	l.Backups = &backups

	dir := filepath.Dir(l.outputPath)
	ns, err := ioutil.ReadDir(dir)
	if err != nil {
		return // pathError, ignore
	}

	prefix, ext := l.prefixAndExt()

	for _, f := range ns {
		if f.IsDir() {
			continue
		}
		if ts, err := l.timeFromName(f.Name(), prefix, ext); err == nil {
			heap.Push(l.Backups, backupInfo{ts, filepath.Join(dir, f.Name())})
			continue
		}
	}

	for l.Backups.Len() > l.maxBackups {
		v := heap.Pop(l.Backups)
		os.Remove(v.(backupInfo).fp)
	}
}

// Open log file when start up.
func (l *Logger) openExistOrNew() (err error) {

	if !fnc.Exist(l.outputPath) {
		return l.openNew()
	}

	f, err := os.OpenFile(l.outputPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return
	}

	l.file = f
	l.size = stat.Size()

	// maybe not correct, but it's ok.
	l.dirtyOffset = stat.Size()
	l.dirtySize = 0

	return
}

// Open a new log file in two conditions:
// 1. Start up with no existed log file.
// 2. Need rename in rotation process.
func (l *Logger) openNew() (err error) {
	fp := l.outputPath
	if fnc.Exist(fp) { // file exist may happen in rotation process.
		backupFP, t := makeBackupFP(fp, l.localTime)

		if err = os.Rename(fp, backupFP); err != nil {
			return fmt.Errorf("failed to rename log file, output: %s backup: %s", fp, backupFP)
		}
		l.sync(true)

		heap.Push(l.Backups, backupInfo{t, backupFP})
		if l.Backups.Len() > l.maxBackups {
			v := heap.Pop(l.Backups)
			os.Remove(v.(backupInfo).fp)
		}
	}

	dir := filepath.Dir(fp)
	err = os.MkdirAll(dir, 0755) // ensure we have created the right dir.
	if err != nil {
		return fmt.Errorf("failed to make dirs for log file: %s", err.Error())
	}

	// Open a totally new file.
	// Truncate here to clean up file content if someone else creates
	// the file between exist checking and create file.
	// Can't use os.O_EXCL here, because it may break rotation process.
	f, err := os.OpenFile(fp, os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %s", err.Error())
	}

	l.file = f
	l.buf = bufio.NewWriterSize(f, l.bufSize)
	l.size = 0
	l.dirtyOffset = 0
	l.dirtySize = 0

	return
}

// prefixAndExt returns the filename part and extension part from the Logger's
// filename.
func (l *Logger) prefixAndExt() (prefix, ext string) {
	name := filepath.Base(l.outputPath)
	ext = filepath.Ext(name)
	prefix = name[:len(name)-len(ext)] + "-"
	return prefix, ext
}

const (
	BackupTimeFmt     = ISO8601TimeFormat
	ISO8601TimeFormat = "2006-01-02T15:04:05.000Z0700"
)

// timeFromName extracts the formatted time from the filename by stripping off
// the filename's prefix and extension. This prevents someone's filename from
// confusing time.parse.
func (l *Logger) timeFromName(fp, prefix, ext string) (int64, error) {
	filename := filepath.Base(fp)
	if !strings.HasPrefix(filename, prefix) {
		return 0, errors.New("mismatched prefix")
	}
	if !strings.HasSuffix(filename, ext) {
		return 0, errors.New("mismatched extension")
	}
	tsStr := filename[len(prefix) : len(filename)-len(ext)]
	t, err := time.Parse(BackupTimeFmt, tsStr)
	if err != nil {
		return 0, err
	}
	return t.Unix(), nil
}

func makeBackupFP(name string, local bool) (string, int64) {
	dir := filepath.Dir(name)
	filename := filepath.Base(name)
	ext := filepath.Ext(filename)
	prefix := filename[:len(filename)-len(ext)]
	t := time.Now()
	if !local {
		t = t.UTC()
	}

	timestamp := t.Format(BackupTimeFmt)
	return filepath.Join(dir, fmt.Sprintf("%s-%s%s", prefix, timestamp, ext)), t.Unix()
}

func (l *Logger) Write(p []byte) (written int, err error) {
	l.Mu.Lock()
	defer l.Mu.Unlock()

	written, err = l.buf.Write(p)
	if err != nil {
		return
	}

	l.size += int64(written)
	l.dirtySize += int64(written)
	if l.syncWrite {
		err = l.buf.Flush()
		if err != nil {
			return
		}
		err = fnc.Flush(l.file, l.dirtyOffset, l.dirtySize)
		if err != nil {
			return
		}
		l.dirtyOffset += l.dirtySize
		l.dirtySize = 0
	} else {
		if l.dirtySize >= l.bytesPerSync {
			l.sync(false)
		}
	}

	if l.size > l.maxSize {
		if err = l.openNew(); err != nil {
			return
		}
	}
	return
}

// Sync buf & dirty_page_cache to the storage media.
func (l *Logger) Sync() (err error) {
	l.Mu.Lock()
	defer l.Mu.Unlock()

	l.sync(false)
	return
}

func (l *Logger) sync(isBackup bool) {

	l.buf.Flush()

	l.syncJobs <- syncJob{l.file, l.size, l.dirtyOffset, l.dirtySize, isBackup}
	l.dirtyOffset += l.dirtySize
	l.dirtySize = 0
}

type syncJob struct {
	f        *os.File
	fsize    int64
	offset   int64
	size     int64
	isBackup bool
}

func (l *Logger) doSyncJob() {

	for job := range l.syncJobs {
		f, offset, size := job.f, job.offset, job.size
		if size == 0 {
			continue
		}
		fnc.FlushHint(f, offset, size)
		if job.isBackup {
			// warn: may drop too much cache or still dirty.
			// because log ship may still need the cache(a bit slower than writing).
			fnc.DropCache(f, 0, job.fsize)
			f.Close()
		}
	}
}

type backupInfo struct {
	ts int64
	fp string
}

// backupInfos implements heap interface.
type backupInfos []backupInfo

func (h *backupInfos) Less(i, j int) bool {
	return (*h)[i].ts < ((*h)[j].ts)
}

func (h *backupInfos) Swap(i, j int) {
	if i >= 0 && j >= 0 {
		(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
	}
}

func (h *backupInfos) Len() int {
	return len(*h)
}

func (h *backupInfos) Pop() (v interface{}) {
	if h.Len()-1 >= 0 {
		*h, v = (*h)[:h.Len()-1], (*h)[h.Len()-1]
	}
	return
}

func (h *backupInfos) Push(v interface{}) {
	*h = append(*h, v.(backupInfo))
}
