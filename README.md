WellWheel
===

[![Build Status][1]][2]

[1]: https://travis-ci.org/tplx/wellwheel.svg?branch=master
[2]: https://travis-ci.org/tplx/wellwheel

Package wellwheel is for log rotation.
It will reopen log file and backup old log file automatically,
in this process, old log file's page cache will be clean up too.

- __WriteSyncCloser__

    wellwheel implements such methods:

    ```
        Write(p []byte) (written int, err error)
        Sync() (err error)
    ```

    Suit for most of log packages.

- __Rolling__

    e.g. The log file's name is a.log,
    all the log files will be:

    ```
    a.log
    a-time.log
    a-time.log
    a-time.log
    a-time.log
                  ....
    ```
      
     Log shippers such as ELK's filebeat can set path to:
    
    ```
    a.log
    ```
    **Warn:**
        
    1. The date in backup file name maybe not correct all the time.
    (some log entries won't belong to this interval)
    2. It will clean up log files when they are too many.
    
- __Buffer Write__

    All log data will be written to a user-space buffer first, 
    then write to the log file.

- __Page Cache Control__

    It's meaningless to keep log files' data in page cache,
    so when the dirty pages are too many or we need reopen a new file,
    wellwheel will sync data to disk, then drop the page cache.

    ps: OS can do the page cache flush by itself, 
    but it may create a burst of write I/O when dirty pages hit a threshold.

## Something Can't Do

1. Compress

    Compress is not implemented now, just because I don't need it. :D

    Maybe add it in future.
    
2. Delete backup log files according by time
   
   In present, wellwheel just delete backups when they are too many,
   is that enough?

## Example

### Stdlib Logger

```
    l, _ := New(&conf)
    log.New(l, "", log.Ldate)
```

### Zap Logger

```
    l, _ := New(&conf)
    zapcore.AddSync(l)
    ...
```

## Acknowledgments

- [lumberjack](https://github.com/natefinch/lumberjack)