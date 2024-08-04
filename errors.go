package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty                 = errors.New("key is empty")
	ErrIndexDeleteFailed          = errors.New("index delete failed")
	ErrKeyNotFound                = errors.New("key not found")
	ErrDataFileNotFound           = errors.New("storage file not found")
	ErrDataDirectoryCorrupted     = errors.New("storage directory corrupted")
	ErrExceedMaxBatchSize         = errors.New("max batch size exceeded")
	ErrMergingFileIsInProgress    = errors.New("merging file is in progress")
	ErrFileIsLockedByOtherProcess = errors.New("file is locked by other process")
	ErrDBClosed                   = errors.New("database is closed")
	ErrMergeRatioNotSatisfied     = errors.New("merge ratio not satisfied")
	ErrNotEnoughDiskSpace         = errors.New("not enough disk space")
)
