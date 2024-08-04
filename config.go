package bitcask_go

import (
	"bitcask-go/index"
	"os"
)

type Config struct {
	DirPath string // db dir path from Config

	DataFileSize int64 // size of Data file

	SyncWrites bool // To flush storage to disk after each write

	BytesToSync uint

	IndexerType index.IndexerType // index type to indicate which index to use

	EnableMMapAtStart bool // mmap to boost start time

	MergeRatio float32 // ratio to define in which threshold should start merging
}

type IteratorConfig struct {
	reverse bool   // order to iterate
	prefix  []byte // search key's prefix, default is empty
}

type WriteBatchConfig struct {
	MaxBatchSize int
	SyncWrites   bool
}

var DefaultConfig = Config{
	DirPath:           os.TempDir(),
	DataFileSize:      64 * 1024 * 1024, // 64MB
	SyncWrites:        false,
	IndexerType:       index.BPlusTreeIndexType,
	EnableMMapAtStart: true,
	MergeRatio:        0.5,
}

var DefaultIteratorConfig = IteratorConfig{
	reverse: false,
	prefix:  nil,
}

var DefaultWriteBatchConfig = WriteBatchConfig{
	MaxBatchSize: 100_000,
	SyncWrites:   true,
}
