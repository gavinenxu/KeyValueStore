package bitcask_go

import (
	"bitcask-go/index"
	"bitcask-go/storage"
	"sync"
	"sync/atomic"
)

type WriteBatch struct {
	mu            *sync.Mutex
	db            *DB
	config        WriteBatchConfig
	pendingWrites map[string]*storage.LogRecord
}

func (db *DB) NewWriteBatch(config WriteBatchConfig) *WriteBatch {
	if !db.isOpen {
		panic(ErrDBClosed)
	}

	if db.config.IndexerType == index.BPlusTreeIndexType && !db.sequenceNumberFileExist && !db.isInitial {
		panic("failed to open write batch: sequence number file not exist")
	}

	return &WriteBatch{
		mu:            new(sync.Mutex),
		db:            db,
		config:        config,
		pendingWrites: make(map[string]*storage.LogRecord)}
}

// Put data into cache
func (batch *WriteBatch) Put(key, value []byte) error {
	if key == nil || len(key) == 0 {
		return ErrKeyIsEmpty
	}

	batch.mu.Lock()
	defer batch.mu.Unlock()

	if len(batch.pendingWrites) == batch.config.MaxBatchSize {
		return ErrExceedMaxBatchSize
	}

	batch.pendingWrites[string(key)] = &storage.LogRecord{
		Key:   key,
		Value: value,
	}

	return nil
}

// Delete create DeleteRecord in cache
func (batch *WriteBatch) Delete(key []byte) error {
	if key == nil || len(key) == 0 {
		return ErrKeyIsEmpty
	}

	batch.mu.Lock()
	defer batch.mu.Unlock()

	batch.pendingWrites[string(key)] = &storage.LogRecord{
		Key:  key,
		Type: storage.LogRecordDeleted,
	}

	return nil
}

// Commit transaction, write data from cache to file, the key for transaction log record should be different,
// in which includes original key and global incremented transaction key, after succeed we update index in memory
func (batch *WriteBatch) Commit() error {
	batch.mu.Lock()
	defer batch.mu.Unlock()

	if len(batch.pendingWrites) == 0 {
		return nil
	}

	if len(batch.pendingWrites) > batch.config.MaxBatchSize {
		return ErrExceedMaxBatchSize
	}

	// Flush cache to file in serialization
	batch.db.mu.Lock()
	defer batch.db.mu.Unlock()

	// Generate global transaction sequence number
	sequenceNumber := batch.generateGlobalIncrementSequenceNumber()

	// For the same key, we only take the last Position, so it could be overwritten
	positionMap := make(map[string]*storage.LogRecordPos)

	for _, logRecord := range batch.pendingWrites {
		pos, err := batch.db.appendLogRecord(&storage.LogRecord{
			Key:            logRecord.Key,
			Value:          logRecord.Value,
			Type:           logRecord.Type,
			SequenceNumber: sequenceNumber,
		})

		if err != nil {
			return err
		}

		positionMap[string(logRecord.Key)] = pos
	}

	// append finish key
	_, err := batch.db.appendLogRecord(&storage.LogRecord{
		Key:            transactionFinishKey,
		Type:           storage.LogRecordTransactionFinished,
		SequenceNumber: sequenceNumber,
	})
	if err != nil {
		return err
	}

	// sync data
	if batch.config.SyncWrites && batch.db.activeFile != nil {
		if err = batch.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// update index for log record
	for key, logRecord := range batch.pendingWrites {
		pos := positionMap[key]
		if logRecord.Type == storage.LogRecordNormal {
			batch.db.index.Put(logRecord.Key, pos)
		} else if logRecord.Type == storage.LogRecordDeleted {
			batch.db.index.Delete(logRecord.Key)
		}
	}

	// clean up cache
	batch.pendingWrites = make(map[string]*storage.LogRecord)

	return nil
}

func (batch *WriteBatch) generateGlobalIncrementSequenceNumber() uint64 {
	return atomic.AddUint64(&batch.db.sequenceNumber, 1)
}
