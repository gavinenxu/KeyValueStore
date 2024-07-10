package bitcask_go

import (
	"bitcask-go/storage"
	"encoding/binary"
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
	sequenceNumber := atomic.AddUint64(&batch.db.sequenceNumber, 1)

	positionMap := make(map[string]*storage.LogRecordPos)

	for _, logRecord := range batch.pendingWrites {
		pos, err := batch.db.appendLogRecord(&storage.LogRecord{
			Key:   encodeLogRecordKeyWithSequenceNumber(logRecord.Key, sequenceNumber),
			Value: logRecord.Value,
			Type:  logRecord.Type,
		})

		if err != nil {
			return err
		}

		positionMap[string(logRecord.Key)] = pos
	}

	// append finish key
	_, err := batch.db.appendLogRecord(&storage.LogRecord{
		Key:  encodeLogRecordKeyWithSequenceNumber(transactionFinishKey, sequenceNumber),
		Type: storage.LogRecordTransactionFinished,
	})
	if err != nil {
		return err
	}

	// sync data
	if batch.config.SyncWrites && batch.db.activeFile != nil {
		if err = batch.db.activeFile.SyncLogRecords(); err != nil {
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

//func (batch *WriteBatch) Rollback() error {
//
//}
//
//func (batch *WriteBatch) Reset() {
//
//}
//
//func (batch *WriteBatch) Write() error {
//
//}
//
//func (batch *WriteBatch) Flush() error {
//
//}

// generate sequenceNumber+key byte
func encodeLogRecordKeyWithSequenceNumber(key []byte, sequenceNumber uint64) []byte {
	sequence := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(sequence, sequenceNumber)

	encodedKey := make([]byte, n+len(key))
	copy(encodedKey[:n], sequence[:n])
	copy(encodedKey[n:], key)
	return encodedKey
}

// decode sequenceNumber+key
func decodeLogRecordKeyWithSequenceNumber(encodedKey []byte) (uint64, []byte) {
	sequenceNumber, n := binary.Uvarint(encodedKey)
	key := encodedKey[n:]
	return sequenceNumber, key
}
