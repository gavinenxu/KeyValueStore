package bitcask_go

import (
	"bitcask-go/index"
	"bitcask-go/storage"
	"errors"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	config         Config
	mu             *sync.RWMutex
	activeFile     *storage.DataFile            // active file to write storage
	inactiveFiles  map[uint32]*storage.DataFile // inactive file to read storage only, <fid, *file>
	index          index.Indexer
	fileIds        []int  // only use for loading index
	sequenceNumber uint64 // transaction number, increment by 1
	isMerging      bool   // use for merging files
}

func OpenDatabase(config Config) (*DB, error) {
	if err := checkDbConfig(config); err != nil {
		return nil, err
	}

	// check if dir path exist
	if _, err := os.Stat(config.DirPath); os.IsNotExist(err) {
		// create dir path for user
		if err = os.Mkdir(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// init db instance
	db := &DB{
		config:        config,
		mu:            new(sync.RWMutex),
		inactiveFiles: make(map[uint32]*storage.DataFile),
		index:         index.NewIndexer(config.IndexerType),
	}

	// load merge file
	if err := db.loadMergeFile(); err != nil {
		return nil, err
	}

	// load storage file
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// load hint file
	if err := db.loadHintFile(); err != nil {
		return nil, err
	}

	// load index for log records
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Put To write key/value storage, key could not be empty
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// construct logRecord
	logRecord := &storage.LogRecord{
		Key:            key,
		Value:          value,
		Type:           storage.LogRecordNormal,
		SequenceNumber: nonTransactionSequenceNumber,
	}

	// 1. append log record on disk if got inactive file
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 2. update index
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Get to get storage from key
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	return db.getValueByLogPosition(logRecordPos)
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if logRecordPos := db.index.Get(key); logRecordPos == nil {
		return nil
	}

	logRecord := &storage.LogRecord{
		Key:            key,
		Type:           storage.LogRecordDeleted,
		SequenceNumber: nonTransactionSequenceNumber,
	}

	// write to storage file
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// delete key in index
	if ok := db.index.Delete(key); !ok {
		return ErrIndexDeleteFailed
	}

	return nil
}

func (db *DB) ListKeys() [][]byte {
	keys := make([][]byte, db.index.Size())
	iter := db.index.Iterator(false)
	var idx int
	for iter.Rewind(); iter.Valid(); iter.Next() {
		keys[idx] = iter.Key()
		idx++
	}

	return keys
}

func (db *DB) Fold(fn func(k []byte, v []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iter := db.index.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		value, err := db.getValueByLogPosition(iter.Value())
		if err != nil {
			return err
		}

		if !fn(iter.Key(), value) {
			break
		}
	}
	return nil
}

// Close active and inactive files
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.activeFile.Close(); err != nil {
		return err
	}

	for _, inactiveFile := range db.inactiveFiles {
		if err := inactiveFile.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Sync active file for data persistence, ensure the data is flushed to disk
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.activeFile.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) appendLogRecordWithLock(logRecord *storage.LogRecord) (*storage.LogRecordPos, error) {
	// lock the properties like writeOffset for active file
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.appendLogRecord(logRecord)
}

func (db *DB) appendLogRecord(logRecord *storage.LogRecord) (*storage.LogRecordPos, error) {
	// 1. set active file
	// check if active file exist, otherwise initialize it
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 2. write log record
	// encode log record
	encodeLogRecord, size := storage.EncodeLogRecord(logRecord)
	// check size if beyond limit, then flush to disk
	if db.activeFile.WriteOffset+size > db.config.DataFileSize {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// put current active file to inactive
		db.inactiveFiles[db.activeFile.FileId] = db.activeFile

		// open a new active file
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOffset := db.activeFile.WriteOffset
	if err := db.activeFile.Write(encodeLogRecord); err != nil {
		return nil, err
	}

	// check if you need to flush to db based on configuration
	if db.config.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	pos := &storage.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOffset,
	}
	return pos, nil

}

// set current storage file, must set mutex lock
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = initialDataFileId
	if db.activeFile != nil {
		// file id will be like 001, 002, 003, so we add by 1 each time from prev file number
		initialFileId = db.activeFile.FileId + 1
	}

	// open a new active file
	dataFile, err := storage.OpenDataFile(db.config.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.config.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// traverse the files under the dir, to find .storage extension files
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), storage.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}

			fileIds = append(fileIds, fileId)
		}
	}

	// load file from small number to large
	sort.Ints(fileIds)

	for i, fid := range fileIds {
		dataFile, err := storage.OpenDataFile(db.config.DirPath, uint32(fid))
		if err != nil {
			return err
		}

		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.inactiveFiles[uint32(fid)] = dataFile
		}
	}

	// to set fileId which is used for loading index
	db.fileIds = fileIds

	return nil
}

func (db *DB) loadIndexFromDataFiles() error {
	// database is empty
	if len(db.fileIds) == 0 {
		return nil
	}

	// store the whole log record for a transaction
	var transactionLogRecordMap = make(map[uint64][]*storage.LogRecordPositionPair)
	var currentSequenceNumber = nonTransactionSequenceNumber

	finishMergeFileName := path.Join(db.config.DirPath, storage.MergeFinishFileName)
	var nonMergedFileId uint32 = 0
	if _, err := os.Stat(finishMergeFileName); err == nil {
		fileId, err := getNonMergedFileId(db.config.DirPath)
		if err != nil {
			return err
		}
		nonMergedFileId = fileId
	}

	// traverse file id to get file content
	for i, fid := range db.fileIds {
		var dataFile *storage.DataFile
		var fileId = uint32(fid)

		if fileId < nonMergedFileId {
			continue
		}

		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.inactiveFiles[fileId]
		}

		var offset int64 = 0
		// read each of log record on file until reach to eof
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			logRecordPos := &storage.LogRecordPos{
				Fid:    dataFile.FileId,
				Offset: offset,
			}

			if logRecord.SequenceNumber == nonTransactionSequenceNumber {
				if err = db.updateLogRecordIndex(logRecord, logRecordPos); err != nil {
					return err
				}
			} else {
				if logRecord.Type == storage.LogRecordTransactionFinished {
					// if we encounter transaction finish tag, update index at a time
					for _, transactionLogRecord := range transactionLogRecordMap[logRecord.SequenceNumber] {
						if err = db.updateLogRecordIndex(transactionLogRecord.Record, transactionLogRecord.Pos); err != nil {
							return err
						}
						delete(transactionLogRecordMap, logRecord.SequenceNumber)
					}
				} else {
					transactionLogRecordMap[logRecord.SequenceNumber] =
						append(transactionLogRecordMap[logRecord.SequenceNumber], &storage.LogRecordPositionPair{
							Record: logRecord,
							Pos:    logRecordPos,
						})
				}
			}

			if logRecord.SequenceNumber > currentSequenceNumber {
				currentSequenceNumber = logRecord.SequenceNumber
			}

			offset += size
		}

		// if current file is active file, update WriteOffset from current offset
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOffset = offset
		}
	}

	db.sequenceNumber = currentSequenceNumber

	return nil
}

func (db *DB) getValueByLogPosition(logRecordPos *storage.LogRecordPos) ([]byte, error) {
	// get storage file from file id
	var dataFile *storage.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.inactiveFiles[logRecordPos.Fid]
	}

	// storage file is empty
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// read storage based on offset
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == storage.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

func (db *DB) updateLogRecordIndex(logRecord *storage.LogRecord, logRecordPos *storage.LogRecordPos) error {
	// build index
	// 1,check if log record has been deleted, if did, then delete it from index (while it's not been merged for log records)
	if logRecord.Type == storage.LogRecordDeleted {
		if ok := db.index.Delete(logRecord.Key); !ok {
			return ErrIndexDeleteFailed
		}
	} else {
		logRecordPos := &storage.LogRecordPos{Fid: logRecordPos.Fid, Offset: logRecordPos.Offset}
		if ok := db.index.Put(logRecord.Key, logRecordPos); !ok {
			return ErrIndexUpdateFailed
		}
	}
	return nil
}

func checkDbConfig(config Config) error {
	if config.DirPath == "" {
		return errors.New("database dir path is empty")
	}

	if config.DataFileSize <= 0 {
		return errors.New("database storage file size less than or equal to zero")
	}

	return nil
}
