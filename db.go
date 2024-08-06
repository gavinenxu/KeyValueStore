package bitcask_go

import (
	"bitcask-go/fio"
	"bitcask-go/index"
	"bitcask-go/storage"
	"bitcask-go/utils"
	"errors"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	config                  Config
	mu                      *sync.RWMutex
	activeFile              *storage.DataFile            // active file to write storage
	inactiveFiles           map[uint32]*storage.DataFile // inactive file to read storage only, <fid, *file>
	index                   index.Indexer
	fileIds                 []int  // only use for loading index
	sequenceNumber          uint64 // transaction number, increment by 1
	isMerging               bool   // use for merging files
	sequenceNumberFileExist bool
	fileLock                *flock.Flock
	totalBytesWritten       uint
	isOpen                  bool
	isInitial               bool  // indicate if Db was used before loading
	reclaimSize             int64 // total size could be reclaimed for merging
}

// Stats Database meta stats
type Stats struct {
	KeyNum                 uint  `json:"keyNumber"`      // number of valid key
	DataFileNum            uint  `json:"dataFileNumber"` // number of valid data files
	ReclaimableSizeInBytes int64 `json:"reclaimSize"`    // size of reclaimable space on disk, only count the data file size
	TotalFileSizeInBytes   int64 `json:"diskSize"`       // total size of files on disk
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

	fileLock, err := acquireFileLock(config)
	if err != nil {
		return nil, err
	}

	// init db instance
	db := &DB{
		config:        config,
		mu:            new(sync.RWMutex),
		inactiveFiles: make(map[uint32]*storage.DataFile),
		index:         index.NewIndexer(config.IndexerType, config.DirPath, config.SyncWrites),
		fileLock:      fileLock,
	}

	// load merge file
	if err := db.loadMergeFile(); err != nil {
		return nil, err
	}

	// load storage file
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	if db.config.IndexerType == index.BPlusTreeIndexType {
		if err := db.loadSequenceNumberFile(); err != nil {
			return nil, err
		}
		// update write offset for bplus tree
		if db.activeFile != nil {
			size, err := db.activeFile.IOManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOffset = size
		}
	} else {
		// load hint file
		if err := db.loadHintFile(); err != nil {
			return nil, err
		}
	}

	// load index for log records
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	// finish loading, set back io type
	if err := db.setDateFileIOType(fio.StandardFileIOType); err != nil {
		return nil, err
	}

	// set db state
	db.isOpen = true
	if db.activeFile == nil {
		db.isInitial = true
	}

	return db, nil
}

// Put To write key/value storage, key could not be empty
func (db *DB) Put(key []byte, value []byte) error {
	if !db.isOpen {
		return ErrDBClosed
	}

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
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 2. update index
	oldPos := db.index.Put(key, pos)

	if oldPos != nil {
		db.reclaimSize += int64(oldPos.LogRecordSize)
	}

	return nil
}

// Get to get storage from key
func (db *DB) Get(key []byte) ([]byte, error) {
	if !db.isOpen {
		return nil, ErrDBClosed
	}

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

	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil
	}

	logRecord := &storage.LogRecord{
		Key:            key,
		Type:           storage.LogRecordDeleted,
		SequenceNumber: nonTransactionSequenceNumber,
	}

	// write to storage file
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// delete key in index
	oldPos, ok := db.index.Delete(logRecord.Key)

	if !ok {
		return ErrIndexDeleteFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.LogRecordSize)
	}
	db.reclaimSize += int64(pos.LogRecordSize)

	return nil
}

func (db *DB) ListKeys() [][]byte {
	keys := make([][]byte, db.index.Size())
	iter := db.index.Iterator(false)
	defer iter.Close()
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
	defer iter.Close()
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
	// To release file lock in any condition and release bplus tree lock
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(err)
		}

		if db.config.IndexerType == index.BPlusTreeIndexType {
			if err := db.index.Close(); err != nil {
				panic(err)
			}
		}
	}()

	db.isOpen = false
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.writeSequenceNumber(); err != nil {
		return err
	}

	if err := db.activeFile.Close(); err != nil {
		return err
	}

	for _, inactiveFile := range db.inactiveFiles {
		if err := inactiveFile.Close(); err != nil {
			return err
		}
	}

	db.activeFile = nil
	db.inactiveFiles = make(map[uint32]*storage.DataFile)

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

func (db *DB) Stats() (Stats, error) {
	var fileNum int
	fileNum += len(db.inactiveFiles)
	if db.activeFile != nil {
		fileNum++
	}

	size, err := utils.DirSize(db.config.DirPath)
	if err != nil {
		return Stats{}, err
	}

	return Stats{
		KeyNum:                 uint(db.index.Size()),
		DataFileNum:            uint(fileNum),
		ReclaimableSizeInBytes: db.reclaimSize,
		TotalFileSizeInBytes:   size,
	}, nil
}

func (db *DB) Backup(path string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return utils.CopyDirWithFiles(db.config.DirPath, path, []string{lockFileName})
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

	db.totalBytesWritten += uint(size)
	// check if you need to flush to db based on configuration
	if db.config.SyncWrites || (db.config.BytesToSync > 0 && db.config.BytesToSync < db.totalBytesWritten) {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		db.totalBytesWritten = 0
	}

	pos := &storage.LogRecordPos{
		Fid:           db.activeFile.FileId,
		Offset:        writeOffset,
		LogRecordSize: uint32(size),
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
	dataFile, err := storage.OpenDataFile(db.config.DirPath, initialFileId, fio.StandardFileIOType)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// open data files
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

	ioType := fio.MMapIOType
	if !db.config.EnableMMapAtStart {
		ioType = fio.StandardFileIOType
	}
	for i, fid := range fileIds {
		dataFile, err := storage.OpenDataFile(db.config.DirPath, uint32(fid), ioType)
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

// traverse all the log records and put the log position in index
// also get current sequence number and write offset for active file
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
	// Only update nonMergedFileId for not bplus tree index, otherwise reload all the index from data file
	if db.config.IndexerType != index.BPlusTreeIndexType {
		if _, err := os.Stat(finishMergeFileName); err == nil {
			fileId, err := getNonMergedFileId(db.config.DirPath)
			if err != nil {
				return err
			}
			nonMergedFileId = fileId
		}
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
				Fid:           dataFile.FileId,
				Offset:        offset,
				LogRecordSize: uint32(size),
			}

			if logRecord.SequenceNumber == nonTransactionSequenceNumber {
				if err = db.updateLogRecordIndex(logRecord, logRecordPos); err != nil {
					return err
				}
			} else {
				// To update a transaction as a whole, keep atomicity
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

// load sequence number for bplus tree index
func (db *DB) loadSequenceNumberFile() error {
	if db.config.IndexerType != index.BPlusTreeIndexType {
		return nil
	}

	fileName := path.Join(db.config.DirPath, storage.SequenceNumberFileName)
	if _, err := os.Stat(fileName); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	seqNoFile, err := storage.OpenSequenceNumberFile(db.config.DirPath)
	if err != nil {
		return err
	}
	seqNoLogRecord, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	db.sequenceNumber = seqNoLogRecord.SequenceNumber
	db.sequenceNumberFileExist = true

	return err
}

func (db *DB) writeSequenceNumber() error {
	if db.config.IndexerType != index.BPlusTreeIndexType {
		return nil
	}

	// store sequence number in file
	seqNoFile, err := storage.OpenSequenceNumberFile(db.config.DirPath)
	if err != nil {
		return err
	}
	seqNoLogRecord := &storage.LogRecord{
		Key:            sequenceNumberKey,
		Value:          nil,
		Type:           storage.LogRecordNormal,
		SequenceNumber: db.sequenceNumber,
	}
	encodedSeqNoBuf, _ := storage.EncodeLogRecord(seqNoLogRecord)

	if err := seqNoFile.Write(encodedSeqNoBuf); err != nil {
		return err
	}

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
	var oldPos *storage.LogRecordPos
	if logRecord.Type == storage.LogRecordDeleted {

		// it's possible key is not on index, but in the log record
		// for example, two thread concurrently executes, one is deleting key and another is merging data file
		// so the delete record will be put into a new active file if
		maybePos := db.index.Get(logRecord.Key)
		if maybePos == nil {
			return nil
		}

		oldPos2, ok := db.index.Delete(logRecord.Key)
		if !ok {
			return ErrIndexDeleteFailed
		}
		oldPos = oldPos2
		db.reclaimSize += int64(logRecordPos.LogRecordSize)
	} else {
		oldPos = db.index.Put(logRecord.Key, logRecordPos)
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.LogRecordSize)
	}

	return nil
}

func (db *DB) setDateFileIOType(ioType fio.IOType) error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOType(db.config.DirPath, ioType); err != nil {
		return err
	}

	for _, inactiveFile := range db.inactiveFiles {
		if err := inactiveFile.SetIOType(db.config.DirPath, ioType); err != nil {
			return err
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

	if config.BytesToSync < 0 {
		return errors.New("database storage file bytes less than zero")
	}

	if config.MergeRatio < 0 || config.MergeRatio > 1 {
		return errors.New("database merge ratio less than 0 or greater than 1")
	}

	return nil
}

func acquireFileLock(config Config) (*flock.Flock, error) {
	// lock file for process
	fileLock := flock.New(filepath.Join(config.DirPath, lockFileName))
	lockedByCurProc, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !lockedByCurProc {
		return nil, ErrFileIsLockedByOtherProcess
	}
	return fileLock, nil
}
