package bitcask_go

import (
	"bitcask-go/storage"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
)

// Merge inactive files' log records, will create new merged file and hint file for all data file while call this function
func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergingFileIsInProgress
	}

	db.isMerging = true
	db.mu.Unlock()
	defer func() {
		db.isMerging = false
	}()

	// we ensure there is only one thread will execute the following part

	// update active file
	db.inactiveFiles[db.activeFile.FileId] = db.activeFile
	if err := db.setActiveDataFile(); err != nil {
		return err
	}

	var nonMergeFileId uint32 = db.activeFile.FileId

	var needMergeFiles []*storage.DataFile
	for _, file := range db.inactiveFiles {
		needMergeFiles = append(needMergeFiles, file)
	}

	sort.Slice(needMergeFiles, func(i, j int) bool {
		return needMergeFiles[i].FileId < needMergeFiles[j].FileId
	})

	mergeDirPath := db.getMergeDirPath()
	if err := buildMergeDirectory(mergeDirPath); err != nil {
		return err
	}

	// init another database instance to handle merge
	mergeDb, err := newMergeDatabase(mergeDirPath)
	if err != nil {
		return err
	}

	// todo remove prev hint file ???
	hintFileName := storage.GetHintFileName(db.config.DirPath)
	if _, err := os.Stat(hintFileName); err == nil {
		if err := os.Remove(hintFileName); err != nil {
			return err
		}
	}

	hintFile, err := storage.OpenHintFile(db.config.DirPath)
	if err != nil {
		return err
	}

	// iterate each of need to be merged files to find the current data we're using in memory
	// put the latest record in merge db
	// finally update log record pos into hint file, which is going to load index when we start db
	for _, dataFile := range needMergeFiles {
		var offset int64 = 0

		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			logRecordPos := db.index.Get(logRecord.Key)
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileId && logRecordPos.Offset == offset {
				pos, err := mergeDb.appendLogRecord(logRecord)
				if err != nil {
					return err
				}

				encodeLogPosRecord := getEncodeLogRecordForPosition(logRecord.Key, pos)
				if err := hintFile.Write(encodeLogPosRecord); err != nil {
					return err
				}
			}

			offset += size
		}
	}

	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDb.Close(); err != nil {
		return err
	}

	// todo remove prev finish file ???
	finishPrevFileName := path.Join(db.config.DirPath, storage.MergeFinishFileName)
	if _, err := os.Stat(finishPrevFileName); err == nil {
		if err := os.Remove(finishPrevFileName); err != nil {
			return err
		}
	}

	// add the merge finish file
	finishFile, err := storage.OpenMergeFinishFile(mergeDirPath)
	if err != nil {
		return err
	}
	finishRecordBuf, _ := storage.EncodeLogRecord(&storage.LogRecord{
		Key:            []byte(mergeFinishKey),
		Value:          []byte(strconv.Itoa(int(nonMergeFileId))),
		Type:           storage.LogRecordNormal,
		SequenceNumber: nonTransactionSequenceNumber,
	})
	if err := finishFile.Write(finishRecordBuf); err != nil {
		return err
	}
	if err := finishFile.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) loadMergeFile() error {
	mergeDirPath := db.getMergeDirPath()

	// 1. check if merge file exists and remove merge dir
	if _, err := os.Stat(mergeDirPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer func() {
		_ = os.RemoveAll(mergeDirPath)
	}()

	dirEntries, err := os.ReadDir(mergeDirPath)
	if err != nil {
		return err
	}

	var mergeFileNames []string
	var mergeFinished bool
	for _, entry := range dirEntries {
		if entry.Name() == storage.MergeFinishFileName {
			mergeFinished = true
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	if !mergeFinished {
		return nil
	}

	// 2. remove all merged data files
	nonMergeFileId, err := getNonMergedFileId(mergeDirPath)
	if err != nil {
		return err
	}

	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		dataFileName := storage.GetDataFileName(db.config.DirPath, fileId)
		if _, err := os.Stat(dataFileName); err == nil {
			if err := os.Remove(dataFileName); err != nil {
				return err
			}
		}
	}

	// 3. move merge file to data file directory and rename it to original data file
	for _, fileName := range mergeFileNames {
		srcFile := path.Join(mergeDirPath, fileName)
		dstFile := path.Join(db.config.DirPath, fileName)
		if err := os.Rename(srcFile, dstFile); err != nil {
			return err
		}
	}

	// move merge finish file
	finishFileNameSrc := path.Join(mergeDirPath, storage.MergeFinishFileName)
	finishFileNameDst := path.Join(db.config.DirPath, storage.MergeFinishFileName)
	if err := os.Rename(finishFileNameSrc, finishFileNameDst); err != nil {
		return err
	}

	return nil
}

// loadHintFile to load index from hint file
func (db *DB) loadHintFile() error {
	hintFileName := storage.GetHintFileName(db.config.DirPath)
	if _, err := os.Stat(hintFileName); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	hintFile, err := storage.OpenHintFile(db.config.DirPath)
	if err != nil {
		return err
	}

	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		logRecordPos, _ := storage.DecodeLogRecordPosition(logRecord.Value)
		db.index.Put(logRecord.Key, logRecordPos)
		offset += size
	}

	return nil
}

func (db *DB) getMergeDirPath() string {
	dir := path.Dir(db.config.DirPath)
	base := path.Base(db.config.DirPath)
	return path.Join(dir, base+mergeDirNameSuffix)
}

func getNonMergedFileId(dirPath string) (uint32, error) {
	finishFile, err := storage.OpenMergeFinishFile(dirPath)
	if err != nil {
		return 0, err
	}

	finishRecord, _, err := finishFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}

	nonMergeFileId, err := strconv.Atoi(string(finishRecord.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

func buildMergeDirectory(dirPath string) error {
	// file exist
	if _, err := os.Stat(dirPath); err == nil {
		if err := os.RemoveAll(dirPath); err != nil {
			return err
		}
	}

	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return err
	}

	return nil
}

func newMergeDatabase(dirPath string) (*DB, error) {
	mergeConfig := DefaultConfig
	mergeConfig.DirPath = dirPath
	return OpenDatabase(mergeConfig)
}

func getEncodeLogRecordForPosition(key []byte, pos *storage.LogRecordPos) []byte {
	encodePos, _ := storage.EncodeLogRecordPosition(pos)
	logRecord := &storage.LogRecord{
		Key:            key,
		Value:          encodePos,
		Type:           storage.LogRecordNormal,
		SequenceNumber: nonTransactionSequenceNumber,
	}

	encodeLogRecord, _ := storage.EncodeLogRecord(logRecord)
	return encodeLogRecord
}
