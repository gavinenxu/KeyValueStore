package storage

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
}

func TestDataFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Close()
	assert.Nil(t, err)
}

func TestDataFile_SyncLogRecords(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.WriteLogRecord([]byte("hello world"))
	assert.Nil(t, err)

	err = dataFile.SyncLogRecords()
	assert.Nil(t, err)
}

func TestDataFile_WriteLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.WriteLogRecord([]byte("hello world"))
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	logRecord := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("world"),
		Type:  LogRecordNormal,
	}
	recordBytes, i := EncodeLogRecord(logRecord)
	assert.NotNil(t, recordBytes)
	assert.Greater(t, i, int64(0))

	err = dataFile.WriteLogRecord(recordBytes)
	assert.Nil(t, err)

	readLogRecord, size, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, i, size)
	assert.Equal(t, logRecord, readLogRecord)

	// write second log record
	logRecord2 := &LogRecord{
		Key:   []byte("hey"),
		Value: []byte("how are u"),
		Type:  LogRecordNormal,
	}
	recordBytes2, i2 := EncodeLogRecord(logRecord2)
	assert.NotNil(t, recordBytes2)
	assert.Greater(t, i2, int64(0))

	err = dataFile.WriteLogRecord(recordBytes2)
	assert.Nil(t, err)

	readLogRecord2, size2, err := dataFile.ReadLogRecord(i)
	assert.Nil(t, err)
	assert.Equal(t, i2, size2)
	assert.Equal(t, logRecord2, readLogRecord2)
}

func TestDataFile_ReadLogRecord_Deleted(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 2)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	logRecord := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("world"),
		Type:  LogRecordDeleted,
	}

	recordBytes, i := EncodeLogRecord(logRecord)
	assert.NotNil(t, recordBytes)
	assert.Greater(t, i, int64(0))

	err = dataFile.WriteLogRecord(recordBytes)
	assert.Nil(t, err)

	recordRead, size, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, i, size)
	assert.Equal(t, logRecord, recordRead)
}
