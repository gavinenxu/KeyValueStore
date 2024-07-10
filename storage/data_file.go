package storage

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid CRC, log record might be corrupted")
)

const DataFileNameSuffix = ".data"

type DataFile struct {
	FileId      uint32
	WriteOffset int64
	IOManager   fio.IOManager
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := buildFileName(dirPath, fileId)

	// Construct IO Manager
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileId:      fileId,
		WriteOffset: 0,
		IOManager:   ioManager,
	}, nil
}

// ReadLogRecord read log record from read offset
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// To read the size of header, which can't be beyond of file size
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+headerBytes > fileSize {
		headerBytes = fileSize - offset
	}

	// 1. Read log record header based on Maximum size
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(headerBuf)
	// reach to the end of file, return eof error
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	logRecord := &LogRecord{
		Type: header.recordType,
	}
	// read real key/value storage
	kyBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
	if err != nil {
		return nil, 0, err
	}

	// Store key/value as byte[], and get it as byte[] so don't need to decode
	logRecord.Key = kyBuf[:keySize]
	logRecord.Value = kyBuf[keySize:]

	// verify crc
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, headerSize + keySize + valueSize, nil
}

func (df *DataFile) WriteLogRecord(buf []byte) error {
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOffset += int64(n)
	return nil
}

func (df *DataFile) SyncLogRecords() error {
	return df.IOManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

func buildFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IOManager.Read(b, offset)
	if err != nil {
		return nil, err
	}
	return b, nil
}
