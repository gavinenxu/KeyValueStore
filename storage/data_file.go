package storage

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid CRC, log record might be corrupted")
)

const (
	DataFileNameSuffix     = ".data"
	HintFileName           = "hint-index"
	MergeFinishFileName    = "merge-finish"
	SequenceNumberFileName = "sequence-number"
)

type DataFile struct {
	FileId      uint32
	WriteOffset int64
	IOManager   fio.IOManager
}

func OpenDataFile(dirPath string, fileId uint32, ioType fio.IOType) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId, ioType)
}

func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := GetHintFileName(dirPath)
	return newDataFile(fileName, 0, fio.StandardFileIOType)
}

func OpenMergeFinishFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishFileName)
	return newDataFile(fileName, 0, fio.StandardFileIOType)
}

func OpenSequenceNumberFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SequenceNumberFileName)
	return newDataFile(fileName, 0, fio.StandardFileIOType)
}

// ReadLogRecord read log record from read offset
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// To read the size of header, which can't be beyond of file size
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// Can't read out of file
	var headerBytes = min(maxLogRecordHeaderSize, fileSize-offset)

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
		Type:           header.recordType,
		SequenceNumber: header.sequenceNumber,
	}
	// read real key/value storage
	buf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
	if err != nil {
		return nil, 0, err
	}

	// Store key/value as byte[], and get it as byte[] so don't need to decode
	logRecord.Key = buf[:keySize]
	logRecord.Value = buf[keySize:]

	// verify crc
	crc := getLogRecordCRC(logRecord, headerBuf[crcSizeInByte:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, headerSize + keySize + valueSize, nil
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOffset += int64(n)
	return nil
}

func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

func (df *DataFile) SetIOType(dirPath string, ioType fio.IOType) error {
	if err := df.IOManager.Close(); err != nil {
		return err
	}

	fileName := GetDataFileName(dirPath, df.FileId)
	IOManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return err
	}
	df.IOManager = IOManager

	return nil
}

func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	buf := make([]byte, n)
	_, err := df.IOManager.Read(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

func GetHintFileName(dirPath string) string {
	return filepath.Join(dirPath, HintFileName)
}

func newDataFile(fileName string, fileId uint32, ioType fio.IOType) (*DataFile, error) {
	// Construct IO Manager
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileId:      fileId,
		WriteOffset: 0,
		IOManager:   ioManager,
	}, nil
}
