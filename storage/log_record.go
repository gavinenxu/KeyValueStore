package storage

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTransactionFinished
)

// LogRecordHeader to define the crc (checksum) 4 byte, type 1 byte, keySize max 5 byte, valueSize max 5 byte
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

type LogRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

// LogRecord To record storage written in the disk,
// reason to call it LogRecord, is because the storage is appended to file like Log
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType // Write in the header on disk, needed in memory
}

// LogRecordPos To record the storage position on disks
type LogRecordPos struct {
	Fid    uint32 // File descriptor
	Offset int64
}

// TransactionLogRecord to store log record position in transaction
type TransactionLogRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord while write record into db for log record header and body, return encoded bytes and size of records
// crc (4) + type (1) + keySize ( < 5) + valueSize (< 5) + key + value
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize)

	header[4] = logRecord.Type

	var index = 5
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)

	encodedBytes := make([]byte, size)
	copy(encodedBytes[:index], header[:index])
	// copy key/value byte array
	copy(encodedBytes[index:], logRecord.Key)
	copy(encodedBytes[index+len(logRecord.Key):], logRecord.Value)

	crc := crc32.ChecksumIEEE(encodedBytes[4:])
	binary.LittleEndian.PutUint32(encodedBytes[:4], crc)

	return encodedBytes, int64(size)
}

func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	keySize, n := binary.Varint(buf[index:])
	index += n
	header.keySize = uint32(keySize)

	valueSize, n := binary.Varint(buf[index:])
	index += n
	header.valueSize = uint32(valueSize)

	return header, int64(index)
}

func getLogRecordCRC(logRecord *LogRecord, headerWithoutCRC []byte) uint32 {
	if logRecord == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(headerWithoutCRC[:])
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Value)

	return crc
}
