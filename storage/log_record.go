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

const crcSizeInByte = crc32.Size
const invariantSize = 5

// LogRecordHeader to define the crc (checksum) 4 byte, type 1 byte,
// sequenceNumberSize max 3 bit < 1 byte, keySize max 5 byte, valueSize max 5 byte
const maxLogRecordHeaderSize = invariantSize + binary.MaxVarintLen64 + binary.MaxVarintLen32*2

type LogRecordHeader struct {
	crc            uint32
	recordType     LogRecordType
	sequenceNumber uint64
	keySize        uint32
	valueSize      uint32
}

// LogRecord To record storage written in the disk,
// reason to call it LogRecord, is because the storage is appended to file like Log
type LogRecord struct {
	Key            []byte
	Value          []byte
	Type           LogRecordType // Write in the header on disk, needed in memory
	SequenceNumber uint64        // transaction number
}

// LogRecordPos To record the storage position on disks
type LogRecordPos struct {
	Fid           uint32 // File descriptor
	Offset        int64
	LogRecordSize uint32 // LogRecordSize In Byte
}

// LogRecordPositionPair to store log record position in transaction
type LogRecordPositionPair struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord while write record into db for log record header and body, return encoded bytes and size of records
// crc (4) + type (1) + keySize ( < 5) + valueSize (< 5) + transaction number (8) + key + value
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 1. setup header
	header := make([]byte, maxLogRecordHeaderSize)

	header[4] = logRecord.Type

	var index = invariantSize
	// sequenceNumber
	index += binary.PutUvarint(header[index:], logRecord.SequenceNumber)
	// key size
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	// value size
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	// total size
	var size = index + len(logRecord.Key) + len(logRecord.Value)

	// 2. start to copy key/value to encoded
	encodedBytes := make([]byte, size)
	copy(encodedBytes[:index], header[:index])
	// copy key/value byte array
	copy(encodedBytes[index:], logRecord.Key)
	copy(encodedBytes[index+len(logRecord.Key):], logRecord.Value)

	crc := crc32.ChecksumIEEE(encodedBytes[crcSizeInByte:])
	binary.LittleEndian.PutUint32(encodedBytes[:crcSizeInByte], crc)

	return encodedBytes, int64(size)
}

func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= crcSizeInByte {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:crcSizeInByte]),
		recordType: buf[4],
	}

	var index = invariantSize
	// parse sequence number
	sequenceNumber, n := binary.Uvarint(buf[index:])
	index += n
	header.sequenceNumber = sequenceNumber

	// parse key size
	keySize, n := binary.Varint(buf[index:])
	index += n
	header.keySize = uint32(keySize)

	// parse value size
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

func EncodeLogRecordPosition(pos *LogRecordPos) ([]byte, int) {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	index += binary.PutVarint(buf[index:], int64(pos.LogRecordSize))

	return buf[:index], index
}

func DecodeLogRecordPosition(buf []byte) (*LogRecordPos, int) {
	var index = 0
	fid, n := binary.Varint(buf[index:])
	index += n

	offset, n := binary.Varint(buf[index:])
	index += n

	logRecordSize, n := binary.Varint(buf[index:])
	index += n

	return &LogRecordPos{Fid: uint32(fid), Offset: offset, LogRecordSize: uint32(logRecordSize)}, index
}
