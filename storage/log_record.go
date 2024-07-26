package storage

import (
	"encoding/binary"
	"hash/crc32"
	"math/bits"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTransactionFinished
)

const crcSizeInByte = crc32.Size
const invariantSize = 5
const sequenceNumberMaxSize = 1

// LogRecordHeader to define the crc (checksum) 4 byte, type 1 byte,
// sequenceNumberSize max 3 bit < 1 byte, keySize max 5 byte, valueSize max 5 byte
const maxLogRecordHeaderSize = invariantSize + sequenceNumberMaxSize + binary.MaxVarintLen32*2

type LogRecordHeader struct {
	crc                uint32
	recordType         LogRecordType
	sequenceNumberSize uint8
	keySize            uint32
	valueSize          uint32
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
	Fid    uint32 // File descriptor
	Offset int64
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
	// sequenceNumberSize
	minBytesNeededForSequenceNumber := minBytesNeededForSequenceNumber(logRecord.SequenceNumber)
	index += binary.PutVarint(header[index:], int64(minBytesNeededForSequenceNumber))
	// key size
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	// value size
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	// total size
	var size = index + minBytesNeededForSequenceNumber + len(logRecord.Key) + len(logRecord.Value)

	// 2. start to copy key/value to encoded
	encodedBytes := make([]byte, size)
	copy(encodedBytes[:index], header[:index])
	// copy transaction number
	copy(encodedBytes[index:], uint64ToBytes(logRecord.SequenceNumber))
	// copy key/value byte array
	copy(encodedBytes[index+minBytesNeededForSequenceNumber:], logRecord.Key)
	copy(encodedBytes[index+minBytesNeededForSequenceNumber+len(logRecord.Key):], logRecord.Value)

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
	// parse sequence number size
	sequenceNumberSize, n := binary.Varint(buf[index:])
	index += n
	header.sequenceNumberSize = uint8(sequenceNumberSize)

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
	crc = crc32.Update(crc, crc32.IEEETable, uint64ToBytes(logRecord.SequenceNumber)[:minBytesNeededForSequenceNumber(logRecord.SequenceNumber)])
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Value)

	return crc
}

// minBytesNeededForSequenceNumber set at least 1 byte, even if the sequence number is 0
func minBytesNeededForSequenceNumber(sequenceNumber uint64) int {
	return max(1, minBytesNeeded(sequenceNumber))
}

// To get minimum bytes needed to build uint64, which is strictly less than 8 bytes
func minBytesNeeded(n uint64) int {
	bitCount := bits.Len64(n)
	return (bitCount + 7) / 8
}

// uint64ToBytes converts an uint64 value to a byte slice using little-endian encoding.
func uint64ToBytes(n uint64) []byte {
	bytes := make([]byte, 8) // uint64 is 8 bytes
	binary.LittleEndian.PutUint64(bytes, n)
	return bytes
}

// bytesToUint64 converts a byte slice to an uint64 value using little-endian encoding.
func bytesToUint64(b []byte) uint64 {
	var arr [8]byte
	copy(arr[:], b)
	return binary.LittleEndian.Uint64(arr[:])
}
