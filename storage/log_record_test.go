package storage

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

// encoder
func TestEncodeLogRecord_Normal(t *testing.T) {
	record := &LogRecord{
		Key:            []byte("key"),
		Value:          []byte("value"),
		Type:           LogRecordNormal,
		SequenceNumber: uint64(0),
	}

	enc, size := EncodeLogRecord(record)

	assert.NotNil(t, enc)
	assert.Greater(t, size, int64(invariantSize))
	assert.Equal(t, enc, []byte{57, 16, 146, 64, 0, 0, 6, 10, 107, 101, 121, 118, 97, 108, 117, 101})
}

func TestEncodeLogRecord_Normal_WithSequenceNumber(t *testing.T) {
	record := &LogRecord{
		Key:            []byte("key"),
		Value:          []byte("value"),
		Type:           LogRecordNormal,
		SequenceNumber: uint64(1),
	}

	enc, size := EncodeLogRecord(record)

	assert.NotNil(t, enc)
	assert.Greater(t, size, int64(invariantSize))
	assert.Equal(t, enc, []byte{249, 207, 28, 129, 0, 1, 6, 10, 107, 101, 121, 118, 97, 108, 117, 101})
}

func TestEncodeLogRecord_Normal_ValueEmpty(t *testing.T) {
	record := &LogRecord{
		Key:            []byte("key"),
		Type:           LogRecordNormal,
		SequenceNumber: uint64(0),
	}

	enc, size := EncodeLogRecord(record)

	assert.NotNil(t, enc)
	assert.Greater(t, size, int64(invariantSize))
	assert.Equal(t, enc, []byte{101, 88, 253, 103, 0, 0, 6, 0, 107, 101, 121})
}

func TestEncodeLogRecord_Normal_ValueEmpty_WithSequenceNumber(t *testing.T) {
	record := &LogRecord{
		Key:            []byte("key"),
		Type:           LogRecordNormal,
		SequenceNumber: uint64(1),
	}

	enc, size := EncodeLogRecord(record)

	assert.NotNil(t, enc)
	assert.Greater(t, size, int64(invariantSize))
	assert.Equal(t, enc, []byte{192, 139, 161, 172, 0, 1, 6, 0, 107, 101, 121})
}

func TestEncodeLogRecord_Deleted(t *testing.T) {
	record := &LogRecord{
		Key:   []byte("key"),
		Value: []byte("value"),
		Type:  LogRecordDeleted,
	}

	enc, size := EncodeLogRecord(record)

	assert.NotNil(t, enc)
	assert.Greater(t, size, int64(invariantSize))
	assert.Equal(t, enc, []byte{86, 92, 55, 219, 1, 0, 6, 10, 107, 101, 121, 118, 97, 108, 117, 101})
}

func TestEncodeLogRecord_Deleted_WithSequenceNumber(t *testing.T) {
	record := &LogRecord{
		Key:            []byte("key"),
		Value:          []byte("value"),
		Type:           LogRecordDeleted,
		SequenceNumber: uint64(1),
	}

	enc, size := EncodeLogRecord(record)

	assert.NotNil(t, enc)
	assert.Greater(t, size, int64(invariantSize))
	assert.Equal(t, enc, []byte{150, 131, 185, 26, 1, 1, 6, 10, 107, 101, 121, 118, 97, 108, 117, 101})
}

// Decoder
func TestDecodeLogRecordHeader_Normal(t *testing.T) {
	headerBuf := []byte{57, 16, 146, 64, 0, 0, 6, 10}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(8), size)
	assert.Equal(t, uint32(1083314233), header.crc)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.Equal(t, uint64(0), header.sequenceNumber)
	assert.Equal(t, uint32(3), header.keySize)
	assert.Equal(t, uint32(5), header.valueSize)
}

func TestDecodeLogRecordHeader_Normal_WithSequenceNumber(t *testing.T) {
	headerBuf := []byte{249, 207, 28, 129, 0, 1, 6, 10}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(8), size)
	assert.Equal(t, uint32(2166149113), header.crc)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.Equal(t, uint64(1), header.sequenceNumber)
	assert.Equal(t, uint32(3), header.keySize)
	assert.Equal(t, uint32(5), header.valueSize)
}

func TestDecodeLogRecordHeader_Normal_ValueEmpty(t *testing.T) {
	headerBuf := []byte{101, 88, 253, 103, 0, 0, 6, 0}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(8), size)
	assert.Equal(t, uint32(1744656485), header.crc)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.Equal(t, uint64(0), header.sequenceNumber)
	assert.Equal(t, uint32(3), header.keySize)
	assert.Equal(t, uint32(0), header.valueSize)
}

func TestDecodeLogRecordHeader_Normal_ValueEmpty_WithSequenceNumber(t *testing.T) {
	headerBuf := []byte{192, 139, 161, 172, 0, 1, 6, 0}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(8), size)
	assert.Equal(t, uint32(2896268224), header.crc)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.Equal(t, uint64(1), header.sequenceNumber)
	assert.Equal(t, uint32(3), header.keySize)
	assert.Equal(t, uint32(0), header.valueSize)
}

func TestDecodeLogRecordHeader_Deleted(t *testing.T) {
	headerBuf := []byte{86, 92, 55, 219, 1, 0, 6, 10}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(8), size)
	assert.Equal(t, uint32(3677838422), header.crc)
	assert.Equal(t, LogRecordDeleted, header.recordType)
	assert.Equal(t, uint64(0), header.sequenceNumber)
	assert.Equal(t, uint32(3), header.keySize)
	assert.Equal(t, uint32(5), header.valueSize)
}

func TestDecodeLogRecordHeader_Deleted_WithSequenceNumber(t *testing.T) {
	headerBuf := []byte{150, 131, 185, 26, 1, 1, 6, 10}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(8), size)
	assert.Equal(t, uint32(448365462), header.crc)
	assert.Equal(t, LogRecordDeleted, header.recordType)
	assert.Equal(t, uint64(1), header.sequenceNumber)
	assert.Equal(t, uint32(3), header.keySize)
	assert.Equal(t, uint32(5), header.valueSize)
}

// getLogRecordCrc
func TestGetLogRecordCRC(t *testing.T) {
	record := &LogRecord{
		Key:            []byte("key"),
		Value:          []byte("value"),
		Type:           LogRecordNormal,
		SequenceNumber: uint64(0),
	}

	headerBuf := []byte{57, 16, 146, 64, 0, 0, 6, 10}
	crc := getLogRecordCRC(record, headerBuf[crc32.Size:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(1083314233), crc)
}

func TestGetLogRecordCRC_WithSequenceNumber(t *testing.T) {
	record := &LogRecord{
		Key:            []byte("key"),
		Value:          []byte("value"),
		Type:           LogRecordNormal,
		SequenceNumber: uint64(1),
	}

	headerBuf := []byte{249, 207, 28, 129, 0, 1, 6, 10}
	crc := getLogRecordCRC(record, headerBuf[crcSizeInByte:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(2166149113), crc)
}

func TestGetLogRecordCRC_ValueEmpty(t *testing.T) {
	record := &LogRecord{
		Key:  []byte("key"),
		Type: LogRecordNormal,
	}

	headerBuf := []byte{101, 88, 253, 103, 0, 0, 6, 0}
	crc := getLogRecordCRC(record, headerBuf[crcSizeInByte:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(1744656485), crc)
}

func TestGetLogRecordCRC_ValueEmpty_WithSequenceNumber(t *testing.T) {
	record := &LogRecord{
		Key:            []byte("key"),
		Type:           LogRecordNormal,
		SequenceNumber: uint64(1),
	}

	headerBuf := []byte{192, 139, 161, 172, 0, 1, 6, 0}
	crc := getLogRecordCRC(record, headerBuf[crcSizeInByte:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(2896268224), crc)
}

func TestGetLogRecordCRC_Deleted(t *testing.T) {
	record := &LogRecord{
		Key:   []byte("key"),
		Value: []byte("value"),
		Type:  LogRecordDeleted,
	}

	headerBuf := []byte{86, 92, 55, 219, 1, 0, 6, 10}
	crc := getLogRecordCRC(record, headerBuf[crcSizeInByte:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(3677838422), crc)
}

func TestGetLogRecordCRC_Deleted_WithSequenceNumber(t *testing.T) {
	record := &LogRecord{
		Key:            []byte("key"),
		Value:          []byte("value"),
		Type:           LogRecordDeleted,
		SequenceNumber: uint64(1),
	}

	headerBuf := []byte{150, 131, 185, 26, 1, 1, 6, 10}
	crc := getLogRecordCRC(record, headerBuf[crcSizeInByte:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(448365462), crc)
}

// Test large SequenceNumber
func TestEncodeLargeSequenceNumber_Normal(t *testing.T) {
	record := &LogRecord{
		Key:            []byte("key"),
		Value:          []byte("value"),
		Type:           LogRecordNormal,
		SequenceNumber: uint64(1<<64 - 1),
	}

	enc, size := EncodeLogRecord(record)
	t.Log(enc)
	assert.NotNil(t, enc)
	assert.Greater(t, size, int64(invariantSize))
	assert.Equal(t, enc, []byte{160, 109, 186, 110, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 6, 10, 107, 101, 121, 118, 97, 108, 117, 101})
}

func TestDecodeLargeSequenceNumber_Normal(t *testing.T) {
	headerBuf := []byte{160, 109, 186, 110, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 6, 10}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(17), size)
	assert.Equal(t, uint32(1857711520), header.crc)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.Equal(t, uint64(1<<64-1), header.sequenceNumber)
	assert.Equal(t, uint32(3), header.keySize)
	assert.Equal(t, uint32(5), header.valueSize)
}

func TestGetLogRecordCRC_LargeSequenceNumber(t *testing.T) {
	record := &LogRecord{
		Key:            []byte("key"),
		Value:          []byte("value"),
		Type:           LogRecordNormal,
		SequenceNumber: uint64(1<<64 - 1),
	}

	headerBuf := []byte{160, 109, 186, 110, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 6, 10}
	crc := getLogRecordCRC(record, headerBuf[crcSizeInByte:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(1857711520), crc)
}

// Test Encode/Decode Log position
func TestEncodeLogRecordPosition(t *testing.T) {
	pos := &LogRecordPos{
		Fid:           128,
		Offset:        256,
		LogRecordSize: 10,
	}

	encodedPos, size := EncodeLogRecordPosition(pos)
	assert.NotNil(t, encodedPos)
	assert.Greater(t, size, 0)
	assert.Equal(t, []byte{128, 2, 128, 4, 20}, encodedPos)
}

func TestDecodeLogRecordPosition(t *testing.T) {
	buf := []byte{128, 2, 128, 4, 20}
	pos, size := DecodeLogRecordPosition(buf)
	assert.NotNil(t, pos)
	assert.Equal(t, uint32(128), pos.Fid)
	assert.Equal(t, int64(256), pos.Offset)
	assert.Equal(t, uint32(10), pos.LogRecordSize)
	assert.Equal(t, 5, size)
}
