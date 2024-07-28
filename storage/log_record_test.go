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
	assert.Equal(t, enc, []byte{24, 174, 175, 191, 0, 2, 6, 10, 0, 107, 101, 121, 118, 97, 108, 117, 101})
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
	assert.Equal(t, enc, []byte{91, 186, 212, 168, 0, 2, 6, 10, 1, 107, 101, 121, 118, 97, 108, 117, 101})
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
	assert.Equal(t, enc, []byte{230, 95, 53, 81, 0, 2, 6, 0, 0, 107, 101, 121})
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
	assert.Equal(t, enc, []byte{131, 56, 137, 233, 0, 2, 6, 0, 1, 107, 101, 121})
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
	assert.Equal(t, enc, []byte{157, 119, 57, 98, 1, 2, 6, 10, 0, 107, 101, 121, 118, 97, 108, 117, 101})
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
	assert.Equal(t, enc, []byte{222, 99, 66, 117, 1, 2, 6, 10, 1, 107, 101, 121, 118, 97, 108, 117, 101})
}

// Decoder
func TestDecodeLogRecordHeader_Normal(t *testing.T) {
	headerBuf := []byte{24, 174, 175, 191, 0, 2, 6, 10}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(8), size)
	assert.Equal(t, uint32(3215961624), header.crc)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.Equal(t, uint8(1), header.sequenceNumberSize)
	assert.Equal(t, uint32(3), header.keySize)
	assert.Equal(t, uint32(5), header.valueSize)
}

func TestDecodeLogRecordHeader_Normal_WithSequenceNumber(t *testing.T) {
	headerBuf := []byte{91, 186, 212, 168, 0, 2, 6, 10}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(8), size)
	assert.Equal(t, uint32(2832513627), header.crc)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.Equal(t, uint8(1), header.sequenceNumberSize)
	assert.Equal(t, uint32(3), header.keySize)
	assert.Equal(t, uint32(5), header.valueSize)
}

func TestDecodeLogRecordHeader_Normal_ValueEmpty(t *testing.T) {
	headerBuf := []byte{230, 95, 53, 81, 0, 2, 6, 0}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(8), size)
	assert.Equal(t, uint32(1362452454), header.crc)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.Equal(t, uint8(1), header.sequenceNumberSize)
	assert.Equal(t, uint32(3), header.keySize)
	assert.Equal(t, uint32(0), header.valueSize)
}

func TestDecodeLogRecordHeader_Normal_ValueEmpty_WithSequenceNumber(t *testing.T) {
	headerBuf := []byte{131, 56, 137, 233, 0, 2, 6, 0}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(8), size)
	assert.Equal(t, uint32(3918084227), header.crc)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.Equal(t, uint8(1), header.sequenceNumberSize)
	assert.Equal(t, uint32(3), header.keySize)
	assert.Equal(t, uint32(0), header.valueSize)
}

func TestDecodeLogRecordHeader_Deleted(t *testing.T) {
	headerBuf := []byte{157, 119, 57, 98, 1, 2, 6, 10}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(8), size)
	assert.Equal(t, uint32(1647933341), header.crc)
	assert.Equal(t, LogRecordDeleted, header.recordType)
	assert.Equal(t, uint8(1), header.sequenceNumberSize)
	assert.Equal(t, uint32(3), header.keySize)
	assert.Equal(t, uint32(5), header.valueSize)
}

func TestDecodeLogRecordHeader_Deleted_WithSequenceNumber(t *testing.T) {
	headerBuf := []byte{222, 99, 66, 117, 1, 2, 6, 10}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(8), size)
	assert.Equal(t, uint32(1967285214), header.crc)
	assert.Equal(t, LogRecordDeleted, header.recordType)
	assert.Equal(t, uint8(1), header.sequenceNumberSize)
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

	headerBuf := []byte{24, 174, 175, 191, 0, 2, 6, 10}
	crc := getLogRecordCRC(record, headerBuf[crc32.Size:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(3215961624), crc)
}

func TestGetLogRecordCRC_WithSequenceNumber(t *testing.T) {
	record := &LogRecord{
		Key:            []byte("key"),
		Value:          []byte("value"),
		Type:           LogRecordNormal,
		SequenceNumber: uint64(1),
	}

	headerBuf := []byte{91, 186, 212, 168, 0, 2, 6, 10}
	crc := getLogRecordCRC(record, headerBuf[crcSizeInByte:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(2832513627), crc)
}

func TestGetLogRecordCRC_ValueEmpty(t *testing.T) {
	record := &LogRecord{
		Key:  []byte("key"),
		Type: LogRecordNormal,
	}

	headerBuf := []byte{230, 95, 53, 81, 0, 2, 6, 0}
	crc := getLogRecordCRC(record, headerBuf[crcSizeInByte:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(1362452454), crc)
}

func TestGetLogRecordCRC_ValueEmpty_WithSequenceNumber(t *testing.T) {
	record := &LogRecord{
		Key:            []byte("key"),
		Type:           LogRecordNormal,
		SequenceNumber: uint64(1),
	}

	headerBuf := []byte{131, 56, 137, 233, 0, 2, 6, 0}
	crc := getLogRecordCRC(record, headerBuf[crcSizeInByte:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(3918084227), crc)
}

func TestGetLogRecordCRC_Deleted(t *testing.T) {
	record := &LogRecord{
		Key:   []byte("key"),
		Value: []byte("value"),
		Type:  LogRecordDeleted,
	}

	headerBuf := []byte{157, 119, 57, 98, 1, 2, 6, 10}
	crc := getLogRecordCRC(record, headerBuf[crcSizeInByte:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(1647933341), crc)
}

func TestGetLogRecordCRC_Deleted_WithSequenceNumber(t *testing.T) {
	record := &LogRecord{
		Key:            []byte("key"),
		Value:          []byte("value"),
		Type:           LogRecordDeleted,
		SequenceNumber: uint64(1),
	}

	headerBuf := []byte{222, 99, 66, 117, 1, 2, 6, 10}
	crc := getLogRecordCRC(record, headerBuf[crcSizeInByte:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(1967285214), crc)
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
	assert.NotNil(t, enc)
	assert.Greater(t, size, int64(invariantSize))
	assert.Equal(t, enc, []byte{238, 98, 222, 20, 0, 16, 6, 10, 255, 255, 255, 255, 255, 255, 255, 255, 107, 101, 121, 118, 97, 108, 117, 101})
}

func TestDecodeLargeSequenceNumber_Normal(t *testing.T) {
	headerBuf := []byte{238, 98, 222, 20, 0, 16, 6, 10}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(8), size)
	assert.Equal(t, uint32(350118638), header.crc)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.Equal(t, uint8(8), header.sequenceNumberSize)
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

	headerBuf := []byte{238, 98, 222, 20, 0, 16, 6, 10}
	crc := getLogRecordCRC(record, headerBuf[crcSizeInByte:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(350118638), crc)
}

// Test Encode/Decode Log position
func TestEncodeLogRecordPosition(t *testing.T) {
	pos := &LogRecordPos{
		Fid:    128,
		Offset: 256,
	}

	encodedPos, size := EncodeLogRecordPosition(pos)
	assert.NotNil(t, encodedPos)
	assert.Greater(t, size, 0)
	assert.Equal(t, []byte{128, 2, 128, 4}, encodedPos)
}

func TestDecodeLogRecordPosition(t *testing.T) {
	buf := []byte{128, 2, 128, 4}
	pos, size := DecodeLogRecordPosition(buf)
	assert.NotNil(t, pos)
	assert.Equal(t, uint32(128), pos.Fid)
	assert.Equal(t, int64(256), pos.Offset)
	assert.Equal(t, 4, size)
}
