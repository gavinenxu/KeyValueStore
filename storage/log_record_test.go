package storage

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord_Normal(t *testing.T) {
	record := &LogRecord{
		Key:   []byte("key"),
		Value: []byte("value"),
		Type:  LogRecordNormal,
	}

	enc, size := EncodeLogRecord(record)

	assert.NotNil(t, enc)
	assert.Greater(t, size, int64(5))
}

func TestEncodeLogRecord_Normal_ValueEmpty(t *testing.T) {
	record := &LogRecord{
		Key:  []byte("key"),
		Type: LogRecordNormal,
	}

	enc, size := EncodeLogRecord(record)

	assert.NotNil(t, enc)
	assert.Greater(t, size, int64(5))
}

func TestEncodeLogRecord_Deleted(t *testing.T) {
	record := &LogRecord{
		Key:   []byte("key"),
		Value: []byte("value"),
		Type:  LogRecordDeleted,
	}

	enc, size := EncodeLogRecord(record)

	assert.NotNil(t, enc)
	assert.Greater(t, size, int64(5))
}

func TestDecodeLogRecordHeader_Normal(t *testing.T) {
	headerBuf := []byte{186, 103, 192, 80, 0, 6, 10}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(7), size)
	assert.Equal(t, uint32(1354786746), header.crc)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.Equal(t, uint32(3), header.keySize)
	assert.Equal(t, uint32(5), header.valueSize)
}

func TestDecodeLogRecordHeader_Normal_ValueEmpty(t *testing.T) {
	headerBuf := []byte{184, 38, 83, 75, 0, 6, 0}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(7), size)
	assert.Equal(t, uint32(1263740600), header.crc)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.Equal(t, uint32(3), header.keySize)
	assert.Equal(t, uint32(0), header.valueSize)
}

func TestDecodeLogRecordHeader_Deleted(t *testing.T) {
	headerBuf := []byte{122, 184, 78, 145, 1, 6, 10}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(7), size)
	assert.Equal(t, uint32(2437855354), header.crc)
	assert.Equal(t, LogRecordDeleted, header.recordType)
	assert.Equal(t, uint32(3), header.keySize)
	assert.Equal(t, uint32(5), header.valueSize)
}

func TestGetLogRecordCRC(t *testing.T) {
	record := &LogRecord{
		Key:   []byte("key"),
		Value: []byte("value"),
		Type:  LogRecordNormal,
	}

	headerBuf := []byte{186, 103, 192, 80, 0, 6, 10}
	crc := getLogRecordCRC(record, headerBuf[crc32.Size:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(1354786746), crc)
}

func TestGetLogRecordCRC_ValueEmpty(t *testing.T) {
	record := &LogRecord{
		Key:  []byte("key"),
		Type: LogRecordNormal,
	}

	headerBuf := []byte{184, 38, 83, 75, 0, 6, 0}
	crc := getLogRecordCRC(record, headerBuf[crc32.Size:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(1263740600), crc)
}

func TestGetLogRecordCRC_Deleted(t *testing.T) {
	record := &LogRecord{
		Key:   []byte("key"),
		Value: []byte("value"),
		Type:  LogRecordDeleted,
	}

	headerBuf := []byte{122, 184, 78, 145, 1, 6, 10}
	crc := getLogRecordCRC(record, headerBuf[crc32.Size:])

	assert.NotNil(t, crc)
	assert.Equal(t, uint32(2437855354), crc)
}
