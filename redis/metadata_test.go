package redis

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMetaData_encode(t *testing.T) {
	m := &metadata{
		dataType: String,
		expireAt: 1 << 32,
		version:  1 << 16,
		size:     1 << 8,
	}

	enc := encodeMetadata(m)
	b := []byte{0, 128, 128, 128, 128, 32, 128, 128, 8, 128, 2}
	assert.NotNil(t, enc)
	assert.Equal(t, b, enc)
}

func TestMetaData_decode(t *testing.T) {
	b := []byte{0, 128, 128, 128, 128, 32, 128, 128, 8, 128, 2}
	m := decodeMetadata(b)
	assert.NotNil(t, m)
	assert.Equal(t, String, m.dataType)
	assert.Equal(t, int64(1<<32), m.expireAt)
	assert.Equal(t, int64(1<<16), m.version)
	assert.Equal(t, uint32(1<<8), m.size)
}

func TestMetaData_encode_list(t *testing.T) {
	m := &metadata{
		dataType: List,
		expireAt: 1 << 32,
		version:  1 << 16,
		size:     1 << 8,
		head:     1 << 32,
		tail:     1 << 33,
	}

	enc := encodeMetadata(m)
	b := []byte{3, 128, 128, 128, 128, 32, 128, 128, 8, 128, 2, 128, 128, 128, 128, 16, 128, 128, 128, 128, 32}
	assert.NotNil(t, enc)
	assert.Equal(t, b, enc)
}

func TestMetaData_decode_list(t *testing.T) {
	b := []byte{3, 128, 128, 128, 128, 32, 128, 128, 8, 128, 2, 128, 128, 128, 128, 16, 128, 128, 128, 128, 32}
	m := decodeMetadata(b)
	assert.NotNil(t, m)
	assert.Equal(t, List, m.dataType)
	assert.Equal(t, int64(1<<32), m.expireAt)
	assert.Equal(t, int64(1<<16), m.version)
	assert.Equal(t, uint32(1<<8), m.size)
	assert.Equal(t, uint64(1<<32), m.head)
	assert.Equal(t, uint64(1<<33), m.tail)
}
