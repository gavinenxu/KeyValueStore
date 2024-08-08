package redis

import (
	"encoding/binary"
	"math"
)

type metadata struct {
	dataType RedisDataType
	expireAt int64
	version  int64
	size     uint32 // number of items under the hash for different field
	head     uint64 // used in List
	tail     uint64 // Used in List
}

const maxMetadataSize = 1 + 2*binary.MaxVarintLen64 + binary.MaxVarintLen32
const maxMetadataSizeForList = maxMetadataSize + 2*binary.MaxVarintLen64
const initialListMidPoint = math.MaxUint64 / 2

func encodeMetadata(m *metadata) []byte {
	var maxSize uint8
	if m.dataType == List {
		maxSize = maxMetadataSizeForList
	} else {
		maxSize = maxMetadataSize
	}

	buf := make([]byte, maxSize)
	buf[0] = m.dataType

	var index = 1
	index += binary.PutVarint(buf[index:], m.expireAt)
	index += binary.PutVarint(buf[index:], m.version)
	index += binary.PutUvarint(buf[index:], uint64(m.size))

	if m.dataType == List {
		index += binary.PutUvarint(buf[index:], m.head)
		index += binary.PutUvarint(buf[index:], m.tail)
	}

	return buf[:index]
}

func decodeMetadata(data []byte) *metadata {
	m := &metadata{}

	m.dataType = data[0]
	var index = 1
	var n int
	m.expireAt, n = binary.Varint(data[index:])
	index += n
	m.version, n = binary.Varint(data[index:])
	index += n
	size, n := binary.Uvarint(data[index:])
	m.size = uint32(size)
	index += n
	if m.dataType == List {
		m.head, n = binary.Uvarint(data[index:])
		index += n
		m.tail, _ = binary.Uvarint(data[index:])
	}

	return m
}

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func encodeHashInternalKey(key []byte, version int64, field []byte) []byte {
	buf := make([]byte, len(key)+8+len(field))

	var index = 0
	copy(buf[index:index+len(key)], key)
	index += len(key)
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(version))
	index += 8
	copy(buf[index:], field)

	return buf
}

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func encodeSetInternalKey(key []byte, version int64, member []byte) []byte {
	buf := make([]byte, len(key)+8+len(member)+4)

	var index = 0
	copy(buf[index:index+len(key)], key)
	index += len(key)

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(version))
	index += 8

	copy(buf[index:index+len(member)], member)
	index += len(member)

	// add member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(member)))

	return buf
}
