package index

import (
	"bitcask-go/storage"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTree_Get_NilKey(t *testing.T) {
	bt := NewBTree(DefaultDegree)

	bt.Put(nil, &storage.LogRecordPos{Fid: 1, Offset: 10})
	pos := bt.Get(nil)
	assert.Equal(t, uint32(1), pos.Fid)
	assert.Equal(t, int64(10), pos.Offset)
}

func TestBTree_Get_NormalKey(t *testing.T) {
	bt := NewBTree(DefaultDegree)

	bt.Put([]byte("123"), &storage.LogRecordPos{Fid: 1, Offset: 10})
	pos := bt.Get([]byte("123"))
	assert.Equal(t, uint32(1), pos.Fid)
	assert.Equal(t, int64(10), pos.Offset)
}

func TestBTree_Get_NormalKey_UpdatePosition(t *testing.T) {
	bt := NewBTree(DefaultDegree)

	bt.Put([]byte("123"), &storage.LogRecordPos{Fid: 1, Offset: 10})
	bt.Put([]byte("123"), &storage.LogRecordPos{Fid: 2, Offset: 20})

	pos := bt.Get([]byte("123"))
	assert.Equal(t, uint32(2), pos.Fid)
	assert.Equal(t, int64(20), pos.Offset)
}

func TestBTree_Put_NilKey(t *testing.T) {
	bt := NewBTree(DefaultDegree)

	res := bt.Put(nil, &storage.LogRecordPos{Fid: 1, Offset: 10})
	assert.True(t, res)
}

func TestBTree_Put_NormalKey(t *testing.T) {
	bt := NewBTree(DefaultDegree)

	res := bt.Put([]byte("123"), &storage.LogRecordPos{Fid: 1, Offset: 10})
	assert.True(t, res)
}

func TestBTree_Delete_NilKey(t *testing.T) {
	bt := NewBTree(DefaultDegree)

	res := bt.Put(nil, &storage.LogRecordPos{Fid: 1, Offset: 10})
	assert.True(t, res)

	res = bt.Delete(nil)
	assert.True(t, res)
}

func TestBTree_Delete_NormalKey(t *testing.T) {
	bt := NewBTree(DefaultDegree)

	res := bt.Put([]byte("123"), &storage.LogRecordPos{Fid: 1, Offset: 10})
	assert.True(t, res)

	res = bt.Delete([]byte("123"))
	assert.True(t, res)
}

func TestBTree_Iterator(t *testing.T) {
	bt := NewBTree(DefaultDegree)
	iter1 := bt.Iterator(false)
	assert.False(t, iter1.Valid())

	key, pos := []byte("123"), &storage.LogRecordPos{Fid: 1, Offset: 10}
	bt.Put(key, pos)
	iter2 := bt.Iterator(false)
	assert.True(t, iter2.Valid())
	assert.Equal(t, key, iter2.Key())
	assert.Equal(t, pos, iter2.Value())
	iter2.Next()
	assert.False(t, iter2.Valid())
}

func TestBTree_Iterator_TestIteration(t *testing.T) {
	bt := NewBTree(DefaultDegree)

	n := 3
	keyArr := make([][]byte, n)
	valArr := make([]*storage.LogRecordPos, n)
	for i := range n {
		key, value := []byte(string(rune(i))), &storage.LogRecordPos{Fid: 1, Offset: int64(i + 1)}
		bt.Put(key, value)
		keyArr[i] = key
		valArr[i] = value
	}

	iter := bt.Iterator(false)
	var i int
	for iter.Rewind(); iter.Valid(); iter.Next() {
		key := keyArr[i]
		value := valArr[i]
		assert.Equal(t, key, iter.Key())
		assert.Equal(t, value, iter.Value())
		i++
	}

	// test reverse
	iter2 := bt.Iterator(true)
	i = n - 1
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		key := keyArr[i]
		value := valArr[i]
		assert.Equal(t, key, iter2.Key())
		assert.Equal(t, value, iter2.Value())
		i--
	}
}

func TestBTree_Iterator_TestSeek(t *testing.T) {
	bt := NewBTree(DefaultDegree)

	key1, value1 := []byte("aabb"), &storage.LogRecordPos{Fid: 1, Offset: 1}
	key2, value2 := []byte("ccdd"), &storage.LogRecordPos{Fid: 1, Offset: 1}
	key3, value3 := []byte("eeff"), &storage.LogRecordPos{Fid: 1, Offset: 1}

	bt.Put(key1, value1)
	bt.Put(key2, value2)
	bt.Put(key3, value3)

	iter := bt.Iterator(false)
	var i int
	for iter.Seek([]byte("cc")); iter.Valid(); iter.Next() {
		if i == 0 {
			assert.Equal(t, key2, iter.Key())
			assert.Equal(t, value2, iter.Value())
		} else if i == 1 {
			assert.Equal(t, key3, iter.Key())
			assert.Equal(t, value3, iter.Value())
		}
		i++
	}

	iter2 := bt.Iterator(true)
	for iter2.Seek([]byte("cc")); iter2.Valid(); iter2.Next() {
		assert.Equal(t, key1, iter2.Key())
		assert.Equal(t, value1, iter2.Value())
	}
}
