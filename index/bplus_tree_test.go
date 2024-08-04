package index

import (
	"bitcask-go/storage"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestBPlusTree_Get_NilKey(t *testing.T) {
	dirPath := createTmpDir()
	defer removeTmpDir(dirPath)

	bpt := NewBPlusTree(dirPath, true)

	res := bpt.Put(nil, &storage.LogRecordPos{Fid: 1, Offset: 10})
	assert.Nil(t, res)

	pos := bpt.Get(nil)
	assert.Nil(t, pos)
	assert.Equal(t, 0, bpt.Size())
}

func TestBPlusTree_Get_NormalKey(t *testing.T) {
	dirPath := createTmpDir()
	defer removeTmpDir(dirPath)

	bpt := NewBPlusTree(dirPath, true)

	bpt.Put([]byte("123"), &storage.LogRecordPos{Fid: 1, Offset: 10})
	pos := bpt.Get([]byte("123"))
	assert.Equal(t, uint32(1), pos.Fid)
	assert.Equal(t, int64(10), pos.Offset)
	assert.Equal(t, 1, bpt.Size())
}

func TestBPlusTree_Get_NormalKey_UpdatePosition(t *testing.T) {
	dirPath := createTmpDir()
	defer removeTmpDir(dirPath)

	bpt := NewBPlusTree(dirPath, true)

	bpt.Put([]byte("123"), &storage.LogRecordPos{Fid: 1, Offset: 10})
	pos := bpt.Get([]byte("123"))
	assert.Equal(t, uint32(1), pos.Fid)
	assert.Equal(t, int64(10), pos.Offset)
	assert.Equal(t, 1, bpt.Size())

	bpt.Put([]byte("123"), &storage.LogRecordPos{Fid: 2, Offset: 20})

	pos = bpt.Get([]byte("123"))
	assert.Equal(t, uint32(2), pos.Fid)
	assert.Equal(t, int64(20), pos.Offset)
	assert.Equal(t, 1, bpt.Size())
}

func TestBPlusTree_Put_NilKey(t *testing.T) {
	dirPath := createTmpDir()
	defer removeTmpDir(dirPath)

	bpt := NewBPlusTree(dirPath, true)

	res := bpt.Put(nil, &storage.LogRecordPos{Fid: 1, Offset: 10})
	assert.Nil(t, res)
	assert.Equal(t, 0, bpt.Size())
}

func TestBPlusTree_Put_NormalKey(t *testing.T) {
	dirPath := createTmpDir()
	defer removeTmpDir(dirPath)

	bpt := NewBPlusTree(dirPath, true)

	res := bpt.Put([]byte("123"), &storage.LogRecordPos{Fid: 1, Offset: 10})
	assert.Nil(t, res)
	assert.Equal(t, 1, bpt.Size())
	pos := bpt.Get([]byte("123"))
	assert.Equal(t, uint32(1), pos.Fid)
	assert.Equal(t, int64(10), pos.Offset)

	res = bpt.Put([]byte("456"), &storage.LogRecordPos{Fid: 2, Offset: 20})
	assert.Nil(t, res)
	assert.Equal(t, 2, bpt.Size())
	pos = bpt.Get([]byte("456"))
	assert.Equal(t, uint32(2), pos.Fid)
	assert.Equal(t, int64(20), pos.Offset)

	res = bpt.Put([]byte("123"), &storage.LogRecordPos{Fid: 3, Offset: 30})
	assert.NotNil(t, res)
	assert.Equal(t, uint32(1), res.Fid)
	assert.Equal(t, int64(10), res.Offset)
	assert.Equal(t, 2, bpt.Size())
	pos = bpt.Get([]byte("123"))
	assert.Equal(t, uint32(3), pos.Fid)
	assert.Equal(t, int64(30), pos.Offset)
}

func TestBPlusTree_Delete_NilKey(t *testing.T) {
	dirPath := createTmpDir()
	defer removeTmpDir(dirPath)

	bpt := NewBPlusTree(dirPath, true)

	res := bpt.Put(nil, &storage.LogRecordPos{Fid: 1, Offset: 10})
	assert.Nil(t, res)
	assert.Equal(t, 0, bpt.Size())

	res, ok := bpt.Delete(nil)
	assert.False(t, ok)
	assert.Nil(t, res)
	assert.Equal(t, 0, bpt.Size())
}

func TestBPlusTree_Delete_NormalKey(t *testing.T) {
	dirPath := createTmpDir()
	defer removeTmpDir(dirPath)

	bpt := NewBPlusTree(dirPath, true)

	res := bpt.Put([]byte("123"), &storage.LogRecordPos{Fid: 1, Offset: 10})
	assert.Nil(t, res)

	res, ok := bpt.Delete([]byte("123"))
	assert.True(t, ok)
	assert.NotNil(t, res)
	assert.Equal(t, uint32(1), res.Fid)
	assert.Equal(t, int64(10), res.Offset)
	assert.Equal(t, 0, bpt.Size())
}

func TestBPlusTree_Iterator(t *testing.T) {
	dirPath := createTmpDir()
	defer removeTmpDir(dirPath)

	bpt := NewBPlusTree(dirPath, true)

	iter1 := bpt.Iterator(false)
	assert.False(t, iter1.Valid())
	iter1.Close()

	key, pos := []byte("123"), &storage.LogRecordPos{Fid: 1, Offset: 10}
	bpt.Put(key, pos)
	iter2 := bpt.Iterator(false)
	assert.True(t, iter2.Valid())
	assert.Equal(t, key, iter2.Key())
	assert.Equal(t, pos, iter2.Value())
	iter2.Next()
	assert.False(t, iter2.Valid())
	iter2.Close()
}

func TestBPlusTree_Iterator_TestIteration(t *testing.T) {
	dirPath := createTmpDir()
	defer removeTmpDir(dirPath)

	bpt := NewBPlusTree(dirPath, true)

	n := 3
	keyArr := make([][]byte, n)
	valArr := make([]*storage.LogRecordPos, n)
	for i := range n {
		key, value := []byte(string(rune(i))), &storage.LogRecordPos{Fid: 1, Offset: int64(i + 1)}
		bpt.Put(key, value)
		keyArr[i] = key
		valArr[i] = value
	}

	iter := bpt.Iterator(false)
	var i int
	for iter.Rewind(); iter.Valid(); iter.Next() {
		key := keyArr[i]
		value := valArr[i]
		assert.Equal(t, key, iter.Key())
		assert.Equal(t, value, iter.Value())
		i++
	}
	iter.Close()

	// test reverse
	iter2 := bpt.Iterator(true)
	i = n - 1
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		key := keyArr[i]
		value := valArr[i]
		assert.Equal(t, key, iter2.Key())
		assert.Equal(t, value, iter2.Value())
		i--
	}
	iter2.Close()
}

func TestBPlusTree_Iterator_TestSeek(t *testing.T) {
	dirPath := createTmpDir()
	defer removeTmpDir(dirPath)

	bpt := NewBPlusTree(dirPath, true)

	key1, value1 := []byte("aabb"), &storage.LogRecordPos{Fid: 1, Offset: 1}
	key2, value2 := []byte("ccdd"), &storage.LogRecordPos{Fid: 1, Offset: 1}
	key3, value3 := []byte("eeff"), &storage.LogRecordPos{Fid: 1, Offset: 1}

	bpt.Put(key1, value1)
	bpt.Put(key2, value2)
	bpt.Put(key3, value3)

	iter := bpt.Iterator(false)
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
	assert.Equal(t, 2, i)
	iter.Close()

	iter2 := bpt.Iterator(true)
	i = 0
	for iter2.Seek([]byte("cc")); iter2.Valid(); iter2.Next() {
		assert.Equal(t, key1, iter2.Key())
		assert.Equal(t, value1, iter2.Value())
		i++
	}
	assert.Equal(t, 1, i)
	iter2.Close()
}

func createTmpDir() string {
	dir, _ := os.MkdirTemp("", "bplustree_test")
	return dir
}

func removeTmpDir(dirPath string) {
	if err := os.RemoveAll(dirPath); err != nil {
		panic(err)
	}
}
