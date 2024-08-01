package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_iterator")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	iter := database.NewIterator(DefaultIteratorConfig)
	assert.NotNil(t, iter)
	assert.False(t, iter.Valid())
}

func TestIterator_OneRecord(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_iterator")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	err = database.Put([]byte("key"), []byte("value"))
	assert.Nil(t, err)

	iter := database.NewIterator(DefaultIteratorConfig)
	assert.NotNil(t, iter)
	assert.True(t, iter.Valid())
	assert.Equal(t, iter.Key(), []byte("key"))
	val, err := iter.Value()
	assert.Nil(t, err)
	assert.Equal(t, val, []byte("value"))

	iter.Next()
	assert.False(t, iter.Valid())
	iter.Close()
}

func TestIterator_MultipleRecords(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_iterator")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	err = database.Put([]byte("b"), utils.GenerateRandomValue(rand.Intn(3)))
	assert.Nil(t, err)
	err = database.Put([]byte("a"), utils.GenerateRandomValue(rand.Intn(3)))
	assert.Nil(t, err)
	err = database.Put([]byte("c"), utils.GenerateRandomValue(rand.Intn(3)))
	assert.Nil(t, err)

	iter := database.NewIterator(DefaultIteratorConfig)
	assert.NotNil(t, iter)

	res := make([][]byte, 0)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		value, err := iter.Value()
		assert.Nil(t, err)
		assert.NotNil(t, value)
		res = append(res, iter.Key())
	}
	assert.Equal(t, len(res), 3)
	assert.Equal(t, res[0], []byte("a"))
	assert.Equal(t, res[1], []byte("b"))
	assert.Equal(t, res[2], []byte("c"))

	res = res[:0]
	iter.Rewind()
	for iter.Seek([]byte("b")); iter.Valid(); iter.Next() {
		value, err := iter.Value()
		assert.Nil(t, err)
		assert.NotNil(t, value)
		res = append(res, iter.Key())
	}
	assert.Equal(t, len(res), 2)
	assert.Equal(t, res[0], []byte("b"))
	assert.Equal(t, res[1], []byte("c"))
	iter.Close()
}

func TestIterator_MultipleRecords_Reverse(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_iterator")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	err = database.Put([]byte("b"), utils.GenerateRandomValue(rand.Intn(3)))
	assert.Nil(t, err)
	err = database.Put([]byte("a"), utils.GenerateRandomValue(rand.Intn(3)))
	assert.Nil(t, err)
	err = database.Put([]byte("c"), utils.GenerateRandomValue(rand.Intn(3)))
	assert.Nil(t, err)

	// reverse
	iterConfig := DefaultIteratorConfig
	iterConfig.reverse = true
	iter := database.NewIterator(iterConfig)
	assert.NotNil(t, iter)

	res := make([][]byte, 0)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		value, err := iter.Value()
		assert.Nil(t, err)
		assert.NotNil(t, value)
		res = append(res, iter.Key())
	}
	assert.Equal(t, len(res), 3)
	assert.Equal(t, res[0], []byte("c"))
	assert.Equal(t, res[1], []byte("b"))
	assert.Equal(t, res[2], []byte("a"))

	res = res[:0]
	iter.Rewind()
	for iter.Seek([]byte("b")); iter.Valid(); iter.Next() {
		value, err := iter.Value()
		assert.Nil(t, err)
		assert.NotNil(t, value)
		res = append(res, iter.Key())
	}
	assert.Equal(t, len(res), 2)
	assert.Equal(t, res[0], []byte("b"))
	assert.Equal(t, res[1], []byte("a"))
	iter.Close()
}

func TestIterator_MultipleRecords_Prefix(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_iterator")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	err = database.Put([]byte("abcd"), utils.GenerateRandomValue(rand.Intn(3)))
	assert.Nil(t, err)
	err = database.Put([]byte("aefg"), utils.GenerateRandomValue(rand.Intn(3)))
	assert.Nil(t, err)
	err = database.Put([]byte("bxy"), utils.GenerateRandomValue(rand.Intn(3)))
	assert.Nil(t, err)

	// 1. match "a"
	iterConfig := DefaultIteratorConfig
	iterConfig.prefix = []byte("a")
	iter := database.NewIterator(iterConfig)
	assert.NotNil(t, iter)

	res := make([][]byte, 0)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		value, err := iter.Value()
		assert.Nil(t, err)
		assert.NotNil(t, value)
		res = append(res, iter.Key())
	}
	assert.Equal(t, len(res), 2)
	assert.Equal(t, res[0], []byte("abcd"))
	assert.Equal(t, res[1], []byte("aefg"))
	iter.Close()

	// 2. match "b"
	iterConfig.prefix = []byte("b")
	iter = database.NewIterator(iterConfig)
	assert.NotNil(t, iter)

	res = res[:0]
	for iter.Rewind(); iter.Valid(); iter.Next() {
		value, err := iter.Value()
		assert.Nil(t, err)
		assert.NotNil(t, value)
		res = append(res, iter.Key())
	}
	assert.Equal(t, len(res), 1)
	assert.Equal(t, res[0], []byte("bxy"))
	iter.Close()

	// 3. match nothing
	iterConfig.prefix = []byte("abcg")
	iter = database.NewIterator(iterConfig)
	iter.Rewind()
	assert.NotNil(t, iter)
	assert.False(t, iter.Valid())
	iter.Close()
}
