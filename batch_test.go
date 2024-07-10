package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestWriteBatch_Put(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_writeBatch_put")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	wb := database.NewWriteBatch(DefaultWriteBatchConfig)

	key := utils.GenerateTestKey(1)
	value := utils.GenerateRandomValue(128)
	err = wb.Put(key, value)
	assert.Nil(t, err)

	_, err = database.Get(key)
	assert.Equal(t, ErrKeyNotFound, err)

	err = wb.Commit()
	assert.Nil(t, err)

	val1, err := database.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, value, val1)
}

func TestWriteBatch_Delete(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_writeBatch_delete")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	wb := database.NewWriteBatch(DefaultWriteBatchConfig)

	key := utils.GenerateTestKey(1)
	value := utils.GenerateRandomValue(1 << 6)
	err = database.Put(key, value)
	assert.Nil(t, err)

	val1, err := database.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, value, val1)

	// delete in write batch
	err = wb.Delete(key)
	assert.Nil(t, err)

	val2, err := database.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, value, val2)

	err = wb.Commit()
	assert.Nil(t, err)

	_, err = database.Get(key)
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestWriteBatch_RestartDatabase(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_writeBatch_restart")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	key1 := utils.GenerateTestKey(1)
	val1 := utils.GenerateRandomValue(1 << 6)

	err = database.Put(key1, val1)
	assert.Nil(t, err)

	wb := database.NewWriteBatch(DefaultWriteBatchConfig)
	err = wb.Delete(key1)
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	key2 := utils.GenerateTestKey(2)
	val2 := utils.GenerateRandomValue(1 << 6)
	err = wb.Put(key2, val2)
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// restart
	err = database.Close()
	assert.Nil(t, err)
	database, err = OpenDatabase(configs)
	assert.Nil(t, err)

	_, err = database.Get(key1)
	assert.Equal(t, ErrKeyNotFound, err)

	assert.Equal(t, uint64(2), wb.db.sequenceNumber)
}
