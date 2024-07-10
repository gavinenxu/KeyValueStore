package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func destroyDatabase(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.activeFile.Close()
		}
		err := os.RemoveAll(db.config.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpenDatabase(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_open")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)
}

func TestDB_PutGet_Normal(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_put_get")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	key, val := utils.GenerateTestKey(1), utils.GenerateRandomValue(24)
	err = database.Put(key, val)
	assert.Nil(t, err)
	value, err := database.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, val, value)

	// put same key/value again
	err = database.Put(key, val)
	assert.Nil(t, err)
	value, err = database.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, val, value)
}

func TestDB_PutGet_WriteToNewFileWhileSizeMoreThanThreshold_ReadFromInactiveFile(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_put")
	configs.DirPath = dir
	configs.DataFileSize = 64 * 1024 // set 64 kb file

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	for i := 0; i < 1000; i++ {
		err := database.Put(utils.GenerateTestKey(i), utils.GenerateRandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(database.inactiveFiles))

	val, err := database.Get(utils.GenerateTestKey(1))
	assert.Nil(t, err)
	assert.Greater(t, len(val), 0)
}

func TestDB_Put_KeyEmpty_ReturnKeyEmptyError(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_put")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	// 1. key is nil
	err = database.Put(nil, []byte("1"))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 2. key is empty byte
	err = database.Put([]byte(""), []byte("1"))
	assert.Equal(t, ErrKeyIsEmpty, err)
}

func TestDB_Put_ValueEmpty(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_put")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	// 1. value is nil
	key := utils.GenerateTestKey(1)
	err = database.Put(key, nil)
	assert.Nil(t, err)
	val, err := database.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(val))

	// 2. value is empty byte
	key = utils.GenerateTestKey(2)
	err = database.Put(key, []byte(""))
	assert.Nil(t, err)
	val, err = database.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(val))
}

func TestDB_PutGet_RestartDatabase(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_restart")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	key1 := utils.GenerateTestKey(1)
	val1 := utils.GenerateRandomValue(128)
	err = database.Put(key1, val1)
	assert.Nil(t, err)

	// close active file
	err = database.activeFile.Close()
	assert.Nil(t, err)

	database, err = OpenDatabase(configs)
	assert.Nil(t, err)

	key2 := utils.GenerateTestKey(2)
	val2 := utils.GenerateRandomValue(128)
	err = database.Put(key2, val2)
	assert.Nil(t, err)

	val1Read, err := database.Get(key1)
	assert.Nil(t, err)
	assert.Equal(t, val1, val1Read)

	val2Read, err := database.Get(key2)
	assert.Nil(t, err)
	assert.Equal(t, val2, val2Read)

	_, err = database.Get([]byte("key_not_exist"))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Get_KeyEmpty_ReturnKeyIsEmptyError(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_get")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	_, err = database.Get(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)
}

func TestDB_Get_NotExistKey_ReturnKeyNotFoundError(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_get")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	_, err = database.Get([]byte("not_exist_key"))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Get_ValueOverwriteForSameKey(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_get")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	key := utils.GenerateTestKey(1)
	val1 := utils.GenerateRandomValue(128)
	err = database.Put(key, val1)
	assert.Nil(t, err)

	val1Read, err := database.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, val1, val1Read)

	val2 := utils.GenerateRandomValue(128)
	err = database.Put(key, val2)
	assert.Nil(t, err)
	val2Read, err := database.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, val2, val2Read)
}

func TestDB_Get_DeleteKey_ReturnKeyNotFoundError(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_get")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	key := utils.GenerateTestKey(1)
	val := utils.GenerateRandomValue(128)
	err = database.Put(key, val)
	assert.Nil(t, err)

	valRead, err := database.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, val, valRead)

	err = database.Delete(key)
	assert.Nil(t, err)
	valRead, err = database.Get(key)
	assert.Equal(t, 0, len(valRead))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Delete_Normal(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_delete")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	key, val := utils.GenerateTestKey(1), utils.GenerateRandomValue(128)
	err = database.Put(key, val)
	assert.Nil(t, err)

	err = database.Delete(key)
	assert.Nil(t, err)

	_, err = database.Get(key)
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Delete_KeyNotExist(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_delete")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	err = database.Delete([]byte("not_exist_key"))
	assert.Nil(t, err)
}

func TestDB_Delete_KeyIsEmpty(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_delete")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	err = database.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)
}

func TestDB_Delete_ThenPutSameKey(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_delete")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	key, val1 := utils.GenerateTestKey(1), utils.GenerateRandomValue(128)
	err = database.Put(key, val1)
	assert.Nil(t, err)

	err = database.Delete(key)
	assert.Nil(t, err)

	val2 := utils.GenerateRandomValue(128)
	err = database.Put(key, val2)
	assert.Nil(t, err)

	val2Read, err := database.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, val2, val2Read)
}

func TestDB_ListKeys(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_list_key")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	keys := database.ListKeys()
	assert.Equal(t, 0, len(keys))

	err = database.Put(utils.GenerateTestKey(1), utils.GenerateRandomValue(128))
	assert.Nil(t, err)
	keys = database.ListKeys()
	assert.Equal(t, 1, len(keys))
	assert.Equal(t, utils.GenerateTestKey(1), keys[0])

	err = database.Put(utils.GenerateTestKey(3), utils.GenerateRandomValue(128))
	assert.Nil(t, err)
	err = database.Put(utils.GenerateTestKey(2), utils.GenerateRandomValue(128))
	assert.Nil(t, err)
	keys = database.ListKeys()
	assert.Equal(t, 3, len(keys))
	assert.Equal(t, utils.GenerateTestKey(1), keys[0])

	expectedBytes := [][]byte{utils.GenerateTestKey(1), utils.GenerateTestKey(2), utils.GenerateTestKey(3)}
	assert.Equal(t, expectedBytes, keys)
}

func TestDB_Fold(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_fold")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	err = database.Put(utils.GenerateTestKey(1), utils.GenerateTestKey(11))
	assert.Nil(t, err)
	err = database.Put(utils.GenerateTestKey(2), utils.GenerateTestKey(22))
	assert.Nil(t, err)
	err = database.Put(utils.GenerateTestKey(3), utils.GenerateTestKey(33))
	assert.Nil(t, err)

	var index int
	err = database.Fold(func(k []byte, v []byte) bool {
		key := index + 1
		assert.Equal(t, utils.GenerateTestKey(key), k)
		assert.Equal(t, utils.GenerateTestKey(key*10+key), v)
		index++
		return true
	})
	assert.Nil(t, err)
}

func TestDB_Close(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_close")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	err = database.Close()
	assert.Nil(t, err)

	err = database.Put(utils.GenerateTestKey(1), utils.GenerateTestKey(11))
	assert.Nil(t, err)

	err = database.Close()
	assert.Nil(t, err)

	_, err = database.Get(utils.GenerateTestKey(1))
	assert.NotNil(t, err)
}

func TestDB_Sync(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_sync")
	configs.DirPath = dir

	t.Log(dir)
	database, err := OpenDatabase(configs)
	//defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	err = database.Put(utils.GenerateTestKey(1), utils.GenerateRandomValue(64))
	assert.Nil(t, err)

	err = database.Sync()
	assert.Nil(t, err)
}
