package bitcask_go

import (
	"bitcask-go/index"
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
)

func destroyMergeDir(db *DB) {
	if db != nil {
		dirPath := db.getMergeDirPath()
		if err := os.RemoveAll(dirPath); err != nil {
			panic(err)
		}
	}
}

func TestDB_Merge_EmptyFiles(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_merge")
	configs.DirPath = dir

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	err = database.Merge()
	defer destroyMergeDir(database)
	assert.Nil(t, err)
}

func TestDB_Merge_AllValidLogRecords(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_merge")
	configs.DirPath = dir
	configs.DataFileSize = 8 * 1024 * 1024
	configs.MergeRatio = 0

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	n := 500
	if configs.IndexerType == index.BPlusTreeIndexType {
		n /= 100
	}

	keyValMap := make(map[string][]byte)
	for i := 0; i < n; i++ {
		key, val := utils.GenerateTestKey(i), utils.GenerateRandomValue(1<<6)
		err = database.Put(key, val)
		keyValMap[string(key)] = val
		assert.Nil(t, err)
	}

	err = database.Merge()
	defer destroyMergeDir(database)
	assert.Nil(t, err)

	// restart DB
	err = database.Close()
	assert.Nil(t, err)

	// load from merge file
	database, err = OpenDatabase(configs)
	assert.Nil(t, err)

	keys := database.ListKeys()
	assert.Equal(t, n, len(keys))

	for i := 0; i < n; i++ {
		key := utils.GenerateTestKey(i)
		val, err := database.Get(key)
		valOld, _ := keyValMap[string(key)]
		assert.Nil(t, err)
		assert.Equal(t, val, valOld)
		assert.Equal(t, utils.GenerateTestKey(i), keys[i])
	}
}

func TestDB_Merge_AllInValidLogRecords(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_merge")
	configs.DirPath = dir
	configs.DataFileSize = 8 * 1024 * 1024
	configs.MergeRatio = 0

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	n := 500
	if configs.IndexerType == index.BPlusTreeIndexType {
		n /= 100
	}
	for i := 0; i < n; i++ {
		key, val := utils.GenerateTestKey(i), utils.GenerateRandomValue(1<<6)
		err = database.Put(key, val)
		assert.Nil(t, err)
	}

	for i := 0; i < n; i++ {
		key := utils.GenerateTestKey(i)
		err = database.Delete(key)
		assert.Nil(t, err)
	}

	err = database.Merge()
	defer destroyMergeDir(database)
	assert.Nil(t, err)

	// restart DB
	err = database.Close()
	assert.Nil(t, err)

	// load from merge file
	database, err = OpenDatabase(configs)
	assert.Nil(t, err)

	keys := database.ListKeys()
	assert.Equal(t, 0, len(keys))
}

func TestDB_Merge_UpdateAllLogRecords(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_merge")
	configs.DirPath = dir
	configs.DataFileSize = 8 * 1024 * 1024
	configs.MergeRatio = 0

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	n := 500
	if configs.IndexerType == index.BPlusTreeIndexType {
		n /= 100
	}
	keyValMap := make(map[string][]byte)
	for i := 0; i < n; i++ {
		key, val := utils.GenerateTestKey(i), utils.GenerateRandomValue(1<<6)
		keyValMap[string(key)] = val
		err = database.Put(key, val)
		assert.Nil(t, err)
	}

	for i := 0; i < n; i++ {
		key, val := utils.GenerateTestKey(i), utils.GenerateRandomValue(1<<3)
		keyValMap[string(key)] = val
		err = database.Put(key, val)
		assert.Nil(t, err)
	}

	err = database.Merge()
	defer destroyMergeDir(database)
	assert.Nil(t, err)

	// restart DB
	err = database.Close()
	assert.Nil(t, err)

	// load from merge file
	database, err = OpenDatabase(configs)
	assert.Nil(t, err)

	keys := database.ListKeys()
	assert.Equal(t, n, len(keys))

	for i := 0; i < n; i++ {
		key := utils.GenerateTestKey(i)
		val, err := database.Get(key)
		valOld, _ := keyValMap[string(key)]
		assert.Nil(t, err)
		assert.Equal(t, val, valOld)
		assert.Equal(t, utils.GenerateTestKey(i), keys[i])
	}
}

func TestDB_Merge_DeleteKeysAndUpdateKeys(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_merge")
	configs.DirPath = dir
	configs.DataFileSize = 8 * 1024 * 1024
	configs.MergeRatio = 0

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	n := 500
	if configs.IndexerType == index.BPlusTreeIndexType {
		n /= 100
	}
	keyValMap := make(map[string][]byte)
	for i := 0; i < n; i++ {
		key, val := utils.GenerateTestKey(i), utils.GenerateRandomValue(1<<6)
		err = database.Put(key, val)
		keyValMap[string(key)] = val
		assert.Nil(t, err)
	}

	n1 := 100
	if configs.IndexerType == index.BPlusTreeIndexType {
		n1 /= 100
	}
	// delete key
	for i := 0; i < n1; i++ {
		key := utils.GenerateTestKey(i)
		err = database.Delete(utils.GenerateTestKey(i))
		delete(keyValMap, string(key))
		assert.Nil(t, err)
	}

	// update key
	n2 := 400
	if configs.IndexerType == index.BPlusTreeIndexType {
		n2 /= 100
	}
	for i := n2; i < n; i++ {
		key, val := utils.GenerateTestKey(i), utils.GenerateRandomValue(1<<6)
		err = database.Put(utils.GenerateTestKey(i), val)
		keyValMap[string(key)] = val
		assert.Nil(t, err)
	}

	err = database.Merge()
	defer destroyMergeDir(database)
	assert.Nil(t, err)

	// restart DB
	err = database.Close()
	assert.Nil(t, err)

	// load from merge file
	database, err = OpenDatabase(configs)
	assert.Nil(t, err)

	keys := database.ListKeys()
	assert.Equal(t, n-n1, len(keys))

	for i := n1; i < n; i++ {
		key := utils.GenerateTestKey(i)
		val, err := database.Get(key)
		valOld, _ := keyValMap[string(key)]
		assert.Nil(t, err)
		assert.Equal(t, val, valOld)
		assert.Equal(t, utils.GenerateTestKey(i), keys[i-n1])
	}
}

func TestDB_Merge_NewDataIsWritingWhileMerging(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_merge")
	configs.DirPath = dir
	configs.DataFileSize = 8 * 1024 * 1024
	configs.MergeRatio = 0

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	n := 500
	if configs.IndexerType == index.BPlusTreeIndexType {
		n /= 100
	}
	keyValMap := make(map[string][]byte)
	for i := 0; i < n; i++ {
		key, val := utils.GenerateTestKey(i), utils.GenerateRandomValue(1<<6)
		err = database.Put(key, val)
		keyValMap[string(key)] = val
		assert.Nil(t, err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	n2 := 100
	if configs.IndexerType == index.BPlusTreeIndexType {
		n2 /= 100
	}
	go func() {
		defer wg.Done()
		// delete all
		for i := 0; i < n; i++ {
			key := utils.GenerateTestKey(i)
			err = database.Delete(utils.GenerateTestKey(i))
			delete(keyValMap, string(key))
			assert.Nil(t, err)
		}

		// add new data
		for i := n; i < n+n2; i++ {
			key, val := utils.GenerateTestKey(i), utils.GenerateRandomValue(1<<3)
			err = database.Put(utils.GenerateTestKey(i), val)
			keyValMap[string(key)] = val
			assert.Nil(t, err)
		}

	}()

	err = database.Merge()
	defer destroyMergeDir(database)
	assert.Nil(t, err)

	wg.Wait()

	// restart DB
	err = database.Close()
	assert.Nil(t, err)

	// load from merge file
	database, err = OpenDatabase(configs)
	assert.Nil(t, err)

	keys := database.ListKeys()
	assert.Equal(t, n2, len(keys))

	for i := n; i < n+n2; i++ {
		key := utils.GenerateTestKey(i)
		val, err := database.Get(key)
		valOld, _ := keyValMap[string(key)]
		assert.Nil(t, err)
		assert.Equal(t, val, valOld)
		assert.Equal(t, utils.GenerateTestKey(i), keys[i-n])
	}
}

func TestDB_Merge_ByTwoThreads(t *testing.T) {
	configs := DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_merge")
	configs.DirPath = dir
	configs.DataFileSize = 8 * 1024 * 1024
	configs.MergeRatio = 0

	database, err := OpenDatabase(configs)
	defer destroyDatabase(database)
	assert.Nil(t, err)
	assert.NotNil(t, database)

	err = database.Put(utils.GenerateTestKey(1), utils.GenerateTestKey(1))
	assert.Nil(t, err)

	wg := new(sync.WaitGroup)
	wg.Add(1)
	var res []error

	go func() {
		defer wg.Done()
		err = database.Merge()
		res = append(res, err)
	}()

	err = database.Merge()
	defer destroyMergeDir(database)
	res = append(res, err)

	wg.Wait()

	assert.Equal(t, 2, len(res))

	if res[0] == nil {
		assert.Equal(t, ErrMergingFileIsInProgress, res[1])
	} else {
		assert.Equal(t, ErrMergingFileIsInProgress, res[0])
	}
}
