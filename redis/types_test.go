package redis

import (
	bitcask "bitcask-go"
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func destroyRedis(rds *RedisDataStruct, dirPath string) {
	if rds == nil || rds.db == nil {
		return
	}

	if err := rds.db.Close(); err != nil {
		panic(err)
	}

	if err := os.RemoveAll(dirPath); err != nil {
		panic(err)
	}
}

func TestRedis_NewRedisDataStruct(t *testing.T) {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "redis-data-struct")
	configs.DirPath = dir

	dataStruct, err := NewRedisDataStruct(configs)
	defer destroyRedis(dataStruct, dir)
	assert.NotNil(t, dataStruct)
	assert.Nil(t, err)
}

func TestRedis_StringSet(t *testing.T) {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "redis-string-put")
	configs.DirPath = dir

	dataStruct, err := NewRedisDataStruct(configs)
	defer destroyRedis(dataStruct, dir)
	assert.NotNil(t, dataStruct)
	assert.Nil(t, err)

	// key nil
	err = dataStruct.Set(nil, utils.GenerateRandomValue(1<<3), 0)
	assert.Nil(t, err)

	// value nil
	err = dataStruct.Set(utils.GenerateTestKey(1), nil, 0)
	assert.Nil(t, err)

	n := 3
	for i := 0; i < n; i++ {
		err = dataStruct.Set(utils.GenerateTestKey(i), utils.GenerateRandomValue(1<<i), 0)
		assert.Nil(t, err)
	}
}

func TestRedis_StringGet(t *testing.T) {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "redis-string-get")
	configs.DirPath = dir

	dataStruct, err := NewRedisDataStruct(configs)
	defer destroyRedis(dataStruct, dir)
	assert.NotNil(t, dataStruct)
	assert.Nil(t, err)

	// key nil
	val, err := dataStruct.Get(nil)
	assert.Nil(t, err)
	assert.Nil(t, val)

	// no expire
	key := utils.GenerateTestKey(1)
	val = utils.GenerateRandomValue(1 << 1)
	err = dataStruct.Set(key, val, 0)
	assert.Nil(t, err)

	valGet, err := dataStruct.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, val, valGet)

	// no key found
	_, err = dataStruct.Get([]byte("not-found"))
	assert.NotNil(t, err)
	assert.Equal(t, bitcask.ErrKeyNotFound, err)

	// set expire 1 nano second
	err = dataStruct.Set(key, val, 1*time.Nanosecond)
	assert.Nil(t, err)

	_, err = dataStruct.Get(key)
	assert.NotNil(t, err)
	assert.Equal(t, ErrKeyIsExpired, err)
}

func TestRedis_StringDel(t *testing.T) {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "redis-string-delete")
	configs.DirPath = dir

	dataStruct, err := NewRedisDataStruct(configs)
	defer destroyRedis(dataStruct, dir)
	assert.NotNil(t, dataStruct)
	assert.Nil(t, err)

	// nil key
	err = dataStruct.Del(nil)
	assert.Nil(t, err)

	// not found key
	_, err = dataStruct.Get([]byte("not-found"))
	assert.NotNil(t, err)
	assert.Equal(t, bitcask.ErrKeyNotFound, err)

	// no expire
	key := utils.GenerateTestKey(1)
	val := utils.GenerateRandomValue(1 << 3)
	err = dataStruct.Set(key, val, 0)
	assert.Nil(t, err)
	getVal, err := dataStruct.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, val, getVal)

	err = dataStruct.Del(key)
	assert.Nil(t, err)
	_, err = dataStruct.Get(utils.GenerateTestKey(1))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}

func TestRedis_StringType(t *testing.T) {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "redis-string-delete")
	configs.DirPath = dir

	dataStruct, err := NewRedisDataStruct(configs)
	defer destroyRedis(dataStruct, dir)
	assert.NotNil(t, dataStruct)
	assert.Nil(t, err)

	// not found key
	_, err = dataStruct.Type([]byte("not-found"))
	assert.NotNil(t, err)
	assert.Equal(t, bitcask.ErrKeyNotFound, err)

	key := utils.GenerateTestKey(1)
	err = dataStruct.Set(key, utils.GenerateRandomValue(1<<3), 0)
	assert.Nil(t, err)

	typ, err := dataStruct.Type(key)
	assert.Nil(t, err)
	assert.Equal(t, typ, "string")
}
