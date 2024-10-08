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

func TestRedis_Hash_HSet(t *testing.T) {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "redis-hash")
	configs.DirPath = dir

	dataStruct, err := NewRedisDataStruct(configs)
	defer destroyRedis(dataStruct, dir)
	assert.NotNil(t, dataStruct)
	assert.Nil(t, err)

	val1 := utils.GenerateRandomValue(1 << 3)
	ok1, err := dataStruct.HSet(utils.GenerateTestKey(1), []byte("field1"), val1)
	assert.Nil(t, err)
	assert.True(t, ok1)

	val2 := utils.GenerateRandomValue(1 << 3)
	ok2, err := dataStruct.HSet(utils.GenerateTestKey(1), []byte("field1"), val2)
	assert.Nil(t, err)
	assert.False(t, ok2)
}

func TestRedis_Hash_HGet(t *testing.T) {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "redis-hash")
	configs.DirPath = dir

	dataStruct, err := NewRedisDataStruct(configs)
	defer destroyRedis(dataStruct, dir)
	assert.NotNil(t, dataStruct)
	assert.Nil(t, err)

	val1 := utils.GenerateRandomValue(1 << 3)
	ok1, err := dataStruct.HSet(utils.GenerateTestKey(1), []byte("field1"), val1)
	assert.Nil(t, err)
	assert.True(t, ok1)

	data1, err := dataStruct.HGet(utils.GenerateTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.Equal(t, val1, data1)

	val2 := utils.GenerateRandomValue(1 << 3)
	ok2, err := dataStruct.HSet(utils.GenerateTestKey(1), []byte("field1"), val2)
	assert.Nil(t, err)
	assert.False(t, ok2)

	data2, err := dataStruct.HGet(utils.GenerateTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.Equal(t, val2, data2)

	// not exist data
	_, err = dataStruct.HGet(utils.GenerateTestKey(1), []byte("field-not-exist"))
	assert.NotNil(t, err)
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}

func TestRedis_Hash_HDel(t *testing.T) {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "redis-hash")
	configs.DirPath = dir

	dataStruct, err := NewRedisDataStruct(configs)
	defer destroyRedis(dataStruct, dir)
	assert.NotNil(t, dataStruct)
	assert.Nil(t, err)

	val1 := utils.GenerateRandomValue(1 << 3)
	ok1, err := dataStruct.HSet(utils.GenerateTestKey(1), []byte("field1"), val1)
	assert.Nil(t, err)
	assert.True(t, ok1)

	data1, err := dataStruct.HGet(utils.GenerateTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.Equal(t, val1, data1)

	ok2, err := dataStruct.HDel(utils.GenerateTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.True(t, ok2)

	val2, err := dataStruct.HGet(utils.GenerateTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.Nil(t, val2)

	// delete a deleted key
	ok2, err = dataStruct.HDel(utils.GenerateTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.False(t, ok2)

	//	key not exist
	ok3, err := dataStruct.HDel(utils.GenerateTestKey(1), []byte("field-not-exist"))
	assert.False(t, ok3)
	assert.Nil(t, err)
}

func TestRedis_Set_SAdd(t *testing.T) {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "redis-set")
	configs.DirPath = dir

	dataStruct, err := NewRedisDataStruct(configs)
	defer destroyRedis(dataStruct, dir)
	assert.NotNil(t, dataStruct)
	assert.Nil(t, err)

	ok1, err := dataStruct.SAdd(utils.GenerateTestKey(1), []byte("m1"))
	assert.Nil(t, err)
	assert.True(t, ok1)

	ok2, err := dataStruct.SAdd(utils.GenerateTestKey(2), []byte("m2"))
	assert.Nil(t, err)
	assert.True(t, ok2)

	// same member
	ok3, err := dataStruct.SAdd(utils.GenerateTestKey(1), []byte("m1"))
	assert.Nil(t, err)
	assert.False(t, ok3)
}

func TestRedis_Set_SIsMember(t *testing.T) {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "redis-set")
	configs.DirPath = dir

	dataStruct, err := NewRedisDataStruct(configs)
	defer destroyRedis(dataStruct, dir)
	assert.NotNil(t, dataStruct)
	assert.Nil(t, err)

	_, err = dataStruct.SAdd(utils.GenerateTestKey(1), []byte("m1"))
	assert.Nil(t, err)

	_, err = dataStruct.SAdd(utils.GenerateTestKey(1), []byte("m2"))
	assert.Nil(t, err)

	ok1, err := dataStruct.SIsMember(utils.GenerateTestKey(1), []byte("m1"))
	assert.Nil(t, err)
	assert.True(t, ok1)

	ok2, err := dataStruct.SIsMember(utils.GenerateTestKey(1), []byte("m2"))
	assert.Nil(t, err)
	assert.True(t, ok2)

	ok3, err := dataStruct.SIsMember(utils.GenerateTestKey(2), []byte("m1"))
	assert.Nil(t, err)
	assert.False(t, ok3)
}

func TestRedis_Set_SRem(t *testing.T) {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "redis-set")
	configs.DirPath = dir

	dataStruct, err := NewRedisDataStruct(configs)
	defer destroyRedis(dataStruct, dir)
	assert.NotNil(t, dataStruct)
	assert.Nil(t, err)

	_, err = dataStruct.SAdd(utils.GenerateTestKey(1), []byte("m1"))
	assert.Nil(t, err)

	ok1, err := dataStruct.SRem(utils.GenerateTestKey(1), []byte("m1"))
	assert.Nil(t, err)
	assert.True(t, ok1)

	ok2, err := dataStruct.SIsMember(utils.GenerateTestKey(1), []byte("m1"))
	assert.Nil(t, err)
	assert.False(t, ok2)

	ok3, err := dataStruct.SRem(utils.GenerateTestKey(1), []byte("m2"))
	assert.Nil(t, err)
	assert.False(t, ok3)

	ok4, err := dataStruct.SRem(utils.GenerateTestKey(2), []byte("m1"))
	assert.Nil(t, err)
	assert.False(t, ok4)
}

func TestRedis_List_LPop(t *testing.T) {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "redis-list")
	configs.DirPath = dir

	dataStruct, err := NewRedisDataStruct(configs)
	defer destroyRedis(dataStruct, dir)
	assert.NotNil(t, dataStruct)
	assert.Nil(t, err)

	val1 := []byte("e1")
	s1, err := dataStruct.LPush(utils.GenerateTestKey(1), val1)
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), s1)

	val2 := []byte("e2")
	s2, err := dataStruct.LPush(utils.GenerateTestKey(1), val2)
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), s2)

	val3, err := dataStruct.LPop(utils.GenerateTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, val2, val3)

	val4, err := dataStruct.LPop(utils.GenerateTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, val1, val4)
}

func TestRedis_List_RPop(t *testing.T) {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "redis-list")
	configs.DirPath = dir

	dataStruct, err := NewRedisDataStruct(configs)
	defer destroyRedis(dataStruct, dir)
	assert.NotNil(t, dataStruct)
	assert.Nil(t, err)

	val1 := []byte("e1")
	s1, err := dataStruct.RPush(utils.GenerateTestKey(1), val1)
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), s1)

	val2 := []byte("e2")
	s2, err := dataStruct.RPush(utils.GenerateTestKey(1), val2)
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), s2)

	val3, err := dataStruct.RPop(utils.GenerateTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, val2, val3)

	val4, err := dataStruct.RPop(utils.GenerateTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, val1, val4)
}

func TestRedis_ZSet_ZAdd(t *testing.T) {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "redis-zset")
	configs.DirPath = dir

	dataStruct, err := NewRedisDataStruct(configs)
	defer destroyRedis(dataStruct, dir)
	assert.NotNil(t, dataStruct)
	assert.Nil(t, err)

	ok1, err := dataStruct.ZAdd(utils.GenerateTestKey(1), 1.0, []byte("m1"))
	assert.Nil(t, err)
	assert.True(t, ok1)

	ok2, err := dataStruct.ZAdd(utils.GenerateTestKey(1), 1.0, []byte("m2"))
	assert.Nil(t, err)
	assert.True(t, ok2)

	ok3, err := dataStruct.ZAdd(utils.GenerateTestKey(1), 2.0, []byte("m1"))
	assert.Nil(t, err)
	assert.False(t, ok3)

	ok4, err := dataStruct.ZAdd(utils.GenerateTestKey(1), 1.0, []byte("m1"))
	assert.Nil(t, err)
	assert.False(t, ok4)

	ok5, err := dataStruct.ZAdd(utils.GenerateTestKey(2), 1.0, []byte("m1"))
	assert.Nil(t, err)
	assert.True(t, ok5)
}

func TestRedis_ZSet_ZScore(t *testing.T) {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "redis-zset")
	configs.DirPath = dir

	dataStruct, err := NewRedisDataStruct(configs)
	defer destroyRedis(dataStruct, dir)
	assert.NotNil(t, dataStruct)
	assert.Nil(t, err)

	score1, err := dataStruct.ZScore(utils.GenerateTestKey(1), []byte("m1"))
	assert.Nil(t, err)
	assert.Equal(t, -1.0, score1)

	_, err = dataStruct.ZAdd(utils.GenerateTestKey(1), 1.0, []byte("m1"))
	assert.Nil(t, err)

	_, err = dataStruct.ZAdd(utils.GenerateTestKey(1), 1.0, []byte("m2"))
	assert.Nil(t, err)

	_, err = dataStruct.ZAdd(utils.GenerateTestKey(1), 2.0, []byte("m1"))
	assert.Nil(t, err)

	score2, err := dataStruct.ZScore(utils.GenerateTestKey(1), []byte("m1"))
	assert.Nil(t, err)
	assert.Equal(t, 2.0, score2)

	score3, err := dataStruct.ZScore(utils.GenerateTestKey(1), []byte("m2"))
	assert.Nil(t, err)
	assert.Equal(t, 1.0, score3)
}
