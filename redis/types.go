package redis

import (
	bitcask "bitcask-go"
	"encoding/binary"
	"errors"
	"time"
)

type RedisDataType = byte

const (
	String RedisDataType = iota
	Hash
	Set
	List
	ZSet
)

var (
	ErrWrongTypeOperation = errors.New("wrong type operation against a key holding a wrong type of value")
	ErrKeyIsExpired       = errors.New("key is expired")
)

type RedisDataStruct struct {
	db *bitcask.DB
}

func NewRedisDataStruct(config bitcask.Config) (*RedisDataStruct, error) {
	database, err := bitcask.OpenDatabase(config)
	if err != nil {
		return nil, err
	}

	return &RedisDataStruct{db: database}, nil
}

// -------------------> Redis String <-----------------------------

// Set encode type+expire+value in Value
func (rds *RedisDataStruct) Set(key, value []byte, ttl time.Duration) error {
	if key == nil || value == nil {
		return nil
	}

	buf := make([]byte, 1+binary.MaxVarintLen64)

	var index int
	// type
	buf[index] = String
	index++

	// expire
	var expireAt int64
	if ttl > 0 {
		expireAt = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expireAt)

	// value
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	// store in db
	if err := rds.db.Put(key, encValue); err != nil {
		return err
	}

	return nil
}

func (rds *RedisDataStruct) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, nil
	}

	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	var index int
	typ := encValue[index]
	if typ != String {
		return nil, ErrWrongTypeOperation
	}
	index++

	expireAt, n := binary.Varint(encValue[index:])
	if expireAt > 0 && expireAt < time.Now().UnixNano() {
		return nil, ErrKeyIsExpired
	}
	index += n

	return encValue[index:], nil
}

func (rds *RedisDataStruct) Del(key []byte) error {
	if key == nil {
		return nil
	}

	return rds.db.Delete(key)
}

func (rds *RedisDataStruct) Type(key []byte) (string, error) {
	if key == nil {
		return "", nil
	}

	encValue, err := rds.db.Get(key)
	if err != nil {
		return "", err
	}

	if len(encValue) == 0 {
		return "", errors.New("value is empty")
	}

	switch encValue[0] {
	case String:
		return "string", nil
	default:
		panic("not supported data type")
	}

	return "", nil
}
