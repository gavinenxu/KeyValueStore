package redis

import (
	bitcask "bitcask-go"
	"bitcask-go/utils"
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

func (rds *RedisDataStruct) Close() error {
	return rds.db.Close()
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

// -------------------> Redis Hash <-----------------------------
// Hash store metadata, which indicates the relation for the key filed and value, key -> metadata -> value

// HSet return true if not exist the encoded hash key and error
// To store, [key, metadata], [encHashKey (key+version+filed), value]
func (rds *RedisDataStruct) HSet(key, field, value []byte) (bool, error) {
	meta, err := rds.getMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	encHashKey := encodeHashInternalKey(key, meta.version, field)

	var notExist bool
	if _, err = rds.db.Get(encHashKey); err != nil {
		if errors.Is(err, bitcask.ErrKeyNotFound) {
			notExist = true
		} else {
			return false, err
		}
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchConfig)

	// update meta
	if notExist {
		meta.size++
		_ = wb.Put(key, encodeMetadata(meta))
	}
	// update value
	_ = wb.Put(encHashKey, value)
	if err := wb.Commit(); err != nil {
		return false, err
	}

	return notExist, nil
}

func (rds *RedisDataStruct) HGet(key, field []byte) ([]byte, error) {
	if key == nil || field == nil {
		return nil, nil
	}

	meta, err := rds.getMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	// no value is set
	if meta.size == 0 {
		return nil, nil
	}
	// expire
	if meta.expireAt != 0 && meta.expireAt < time.Now().UnixNano() {
		return nil, ErrKeyIsExpired
	}

	encHashKey := encodeHashInternalKey(key, meta.version, field)
	return rds.db.Get(encHashKey)
}

func (rds *RedisDataStruct) HDel(key, field []byte) (bool, error) {
	if key == nil || field == nil {
		return false, nil
	}

	meta, err := rds.getMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	encHashKey := encodeHashInternalKey(key, meta.version, field)
	if _, err = rds.db.Get(encHashKey); err != nil {
		if errors.Is(err, bitcask.ErrKeyNotFound) {
			return true, nil
		}
		return false, err
	}

	// update meta then delete
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchConfig)
	meta.size--
	// repoint to the previous metadata key
	_ = wb.Put(key, encodeMetadata(meta))
	_ = wb.Delete(encHashKey)
	if err := wb.Commit(); err != nil {
		return false, err
	}

	return true, nil
}

// -------------------> Redis Set <-----------------------------

func (rds *RedisDataStruct) SAdd(key, member []byte) (bool, error) {
	if key == nil || member == nil {
		return false, nil
	}

	meta, err := rds.getMetadata(key, Set)
	if err != nil {
		return false, err
	}

	var ok bool
	encSetKey := encodeSetInternalKey(key, meta.version, member)
	if _, err := rds.db.Get(encSetKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		meta.size++

		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchConfig)
		_ = wb.Put(key, encodeMetadata(meta))
		_ = wb.Put(encSetKey, nil)
		if err := wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

func (rds *RedisDataStruct) SIsMember(key, member []byte) (bool, error) {
	if key == nil || member == nil {
		return false, nil
	}

	meta, err := rds.getMetadata(key, Set)
	if err != nil {
		return false, err
	}

	encSetKey := encodeSetInternalKey(key, meta.version, member)
	if _, err := rds.db.Get(encSetKey); err != nil {
		if errors.Is(err, bitcask.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (rds *RedisDataStruct) SRem(key, member []byte) (bool, error) {
	if key == nil {
		return false, nil
	}

	meta, err := rds.getMetadata(key, Set)
	if err != nil {
		return false, err
	}

	encSetKey := encodeSetInternalKey(key, meta.version, member)
	if _, err := rds.db.Get(encSetKey); err != nil {
		if errors.Is(err, bitcask.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchConfig)
	meta.size--
	_ = wb.Put(key, encodeMetadata(meta))
	_ = wb.Delete(encSetKey)
	if err := wb.Commit(); err != nil {
		return false, err
	}

	return true, nil
}

// -------------------> Redis ZSet <-----------------------------

func (rds *RedisDataStruct) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.getMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	encZSetKey := encodeZSetInternalKeyWithMember(key, meta.version, member)
	scoreByte, err := rds.db.Get(encZSetKey)

	var notExist bool
	if err != nil {
		if errors.Is(err, bitcask.ErrKeyNotFound) {
			notExist = true
		} else {
			return false, err
		}
	} else if score == utils.BytesToFloat64(scoreByte) {
		return false, nil
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchConfig)
	if notExist {
		meta.size++
		_ = wb.Put(key, encodeMetadata(meta))
	} else {
		// delete old score key, so we could get a not duplicate score list
		oldEncZSetKeyWithScore := encodeZSetInternalKeyWithScore(key, meta.version, utils.BytesToFloat64(scoreByte), member)
		_ = wb.Delete(oldEncZSetKeyWithScore)
	}
	_ = wb.Put(encZSetKey, utils.Float64ToBytes(score))
	_ = wb.Put(encodeZSetInternalKeyWithScore(key, meta.version, score, member), nil)
	if err := wb.Commit(); err != nil {
		return false, err
	}

	return notExist, nil
}

func (rds *RedisDataStruct) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.getMetadata(key, ZSet)
	if err != nil {
		return -1.0, err
	}

	encZSetKey := encodeZSetInternalKeyWithMember(key, meta.version, member)
	scoreByte, err := rds.db.Get(encZSetKey)
	if err != nil {
		if errors.Is(err, bitcask.ErrKeyNotFound) {
			return -1.0, nil
		}
		return -1.0, err
	}
	return utils.BytesToFloat64(scoreByte), nil
}

// -------------------> Redis List <-----------------------------

// LPush return current size
func (rds *RedisDataStruct) LPush(key, element []byte) (uint32, error) {
	return rds.push(key, element, true)
}

func (rds *RedisDataStruct) RPush(key, element []byte) (uint32, error) {
	return rds.push(key, element, false)
}

func (rds *RedisDataStruct) LPop(key []byte) ([]byte, error) {
	return rds.pop(key, true)
}

func (rds *RedisDataStruct) RPop(key []byte) ([]byte, error) {
	return rds.pop(key, false)
}

func (rds *RedisDataStruct) push(key, element []byte, isLeft bool) (uint32, error) {
	if key == nil || element == nil {
		return 0, nil
	}

	meta, err := rds.getMetadata(key, List)
	if err != nil {
		return 0, err
	}

	var index uint64
	if isLeft {
		index = meta.head - 1
		meta.head--
	} else {
		index = meta.tail
		meta.tail++
	}

	encListKey := encodeListInternalKey(key, meta.version, index)

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchConfig)
	// update meta
	meta.size++
	_ = wb.Put(key, encodeMetadata(meta))
	_ = wb.Put(encListKey, element)
	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return meta.size, nil
}

func (rds *RedisDataStruct) pop(key []byte, isLeft bool) ([]byte, error) {
	if key == nil {
		return nil, nil
	}

	meta, err := rds.getMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	var index uint64
	if isLeft {
		index = meta.head
		meta.head++
	} else {
		index = meta.tail - 1
		meta.tail--
	}

	encListKey := encodeListInternalKey(key, meta.version, index)
	val, err := rds.db.Get(encListKey)
	if err != nil {
		return nil, err
	}

	meta.size--
	if err := rds.db.Put(key, encodeMetadata(meta)); err != nil {
		return nil, err
	}

	return val, nil

}

// -------------------> Redis Generic methods <-----------------------------

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

func (rds *RedisDataStruct) getMetadata(key []byte, redisDataType RedisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil {
		if errors.Is(err, bitcask.ErrKeyNotFound) {
			m := &metadata{
				dataType: redisDataType,
				expireAt: 0,
				version:  time.Now().UnixNano(),
				size:     0,
			}
			if redisDataType == List {
				m.head = initialListMidPoint
				m.tail = initialListMidPoint
			}
			return m, nil

		}
		return nil, err
	}

	m := decodeMetadata(metaBuf)
	if m.dataType != redisDataType {
		return nil, ErrWrongTypeOperation
	} else if m.expireAt != 0 && m.expireAt <= time.Now().UnixNano() {
		return nil, ErrKeyIsExpired
	}

	return m, nil
}
