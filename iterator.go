package bitcask_go

import (
	"bitcask-go/index"
	"bytes"
)

// Iterator used for client query
type Iterator struct {
	indexIterator index.Iterator
	db            *DB
	config        IteratorConfig
}

func (db *DB) NewIterator(config IteratorConfig) *Iterator {
	iterator := db.index.Iterator(config.reverse)
	return &Iterator{
		indexIterator: iterator,
		db:            db,
		config:        config,
	}
}

// Rewind set iterator to first element
func (it *Iterator) Rewind() {
	it.indexIterator.Rewind()
	it.skipToNext()
}

// Seek the first element less/greater than key byte[]
func (it *Iterator) Seek(key []byte) {
	it.indexIterator.Seek(key)
	it.skipToNext()
}

// Next go to next element
func (it *Iterator) Next() {
	it.indexIterator.Next()
	it.skipToNext()
}

// Valid check if has next element
func (it *Iterator) Valid() bool {
	return it.indexIterator.Valid()
}

// Key current element key
func (it *Iterator) Key() []byte {
	return it.indexIterator.Key()
}

// Value current element value
func (it *Iterator) Value() ([]byte, error) {
	logPos := it.indexIterator.Value()
	it.db.mu.Lock()
	defer it.db.mu.Unlock()
	return it.db.getValueByLogPosition(logPos)
}

// Close iterator, free resource
func (it *Iterator) Close() {
	it.indexIterator.Close()
}

func (it *Iterator) skipToNext() {
	if len(it.config.prefix) == 0 {
		return
	}

	// To find the key has prefix match with config.prefix
	for ; it.Valid(); it.Next() {
		if bytes.HasPrefix(it.Key(), it.config.prefix) {
			break
		}
	}
}
