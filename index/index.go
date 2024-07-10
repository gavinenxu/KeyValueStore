package index

import (
	"bitcask-go/storage"
)

type Indexer interface {
	// Get index position on disk
	Get(key []byte) *storage.LogRecordPos
	// Put Insert index position on disk
	Put(key []byte, pos *storage.LogRecordPos) bool
	// Delete Remove index position on disk
	Delete(key []byte) bool
	// Iterator indexer iterator
	Iterator(reverse bool) Iterator
	Size() int
}

type IndexerType = int8

const (
	BTreeIndexType IndexerType = iota + 1 // BTree index type enumeration

	ARTIndexType // ART?
)

func NewIndexer(typ IndexerType) Indexer {
	switch typ {
	case BTreeIndexType:
		return NewBTree(32) // todo set degree for btree
	case ARTIndexType:
		return nil
	default:
		panic("unsupported index type")
	}
}

// Iterator Index Iterator
type Iterator interface {
	// Rewind set iterator to first element
	Rewind()

	// Seek the first element less/greater than key byte[]
	Seek(key []byte)

	// Next go to next element
	Next()

	// Valid check if has next element
	Valid() bool

	// Key current element key
	Key() []byte

	// Value current element value
	Value() *storage.LogRecordPos

	// Close iterator, free resource
	Close()
}
