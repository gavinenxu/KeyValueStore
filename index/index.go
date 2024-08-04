package index

import (
	"bitcask-go/storage"
)

// Indexer Key can't be nil
type Indexer interface {
	// Get index position on disk
	Get(key []byte) *storage.LogRecordPos
	// Put Insert index position on disk
	Put(key []byte, pos *storage.LogRecordPos) *storage.LogRecordPos
	// Delete Remove index position on disk
	Delete(key []byte) (*storage.LogRecordPos, bool)
	// Iterator indexer iterator
	Iterator(reverse bool) Iterator
	Size() int
	Close() error
}

type IndexerType = byte

const (
	BTreeIndexType IndexerType = iota + 1 // BTree index type enumeration
	ARTIndexType                          // ART?
	BPlusTreeIndexType
)

func NewIndexer(typ IndexerType, dirPath string, syncWrites bool) Indexer {
	switch typ {
	case BTreeIndexType:
		return NewBTree(DefaultDegree)
	case ARTIndexType:
		return NewAdaptiveRadixTree()
	case BPlusTreeIndexType:
		return NewBPlusTree(dirPath, syncWrites)
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

// Item To define the shape of value we store, could be used for wrapping iterator
type Item struct {
	key []byte
	pos *storage.LogRecordPos
}
