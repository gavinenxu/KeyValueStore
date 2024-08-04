package index

import (
	"bitcask-go/storage"
	"bytes"
	bbolt "go.etcd.io/bbolt"
	"path/filepath"
)

const bPlusTreeFileName = "bplustree-index"

var BPTreeBucketName = []byte("bitcask-index")

type BPlusTree struct {
	tree *bbolt.DB
}

func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	options := bbolt.DefaultOptions
	options.NoSync = syncWrites

	bPlusTree, err := bbolt.Open(filepath.Join(dirPath, bPlusTreeFileName), 0644, options)
	if err != nil {
		panic("failed to open bplus tree: " + err.Error())
	}
	err = bPlusTree.Update(func(tx *bbolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(BPTreeBucketName)
		return err
	})
	if err != nil {
		panic("failed to create bplus tree bucket: " + err.Error())
	}
	return &BPlusTree{tree: bPlusTree}
}

func (bPlusTree *BPlusTree) Get(key []byte) *storage.LogRecordPos {
	if key == nil {
		return nil
	}

	var pos *storage.LogRecordPos
	_ = bPlusTree.tree.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(BPTreeBucketName)
		posBuf := b.Get(key)
		if len(posBuf) > 0 {
			pos, _ = storage.DecodeLogRecordPosition(posBuf)
		}
		return nil
	})

	return pos
}

func (bPlusTree *BPlusTree) Put(key []byte, pos *storage.LogRecordPos) *storage.LogRecordPos {
	if key == nil {
		return nil
	}

	var oldPos *storage.LogRecordPos
	err := bPlusTree.tree.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(BPTreeBucketName)
		oldItem := b.Get(key)
		if len(oldItem) > 0 {
			oldPos, _ = storage.DecodeLogRecordPosition(oldItem)
		}
		posBuf, _ := storage.EncodeLogRecordPosition(pos)
		return b.Put(key, posBuf)
	})
	if err != nil {
		panic("failed to put index: " + err.Error())
	}

	return oldPos
}

func (bPlusTree *BPlusTree) Delete(key []byte) (*storage.LogRecordPos, bool) {
	if key == nil {
		return nil, false
	}

	var oldPos *storage.LogRecordPos
	err := bPlusTree.tree.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(BPTreeBucketName)
		oldItem := b.Get(key)
		if len(oldItem) > 0 {
			oldPos, _ = storage.DecodeLogRecordPosition(oldItem)
			return b.Delete(key)
		}
		return nil
	})
	if err != nil {
		panic("failed to delete index: " + err.Error())
	}

	return oldPos, oldPos != nil
}

func (bPlusTree *BPlusTree) Size() int {
	var size int
	_ = bPlusTree.tree.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(BPTreeBucketName)
		size = b.Stats().KeyN
		return nil
	})
	return size
}

func (bPlusTree *BPlusTree) Iterator(reverse bool) Iterator {
	if bPlusTree.tree == nil {
		return nil
	}

	iter := newBPlusTreeIterator(bPlusTree, reverse)
	iter.Rewind()
	return iter
}

func (bPlusTree *BPlusTree) Close() error {
	return bPlusTree.tree.Close()
}

type bPlusTreeIterator struct {
	tx      *bbolt.Tx
	cursor  *bbolt.Cursor
	reverse bool
	key     []byte
	value   []byte
}

func newBPlusTreeIterator(bPlusTree *BPlusTree, reverse bool) *bPlusTreeIterator {
	tx, err := bPlusTree.tree.Begin(true)
	if err != nil {
		panic("failed to start a bplus tree transaction: " + err.Error())
	}

	return &bPlusTreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(BPTreeBucketName).Cursor(),
		reverse: reverse,
	}
}

// Rewind set iterator to first element
func (bpti *bPlusTreeIterator) Rewind() {
	if bpti.reverse {
		bpti.key, bpti.value = bpti.cursor.Last()
	} else {
		bpti.key, bpti.value = bpti.cursor.First()
	}
}

// Seek the first element less/greater than key byte[]
func (bpti *bPlusTreeIterator) Seek(key []byte) {
	bpti.key, bpti.value = bpti.cursor.Seek(key)
	// keep the same logic with btree
	if bpti.reverse && bytes.Compare(bpti.key, key) > 0 {
		bpti.key, bpti.value = bpti.cursor.Prev()
	}
}

// Next go to next element
func (bpti *bPlusTreeIterator) Next() {
	if bpti.reverse {
		bpti.key, bpti.value = bpti.cursor.Prev()
	} else {
		bpti.key, bpti.value = bpti.cursor.Next()
	}
}

// Valid check if has next element
func (bpti *bPlusTreeIterator) Valid() bool {
	return len(bpti.key) > 0
}

// Key current element key
func (bpti *bPlusTreeIterator) Key() []byte {
	return bpti.key
}

// Value current element value
func (bpti *bPlusTreeIterator) Value() *storage.LogRecordPos {
	pos, _ := storage.DecodeLogRecordPosition(bpti.value)
	return pos
}

// Close iterator, free resource
func (bpti *bPlusTreeIterator) Close() {
	// rollback read transaction, and avoid deadlock
	if err := bpti.tx.Rollback(); err != nil {
		panic("failed to rollback bplus tree transaction: " + err.Error())
	}
}
