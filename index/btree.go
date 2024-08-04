package index

import (
	"bitcask-go/storage"
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"
)

const (
	DefaultDegree = 32
)

// BTree Wrapper for Google btree
type BTree struct {
	tree *btree.BTree
	mu   *sync.RWMutex
}

// NewBTree Initialize BTree
func NewBTree(degree int) *BTree {
	return &BTree{
		tree: btree.New(degree),
		mu:   new(sync.RWMutex),
	}
}

func (bt *BTree) Get(key []byte) *storage.LogRecordPos {
	if key == nil {
		return nil
	}

	item := &Item{key: key}

	bt.mu.RLock()
	btreeItem := bt.tree.Get(item)
	bt.mu.RUnlock()
	if btreeItem == nil {
		return nil
	}

	return btreeItem.(*Item).pos
}

func (bt *BTree) Put(key []byte, pos *storage.LogRecordPos) *storage.LogRecordPos {
	if key == nil {
		return nil
	}

	item := &Item{key: key, pos: pos}

	bt.mu.Lock()
	oldItem := bt.tree.ReplaceOrInsert(item)
	bt.mu.Unlock()

	if oldItem == nil {
		return nil
	}

	return oldItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) (*storage.LogRecordPos, bool) {
	if key == nil {
		return nil, false
	}

	item := &Item{key: key}

	bt.mu.Lock()
	oldItem := bt.tree.Delete(item)
	bt.mu.Unlock()

	if oldItem == nil {
		return nil, false
	}

	return oldItem.(*Item).pos, true
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}

	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

func (bt *BTree) Size() int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.tree.Len()
}

func (bt *BTree) Close() error {
	return nil
}

// Less To implement Less interface in Btree lib
func (item *Item) Less(than btree.Item) bool {
	return bytes.Compare(item.key, than.(*Item).key) == -1
}

type BTreeIterator struct {
	currentIndex int
	reverse      bool
	values       []*Item
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *BTreeIterator {
	values := make([]*Item, tree.Len())
	var index int

	getValue := func(item btree.Item) bool {
		values[index] = item.(*Item)
		index++
		return true
	}

	if reverse {
		tree.Descend(getValue)
	} else {
		tree.Ascend(getValue)
	}

	return &BTreeIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}
}

// Rewind set iterator to first element
func (bi *BTreeIterator) Rewind() {
	bi.currentIndex = 0
}

// Seek the first element less/greater than key
func (bi *BTreeIterator) Seek(key []byte) {
	if bi.reverse {
		bi.currentIndex = sort.Search(len(bi.values), func(i int) bool {
			return bytes.Compare(bi.values[i].key, key) <= 0
		})
	} else {
		bi.currentIndex = sort.Search(len(bi.values), func(i int) bool {
			return bytes.Compare(bi.values[i].key, key) >= 0
		})
	}
}

// Next go to next element
func (bi *BTreeIterator) Next() {
	bi.currentIndex++
}

// Valid check if has next element
func (bi *BTreeIterator) Valid() bool {
	return bi.currentIndex < len(bi.values)
}

// Key current element key
func (bi *BTreeIterator) Key() []byte {
	return bi.values[bi.currentIndex].key
}

// Value current element value
func (bi *BTreeIterator) Value() *storage.LogRecordPos {
	return bi.values[bi.currentIndex].pos
}

// Close iterator, free resource
func (bi *BTreeIterator) Close() {
	bi.values = nil
}
