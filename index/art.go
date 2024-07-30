package index

import (
	"bitcask-go/storage"
	"bytes"
	art "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

type AdaptiveRadixTree struct {
	tree art.Tree
	mu   *sync.RWMutex
}

// NewAdaptiveRadixTree Initialize AdaptiveRadixTree
func NewAdaptiveRadixTree() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: art.New(),
		mu:   new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Get(key []byte) *storage.LogRecordPos {
	if key == nil {
		return nil
	}

	art.mu.RLock()
	pos, found := art.tree.Search(key)
	art.mu.RUnlock()

	if !found {
		return nil
	}

	return pos.(*storage.LogRecordPos)
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *storage.LogRecordPos) bool {
	if key == nil {
		return false
	}

	art.mu.Lock()
	art.tree.Insert(key, pos)
	art.mu.Unlock()

	return true
}

func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	if key == nil {
		return false
	}

	art.mu.Lock()
	_, deleted := art.tree.Delete(key)
	art.mu.Unlock()

	return deleted
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	if art.tree == nil {
		return nil
	}

	art.mu.RLock()
	defer art.mu.RUnlock()
	return newArtIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Size() int {
	art.mu.RLock()
	defer art.mu.RUnlock()
	return art.tree.Size()
}

type ArtIterator struct {
	currentIndex int
	reverse      bool
	values       []*Item
}

func newArtIterator(tree art.Tree, reverse bool) *ArtIterator {
	values := make([]*Item, tree.Size())

	var index int
	if reverse {
		index = tree.Size() - 1
	}

	getValue := func(node art.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*storage.LogRecordPos),
		}
		values[index] = item

		if reverse {
			index--
		} else {
			index++
		}

		return true
	}

	tree.ForEach(getValue)

	return &ArtIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}
}

// Rewind set iterator to first element
func (ai *ArtIterator) Rewind() {
	ai.currentIndex = 0
}

// Seek the first element less/greater than key
func (ai *ArtIterator) Seek(key []byte) {
	if ai.reverse {
		ai.currentIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		ai.currentIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}
}

// Next go to next element
func (ai *ArtIterator) Next() {
	ai.currentIndex++
}

// Valid check if has next element
func (ai *ArtIterator) Valid() bool {
	return ai.currentIndex < len(ai.values)
}

// Key current element key
func (ai *ArtIterator) Key() []byte {
	return ai.values[ai.currentIndex].key
}

// Value current element value
func (ai *ArtIterator) Value() *storage.LogRecordPos {
	return ai.values[ai.currentIndex].pos
}

// Close iterator, free resource
func (ai *ArtIterator) Close() {
	ai.values = nil
}
