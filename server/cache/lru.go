// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"container/list"
	"sync"
)

// Item is the cache entry.
type Item struct {
	Key   uint64
	Value interface{}
}

// LRU is 'Least-Recently-Used' cache.
type LRU struct {
	sync.RWMutex

	// maxCount is the maximum number of items.
	// 0 means no limit.
	maxCount int

	ll    *list.List
	cache map[uint64]*list.Element
}

// NewLRU returns a new lru cache.
func NewLRU(maxCount int) *LRU {
	return &LRU{
		maxCount: maxCount,
		ll:       list.New(),
		cache:    make(map[uint64]*list.Element),
	}
}

// Put puts an item into cache.
func (c *LRU) Put(key uint64, value interface{}) {
	c.Lock()
	defer c.Unlock()
	c.put(key, value)
}

func (c *LRU) put(key uint64, value interface{}) {
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		ele.Value.(*Item).Value = value
		return
	}

	kv := &Item{Key: key, Value: value}
	ele := c.ll.PushFront(kv)
	c.cache[key] = ele
	if c.maxCount != 0 && c.ll.Len() > c.maxCount {
		c.removeOldest()
	}
}

// Get retrives an item from cache.
func (c *LRU) Get(key uint64) (interface{}, bool) {
	c.Lock()
	defer c.Unlock()
	return c.get(key)
}

func (c *LRU) get(key uint64) (interface{}, bool) {
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		return ele.Value.(*Item).Value, true
	}

	return nil, false
}

// Peek reads an item from cache. The action is no considerd 'Use'.
func (c *LRU) Peek(key uint64) (interface{}, bool) {
	c.RLock()
	defer c.RUnlock()
	return c.peek(key)
}

func (c *LRU) peek(key uint64) (interface{}, bool) {
	if ele, ok := c.cache[key]; ok {
		return ele.Value.(*Item).Value, true
	}
	return nil, false
}

// Remove eliminates an item from cache.
func (c *LRU) Remove(key uint64) {
	c.Lock()
	defer c.Unlock()
	c.remove(key)
}

func (c *LRU) remove(key uint64) {
	if ele, ok := c.cache[key]; ok {
		c.removeElement(ele)
	}
}

func (c *LRU) checkAndRemove(key uint64) bool {
	if ele, ok := c.cache[key]; ok {
		c.removeElement(ele)
		return true
	}
	return false
}

func (c *LRU) removeOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

func (c *LRU) getAndRemoveOldest() (uint64, interface{}, bool) {
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
		return ele.Value.(*Item).Key, ele.Value.(*Item).Value, true
	}
	return 0, nil, false
}

func (c *LRU) removeElement(ele *list.Element) {
	c.ll.Remove(ele)
	kv := ele.Value.(*Item)
	delete(c.cache, kv.Key)
}

func (c *LRU) contains(key uint64) bool {
	c.RLock()
	defer c.RUnlock()
	_, ok := c.cache[key]
	return ok
}

// Elems return all items in cache.
func (c *LRU) Elems() []*Item {
	c.RLock()
	defer c.RUnlock()
	return c.elems()
}

func (c *LRU) elems() []*Item {
	elems := make([]*Item, 0, c.ll.Len())
	for ele := c.ll.Front(); ele != nil; ele = ele.Next() {
		clone := *(ele.Value.(*Item))
		elems = append(elems, &clone)
	}

	return elems
}

// Len returns current cache size.
func (c *LRU) Len() int {
	c.RLock()
	defer c.RUnlock()
	return c.size()
}

func (c *LRU) size() int {
	return c.ll.Len()
}
