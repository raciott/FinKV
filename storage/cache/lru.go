package cache

import (
	"container/list"
	"fmt"
)

// LRUCache 定义 LRUCache 结构体
type LRUCache[K comparable, V any] struct {
	// capacity 是缓存的最大容量
	capacity int
	// cache 是用于快速查找的哈希表
	cache map[K]*list.Element
	// list 是用于维护最近使用顺序的双向链表
	list *list.List
}

// entry 定义双向链表节点存储的数据
type entry[K comparable, V any] struct {
	key   K
	value V
}

// NewLRUCache 初始化一个新的 LRUCache
func NewLRUCache[K comparable, V any](capacity int) *LRUCache[K, V] {
	return &LRUCache[K, V]{
		capacity: capacity,
		cache:    make(map[K]*list.Element),
		list:     list.New(),
	}
}

// Insert 插入或更新键值对
func (c *LRUCache[K, V]) Insert(key K, value V) error {
	if elem, exist := c.cache[key]; exist {
		// 更新已存在的键值对
		c.list.MoveToFront(elem)
		elem.Value.(*entry[K, V]).value = value
	} else {
		// 插入新键值对
		newEntry := &entry[K, V]{key: key, value: value}
		newElem := c.list.PushFront(newEntry)
		c.cache[key] = newElem

		// 如果超出容量，移除最久未使用的元素
		if c.list.Len() > c.capacity {
			c.removeOldest()
		}
	}
	return nil
}

// Find 查找键对应的值
func (c *LRUCache[K, V]) Find(key K) (V, error) {
	var zero V
	if elem, exist := c.cache[key]; exist {
		// 将访问的元素移到链表头部
		c.list.MoveToFront(elem)
		return elem.Value.(*entry[K, V]).value, nil
	}
	return zero, fmt.Errorf("cannot find value [%v] in LRU cache", key)
}

// Get 查找键对应的值，并返回布尔值表示是否存在
func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	var zero V
	if elem, exist := c.cache[key]; exist {
		// 将访问的元素移到链表头部
		c.list.MoveToFront(elem)
		return elem.Value.(*entry[K, V]).value, true
	}
	return zero, false
}

// Delete 删除指定键
func (c *LRUCache[K, V]) Delete(key K) error {
	if elem, exist := c.cache[key]; exist {
		c.list.Remove(elem)
		delete(c.cache, key)
		return nil
	}
	return fmt.Errorf("cannot find value [%v] in LRU cache", key)
}

// Exist 判断键是否存在
func (c *LRUCache[K, V]) Exist(key K) bool {
	_, exist := c.cache[key]
	return exist
}

// Values 返回缓存中所有键值对的值
func (c *LRUCache[K, V]) Values() []V {
	values := make([]V, 0, c.list.Len())
	for elem := c.list.Front(); elem != nil; elem = elem.Next() {
		values = append(values, elem.Value.(*entry[K, V]).value)
	}
	return values
}

func (c *LRUCache[K, V]) Items() map[interface{}]any {
	items := make(map[interface{}]any)
	for elem := c.list.Front(); elem != nil; elem = elem.Next() {
		entry := elem.Value.(*entry[K, V])
		items[entry.key] = entry.value
	}
	return items
}

// Purge 清空整个缓存
func (c *LRUCache[K, V]) Purge() {
	// 清空双向链表
	c.list.Init()
	// 清空哈希表
	c.cache = make(map[K]*list.Element)
}

// removeOldest 移除最久未使用的元素
func (c *LRUCache[K, V]) removeOldest() {
	if oldest := c.list.Back(); oldest != nil {
		c.list.Remove(oldest)
		delete(c.cache, oldest.Value.(*entry[K, V]).key)
	}
}
