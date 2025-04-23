package index

import (
	"fmt"
	"github.com/dolthub/swiss"
	"sync"
)

// SwissIndex 是一个基于瑞士表的线程安全索引结构
type SwissIndex[K comparable, V any] struct {
	swissTable *swiss.Map[K, V] // 底层使用的瑞士表实现
	mu         sync.RWMutex     // 读写锁，用于保证线程安全
}

// NewSwissIndex 创建一个新的 SwissIndex 实例
func NewSwissIndex[K comparable, V any](size uint32) *SwissIndex[K, V] {
	return &SwissIndex[K, V]{
		swissTable: swiss.NewMap[K, V](size), // 初始化瑞士表，指定初始大小
	}
}

// Put 向索引中插入一个键值对
func (s *SwissIndex[K, V]) Put(key K, value V) error {
	s.mu.Lock()                  // 加写锁，确保线程安全
	defer s.mu.Unlock()          // 方法结束时释放锁
	s.swissTable.Put(key, value) // 插入键值对到瑞士表
	return nil
}

// Get 根据键获取对应的值
func (s *SwissIndex[K, V]) Get(key K) (V, error) {
	s.mu.RLock()                       // 加读锁，确保线程安全
	defer s.mu.RUnlock()               // 方法结束时释放锁
	value, ok := s.swissTable.Get(key) // 从瑞士表中获取值
	if !ok {
		var zero V
		return zero, fmt.Errorf("no value found for key %v", key) // 如果键不存在，返回错误
	}
	return value, nil
}

// Del 删除指定键的键值对
func (s *SwissIndex[K, V]) Del(key K) error {
	s.mu.Lock()                    // 加写锁，确保线程安全
	defer s.mu.Unlock()            // 方法结束时释放锁
	ok := s.swissTable.Delete(key) // 从瑞士表中删除键值对
	if !ok {
		return fmt.Errorf("delete failed") // 如果删除失败，返回错误
	}
	return nil
}

// Foreach 遍历索引中的所有键值对，并对每个键值对执行回调函数
func (s *SwissIndex[K, V]) Foreach(f func(key K, value V) bool) error {
	s.mu.RLock()         // 加读锁，确保线程安全
	defer s.mu.RUnlock() // 方法结束时释放锁
	s.swissTable.Iter(func(key K, value V) bool {
		if stop := f(key, value); stop { // 执行回调函数，如果返回 true 则停止遍历
			return false
		} else {
			return true
		}
	})
	return nil
}

// Clear 清空索引中的所有键值对
func (s *SwissIndex[K, V]) Clear() error {
	s.mu.Lock()          // 加写锁，确保线程安全
	defer s.mu.Unlock()  // 方法结束时释放锁
	s.swissTable.Clear() // 清空瑞士表中的所有数据
	return nil
}
