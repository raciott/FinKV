package index

import (
	storage2 "FinKV/storage"
	"fmt"
	"hash/fnv"
	"log"
	"sync"
)

// MemIndexShard 是一个分片内存索引结构，支持多种类型的内存索引（如 BTree、SkipList 和 SwissTable）
type MemIndexShard[K comparable, V any] struct {
	shardCount   int                       // 分片数量
	shards       []storage2.MemIndex[K, V] // 存储每个分片的内存索引
	sync.RWMutex                           // 读写锁，用于保证线程安全
}

// NewMemIndexShard 创建一个新的 MemIndexShard 实例
// 参数：
// - memIndexType: 内存索引类型（BTree、SkipList 或 SwissTable）
// - shardCount: 分片数量
// - btreeDegree: BTree 的度数（仅在 memIndexType 为 BTree 时使用）
// - btreeLessFunc: BTree 的比较函数（仅在 memIndexType 为 BTree 时使用）
// - swissTableSize: SwissTable 的初始大小（仅在 memIndexType 为 SwissTable 时使用）
// 返回值：初始化后的 MemIndexShard 实例
func NewMemIndexShard[K comparable, V any](
	memIndexType storage2.MemIndexType,
	shardCount int,
	btreeDegree int,
	btreeLessFunc func(a, b K) bool,
	swissTableSize uint32,
) *MemIndexShard[K, V] {
	index := &MemIndexShard[K, V]{
		shardCount: shardCount,
		shards:     make([]storage2.MemIndex[K, V], shardCount),
	}

	for i := 0; i < shardCount; i++ {
		switch memIndexType {
		case storage2.BTree:
			if btreeDegree <= 0 {
				log.Fatal("BTree degree must be greater than 0")
			}
			if btreeLessFunc == nil {
				log.Fatal("BTree less func cannot be nil")
			}
			// index.shards[i] = NewBTreeIndex[K, V](btreeDegree, btreeLessFunc)
		case storage2.SwissTable:
			if swissTableSize <= 0 {
				swissTableSize = 1 << 10 // 默认大小为 1024
			}
			index.shards[i] = NewSwissIndex[K, V](swissTableSize)
		default:
			log.Fatal("Unsupported memIndex type")
		}
	}

	return index
}

// getShard 根据键计算哈希值并返回对应的分片
// 参数 key: 键
// 返回值：对应的分片内存索引
func (s *MemIndexShard[K, V]) getShard(key K) storage2.MemIndex[K, V] {
	h := fnv.New32a()
	_, err := h.Write([]byte(fmt.Sprintf("%v", key)))
	if err != nil {
		return nil
	}
	return s.shards[h.Sum32()%uint32(s.shardCount)]
}

// Put 向索引中插入一个键值对
// 参数 key: 键，value: 值
// 返回值：操作是否成功
func (s *MemIndexShard[K, V]) Put(key K, value V) error {
	s.Lock()
	defer s.Unlock()
	shard := s.getShard(key)
	return shard.Put(key, value)
}

// Get 根据键获取对应的值
// 参数 key: 键
// 返回值：查找到的值和操作是否成功
func (s *MemIndexShard[K, V]) Get(key K) (V, error) {
	s.RLock()
	defer s.RUnlock()
	shard := s.getShard(key)
	return shard.Get(key)
}

// Del 删除指定键的键值对
// 参数 key: 键
// 返回值：操作是否成功
func (s *MemIndexShard[K, V]) Del(key K) error {
	s.Lock()
	defer s.Unlock()
	shard := s.getShard(key)
	return shard.Del(key)
}

// Foreach 遍历所有分片中的键值对，并对每个键值对执行回调函数
// 参数 f: 回调函数，接收键值对并返回布尔值，若返回 false 则停止遍历
// 返回值：操作是否成功
func (s *MemIndexShard[K, V]) Foreach(f func(key K, value V) bool) error {
	s.RLock()
	defer s.RUnlock()
	for _, shard := range s.shards {
		stop := false
		err := shard.Foreach(func(key K, value V) bool {
			if stop {
				return false
			}
			if !f(key, value) {
				stop = true
				return false
			}
			return true
		})
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}
	return nil
}

// Clear 清空所有分片中的键值对
// 返回值：操作是否成功
func (s *MemIndexShard[K, V]) Clear() error {
	s.Lock()
	defer s.Unlock()

	var wg sync.WaitGroup
	errChan := make(chan error, len(s.shards))
	defer close(errChan)

	for _, shard := range s.shards {
		wg.Add(1)
		go func(s storage2.MemIndex[K, V]) {
			defer wg.Done()
			if err := s.Clear(); err != nil {
				errChan <- err
			}
		}(shard)
	}
	wg.Wait()

	select {
	case err := <-errChan:
		return fmt.Errorf("could not clear index: %w", err)
	default:
		return nil
	}
}
