// Package storage 实现了FincasKV的存储引擎，提供键值对的持久化存储和高效访问
package storage

import (
	"FinKV/util"
	"log"
	"math/rand"
	"time"
)

// BTreeLessFunc 定义了B树索引的比较函数类型 (用于比较两个键的大小关系，返回a是否小于b)
type BTreeLessFunc[K comparable] func(a, b K) bool

// SkipListLessFunc 定义了跳表索引的比较函数类型
// 用于比较两个键的大小关系，返回负数表示a<b，0表示a=b，正数表示a>b
type SkipListLessFunc[K comparable] func(a, b K) int

// MemIndexType 定义了内存索引的类型 (用于选择不同的内存索引实现)
type MemIndexType string

// 支持的内存索引类型常量
const (
	BTree      MemIndexType = "btree"      // B树索引，平衡树结构，适合范围查询
	SkipList   MemIndexType = "skiplist"   // 跳表索引，概率性数据结构，插入删除效率高
	SwissTable MemIndexType = "swisstable" // 瑞士表索引，基于哈希的高性能索引
)

// MemCacheType 定义了内存缓存的类型 (用于选择不同的缓存淘汰策略)
type MemCacheType string

// LRU 支持的内存缓存类型常量
const (
	LRU MemCacheType = "lru" // 最近最少使用缓存策略，淘汰最久未使用的数据
)

// Options 存储引擎的配置选项
// 包含了存储引擎的所有可配置参数，用于自定义存储引擎的行为
type Options struct {
	// 基本配置
	DataDir string // 数据目录路径，存储引擎的所有数据文件将存放在此目录下

	// 内存索引相关配置
	MemIndexDS         MemIndexType             // 内存索引数据结构类型，可选BTree、SkipList或SwissTable
	MemIndexShardCount int                      // 内存索引分片数量，用于提高并发性能，减少锁竞争
	BTreeDegree        int                      // B树的度，影响B树的分支因子和平衡性能
	BTreeComparator    BTreeLessFunc[string]    // B树的比较器，用于比较键的大小关系
	SkipListRandSource rand.Source              // 跳表的随机源，用于生成随机层数
	SkipListComparator SkipListLessFunc[string] // 跳表的比较器，用于比较键的大小关系
	SwissTableSize     uint32                   // SwissTable的初始大小，影响哈希表的性能和内存使用

	// 内存缓存相关配置
	OpenMemCache bool         // 是否开启内存缓存，开启后可提高热点数据访问速度
	MemCacheDS   MemCacheType // 内存缓存数据结构类型，目前支持LRU
	MemCacheSize int          // 内存缓存大小，限制缓存可使用的最大内存

	// 文件管理器相关配置
	MaxFileSize  int64         // 每个数据文件的最大大小，超过此大小将创建新文件
	MaxOpenFiles int           // 最大同时打开的文件数，控制文件句柄资源使用
	SyncInterval time.Duration // 数据同步到磁盘的时间间隔，影响数据持久化频率

	// 合并操作相关配置
	AutoMerge     bool          // 是否自动执行合并操作，合并可回收已删除数据的空间
	MergeInterval time.Duration // 自动合并的时间间隔，控制合并操作的频率
	MinMergeRatio float64       // 最小合并比例，只有当可回收空间超过此比例时才执行合并
}

// Option 定义了配置选项的函数类型
// 用于以函数选项模式设置存储引擎的配置参数
type Option func(opt *Options)

// DefaultOptions 返回存储引擎的默认配置选项
// 提供了一组合理的默认值，适用于大多数场景
func DefaultOptions() *Options {
	// 创建安全的随机源，用于跳表的随机层数生成
	source, err := util.NewSecureRandSource()
	if err != nil {
		log.Panic("Get secure rand source failed: ", err)
	}

	// 返回默认配置
	return &Options{
		// 基本配置
		DataDir: "/tmp/fincas", // 默认数据目录

		// 内存索引配置
		MemIndexDS:         SwissTable, // 默认使用SwissTable作为索引结构
		MemIndexShardCount: 1 << 8,     // 默认256个分片
		BTreeDegree:        8,          // B树默认度为8

		// B树比较器，使用字符串自然顺序
		BTreeComparator: func(a, b string) bool {
			return a < b
		},

		// 跳表配置
		SkipListRandSource: rand.New(source), // 使用安全随机源

		// 跳表比较器，实现字符串比较
		SkipListComparator: func(a, b string) int {
			if a < b {
				return -1
			} else if a > b {
				return 1
			} else {
				return 0
			}
		},

		// 其他配置参数
		SwissTableSize: 1 << 10,         // SwissTable默认大小1024
		OpenMemCache:   true,            // 默认开启内存缓存
		MemCacheDS:     LRU,             // 默认使用LRU缓存策略
		MemCacheSize:   1 << 10,         // 缓存默认大小1024项
		MaxFileSize:    1 << 30,         // 单个文件最大1GB
		MaxOpenFiles:   10,              // 最多同时打开10个文件
		SyncInterval:   5 * time.Second, // 每5秒同步一次数据到磁盘

		// 合并配置
		AutoMerge:     true,      // 默认开启自动合并
		MergeInterval: time.Hour, // 每小时执行一次合并
		MinMergeRatio: 0.3,       // 当可回收空间超过30%时执行合并
	}
}

// WithDataDir 设置数据目录路径的选项函数
// 参数dataDir指定存储引擎数据文件的存储位置
func WithDataDir(dataDir string) Option {
	return func(opt *Options) {
		opt.DataDir = dataDir
	}
}

func WithMemIndexDS(memIndexDS MemIndexType) Option {
	return func(opt *Options) {
		opt.MemIndexDS = memIndexDS
	}
}

func WithMemIndexShardCount(memIndexShardCount int) Option {
	return func(opt *Options) {
		opt.MemIndexShardCount = memIndexShardCount
	}
}

func WithBTreeDegree(bTreeDegree int) Option {
	return func(opt *Options) {
		opt.BTreeDegree = bTreeDegree
	}
}

func WithBTreeComparator(bTreeComparator BTreeLessFunc[string]) Option {
	return func(opt *Options) {
		opt.BTreeComparator = bTreeComparator
	}
}

func WithSkipListRandSource(skipListRandSource rand.Source) Option {
	return func(opt *Options) {
		opt.SkipListRandSource = skipListRandSource
	}
}

func WithSkipListComparator(skipListComparator SkipListLessFunc[string]) Option {
	return func(opt *Options) {
		opt.SkipListComparator = skipListComparator
	}
}

func WithOpenMemCache(openMemCache bool) Option {
	return func(opt *Options) {
		opt.OpenMemCache = openMemCache
	}
}

func WithMemCacheDS(memCacheDS MemCacheType) Option {
	return func(opt *Options) {
		opt.MemCacheDS = memCacheDS
	}
}

func WithMemCacheSize(memCacheSize int) Option {
	return func(opt *Options) {
		opt.MemCacheSize = memCacheSize
	}
}

func WithMaxFileSize(maxFileSize int64) Option {
	return func(opt *Options) {
		opt.MaxFileSize = maxFileSize
	}
}

func WithMaxOpenFiles(maxOpenFiles int) Option {
	return func(opt *Options) {
		opt.MaxOpenFiles = maxOpenFiles
	}
}

func WithSyncInterval(interval time.Duration) Option {
	return func(opt *Options) {
		opt.SyncInterval = interval
	}
}

func WithAutoMerge(autoMerge bool) Option {
	return func(opt *Options) {
		opt.AutoMerge = autoMerge
	}
}

func WithMergeInterval(interval time.Duration) Option {
	return func(opt *Options) {
		opt.MergeInterval = interval
	}
}

func WithMinMergeRatio(minMergeRatio float64) Option {
	return func(opt *Options) {
		opt.MinMergeRatio = minMergeRatio
	}
}
