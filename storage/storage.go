// Package storage 定义了FincasKV的存储引擎接口和常量
// 实现了基于Bitcask模型的高性能键值存储系统
// 提供了持久化、索引、缓存等核心功能
package storage

// 存储引擎相关常量
var (
	FilePrefix = "data-" // 数据文件前缀，用于标识存储引擎的数据文件
	FileSuffix = ".txt"  // 数据文件后缀，表示这是一个日志格式的数据文件
	// HeaderSize 记录头部大小: timestamp(8) + flags(4) + keyLen(4) + valueLen(4) = 20 bytes
	// 每条记录的头部包含时间戳、标志位、键长度和值长度信息
	HeaderSize = 20
	// MaxKeySize 键最大长度 32MB (限制单个键的最大大小，防止异常数据导致内存溢出)
	MaxKeySize = 32 << 20
	// MaxValueSize 值最大长度 32MB (限制单个值的最大大小，防止异常数据导致内存溢出)
	MaxValueSize = 32 << 20
)

// Storage 定义了通用键值存储接口
// 这是存储引擎的核心抽象，定义了所有存储引擎必须实现的方法
// KeyType 是键的类型，必须是可比较的，用于索引和查找
// ValueType 是值的类型，可以是任意类型，提供了灵活的数据存储能力
type Storage[KeyType comparable, ValueType any] interface {
	Open(opts ...Options) (Storage[KeyType, ValueType], error) // 打开存储引擎，初始化并返回存储实例
	Put(key KeyType, value ValueType) error                    // 存储键值对，如果键已存在则更新值
	Get(key KeyType) (ValueType, error)                        // 获取键对应的值，如果键不存在则返回错误
	Del(key ValueType) error                                   // 删除键值对，从存储中移除指定的键
	ListKeys() ([]KeyType, error)                              // 列出所有键，返回存储中的所有键列表
	Fold(f func(key KeyType, value ValueType) bool) error      // 遍历所有键值对，对每个键值对应用指定的函数
	Merge() error                                              // 合并存储文件，优化存储空间和读取性能
	Sync() error                                               // 同步数据到磁盘，确保数据持久化
	Close() error                                              // 关闭存储引擎，释放资源并确保数据安全
}

// MemIndex 定义了内存索引接口
// 内存索引用于快速查找键值对，提高读取性能
// 支持多种索引实现，如B树、跳表和哈希表等
type MemIndex[KeyType comparable, ValueType any] interface {
	Put(key KeyType, value ValueType) error                  // 添加索引项，将键与对应的值信息关联起来
	Get(key KeyType) (ValueType, error)                      // 获取索引项，根据键快速查找对应的值信息
	Del(key KeyType) error                                   // 删除索引项，移除键对应的索引记录
	Foreach(f func(key KeyType, value ValueType) bool) error // 遍历所有索引项，对每个索引项应用指定的函数
	Clear() error                                            // 清空索引，移除所有索引项
}

// MemCache 定义了内存缓存接口
// 内存缓存用于缓存热点数据，减少磁盘IO，提高性能
// 实现了缓存淘汰策略，在内存有限的情况下保留最有价值的数据
type MemCache[KeyType comparable, ValueType any] interface {
	Insert(key KeyType, value ValueType) error // 插入缓存项，将数据添加到缓存中
	Find(key KeyType) (ValueType, error)       // 查找缓存项，从缓存中快速获取数据
	Delete(key KeyType) error                  // 删除缓存项，从缓存中移除指定的数据
	Exist(key KeyType) bool                    // 检查缓存项是否存在，判断数据是否在缓存中
	Items() map[interface{}]any
}
