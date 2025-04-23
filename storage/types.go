package storage

import (
	"os"
	"sync"
	"sync/atomic"
)

// KVItem 表示键值对数据结构
// 用于存储键值对的原始字节数据
type KVItem struct {
	Key   []byte // 键的字节表示
	Value []byte // 值的字节表示
}

// Record 表示存储引擎中的一条记录
// 包含时间戳、校验和、标志位和键值对数据
type Record struct {
	Timestamp int64  // 记录创建的时间戳，用于版本控制和过期判断
	Checksum  uint64 // 数据校验和，用于验证数据完整性
	Flags     uint32 // 标志位，用于标记记录状态（如正常、已删除等）
	KVItem           // 嵌入的键值对数据
}

// Entry 表示内存索引中的条目
// 记录了数据在磁盘上的位置信息
type Entry struct {
	FileID    int    // 数据文件的ID
	Offset    int64  // 记录在文件中的偏移量
	Size      uint32 // 记录的大小（字节数）
	Timestamp int64  // 记录的时间戳，用于版本控制
}

// DataFile 表示存储引擎的数据文件
// 管理单个数据文件的读写操作
type DataFile struct {
	ID     int          // 文件的唯一标识符
	Path   string       // 文件的完整路径
	File   *os.File     // 文件句柄
	Offset atomic.Int64 // 当前写入位置的偏移量，使用原子操作保证并发安全
	Closed atomic.Bool  // 文件是否已关闭的标志，使用原子操作保证并发安全
	mu     sync.Mutex   // 互斥锁，用于保护文件写入操作的并发安全
}
