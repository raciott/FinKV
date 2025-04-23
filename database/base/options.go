package base

import "time"

// BaseDBOptions 定义了基础数据库的选项
type BaseDBOptions struct {
	// ExpireCheckInterval 是检查过期键的时间间隔
	ExpireCheckInterval time.Duration
	// TTLMetadataFile 是存储TTL元数据的文件路径
	TTLMetadataFile string
	// FlushTTLOnChange 指示在TTL更改时是否刷新
	FlushTTLOnChange bool
}

// DefaultBaseDBOptions 返回默认的基础数据库选项
func DefaultBaseDBOptions() *BaseDBOptions {
	return &BaseDBOptions{
		ExpireCheckInterval: 1 * time.Minute,
		TTLMetadataFile:     "ttl.data",
		FlushTTLOnChange:    false,
	}
}
