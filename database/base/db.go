package base

import (
	"FinKV/err_def"
	"FinKV/storage"
	"FinKV/storage/bitcask"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
)

// DB 是 FinKV 数据库的核心结构体，负责管理键值对的存储、过期逻辑以及批量操作。
type DB struct {
	bc *bitcask.Bitcask // Bitcask 存储引擎实例

	closeCh chan struct{}  // 关闭信号通道
	wg      sync.WaitGroup // 等待组，用于确保后台任务完成

}

// NewDB 创建一个新的 DB 实例。 bcOpts 是 Bitcask 存储引擎的选项。
// 返回值为 DB 实例和可能的错误。
func NewDB(bcOpts ...storage.Option) (*DB, error) {

	bc, err := bitcask.Open(bcOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to open bitcask: %w", err)
	}

	db := &DB{
		bc:      bc,
		closeCh: make(chan struct{}),
	}

	return db, nil
}

// Put 将键值对存储到数据库中。
func (db *DB) Put(key string, value string) error {
	return db.bc.Put(key, []byte(value))
}

// Get 从数据库中获取指定键的值。
func (db *DB) Get(key string) (string, error) {

	val, err := db.bc.Get(key)
	if err != nil {
		return "", err
	}
	return string(val), nil
}

// Del 删除指定键及其对应的值。
func (db *DB) Del(key string) error {
	return db.bc.Del(key)
}

// Close 关闭数据库连接并释放资源。
func (db *DB) Close() {
	close(db.closeCh)
	db.wg.Wait()

	_ = db.bc.Close()
}

// Exists 检查指定键是否存在。
func (db *DB) Exists(key string) (bool, error) {

	filter := db.bc.GetFilter()
	if filter != nil && !filter.Contains([]byte(key)) {
		return false, nil
	}

	if _, err := db.bc.GetMemIndex().Get(key); err != nil {
		if errors.Is(err, err_def.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Keys 根据模式匹配返回所有符合条件的键。
func (db *DB) Keys(pattern string) ([]string, error) {
	allKeys, err := db.bc.ListKeys()
	if err != nil {
		return nil, err
	}

	results := make([]string, 0, len(allKeys))

	if !strings.ContainsAny(pattern, "*?[]") {
		for _, k := range allKeys {
			if k == pattern {
				results = append(results, k)
			}
		}
		return results, nil
	}

	prefix := ""
	if strings.HasSuffix(pattern, "*") {
		prefix = pattern[:len(pattern)-1]
	}

	for _, k := range allKeys {
		if prefix != "" {
			if strings.HasPrefix(k, prefix) {
				results = append(results, k)
			}
			continue
		}
		matched, _ := filepath.Match(pattern, k)
		if matched {
			results = append(results, k)
		}
	}
	return results, nil
}

// Type 返回指定键的数据类型。
func (db *DB) Type(key string) (string, error) {

	_, err := db.bc.Get(key)
	if err != nil {
		if errors.Is(err, err_def.ErrKeyNotFound) {
			return "none", nil
		}
		return "none", err
	}
	return "string", nil
}

// NewWriteBatch 创建一个新的写入批处理对象。
func (db *DB) NewWriteBatch(opts *BatchOptions) *WriteBatch {
	if opts == nil {
		opts = DefaultBatchOptions()
	}

	wb := batchPool.Get().(*WriteBatch)
	wb.db = db
	wb.committed = false
	wb.opts = opts
	wb.operations = wb.operations[:0]

	return wb
}
