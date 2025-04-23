package base

import (
	"FinKV/err_def"
	"FinKV/storage"
	"FinKV/storage/bitcask"
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DB 是 FinKV 数据库的核心结构体，负责管理键值对的存储、过期逻辑以及批量操作。
type DB struct {
	bc *bitcask.Bitcask // Bitcask 存储引擎实例

	expireMap map[string]time.Time // 存储键的过期时间
	expireMu  sync.RWMutex         // 保护 expireMap 的读写锁

	closeCh chan struct{}  // 关闭信号通道
	wg      sync.WaitGroup // 等待组，用于确保后台任务完成

	ttlPath string // TTL 元数据文件路径

	needFlush bool // 标记是否需要刷新 TTL 元数据

	dbOpts *BaseDBOptions // 数据库配置选项
}

// NewDB 创建一个新的 DB 实例。
// 参数 dbOpts 是数据库配置选项，bcOpts 是 Bitcask 存储引擎的选项。
// 返回值为 DB 实例和可能的错误。
func NewDB(dbOpts *BaseDBOptions, bcOpts ...storage.Option) (*DB, error) {
	if dbOpts == nil {
		dbOpts = DefaultBaseDBOptions()
	}

	bc, err := bitcask.Open(bcOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to open bitcask: %w", err)
	}

	db := &DB{
		bc:        bc,
		expireMap: make(map[string]time.Time),
		closeCh:   make(chan struct{}),
		dbOpts:    dbOpts,
	}

	dataDirField := bc.GetDataDir()
	db.ttlPath = filepath.Join(dataDirField, db.dbOpts.TTLMetadataFile)

	if err := db.loadTTLMetadata(); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			_ = bc.Close()
			return nil, fmt.Errorf("failed to load TTL metadata: %w", err)
		}
	}

	db.wg.Add(1)
	go db.expirationWorker(dbOpts.ExpireCheckInterval)

	return db, nil
}

// Put 将键值对存储到数据库中。
// 参数 key 是键名，value 是值。
// 返回值为可能的错误。
func (db *DB) Put(key string, value string) error {
	return db.bc.Put(key, []byte(value))
}

// Get 从数据库中获取指定键的值。
// 参数 key 是键名。
// 返回值为键对应的值和可能的错误。
func (db *DB) Get(key string) (string, error) {
	if db.isExpired(key) {
		_ = db.deleteExpiredKey(key)
		return "", err_def.ErrKeyNotFound
	}

	val, err := db.bc.Get(key)
	if err != nil {
		return "", err
	}
	return string(val), nil
}

// Del 删除指定键及其对应的值。
// 参数 key 是键名。
// 返回值为可能的错误。
func (db *DB) Del(key string) error {
	return db.bc.Del(key)
}

// Close 关闭数据库连接并释放资源。
func (db *DB) Close() {
	close(db.closeCh)
	db.wg.Wait()

	_ = db.saveTTLMetadata()

	_ = db.bc.Close()
}

// Exists 检查指定键是否存在。
// 参数 key 是键名。
// 返回值为布尔值表示键是否存在，以及可能的错误。
func (db *DB) Exists(key string) (bool, error) {
	if db.isExpired(key) {
		_ = db.deleteExpiredKey(key)
		return false, nil
	}

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

// Expire 设置指定键的过期时间。
// 参数 key 是键名，ttl 是过期时间。
// 返回值为可能的错误。
func (db *DB) Expire(key string, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("invalid TTL: %v", ttl)
	}
	ex, err := db.Exists(key)
	if err != nil {
		return err
	}
	if !ex {
		return err_def.ErrKeyNotFound
	}
	expireAt := time.Now().Add(ttl)

	db.expireMu.Lock()
	db.expireMap[key] = expireAt
	db.needFlush = db.needFlush || db.dbOpts.FlushTTLOnChange
	db.expireMu.Unlock()

	if db.dbOpts.FlushTTLOnChange {
		_ = db.saveTTLMetadata()
	}

	return nil
}

// Keys 根据模式匹配返回所有符合条件的键。
// 参数 pattern 是匹配模式。
// 返回值为匹配的键列表和可能的错误。
func (db *DB) Keys(pattern string) ([]string, error) {
	allKeys, err := db.bc.ListKeys()
	if err != nil {
		return nil, err
	}

	results := make([]string, 0, len(allKeys))
	now := time.Now()

	if !strings.ContainsAny(pattern, "*?[]") {
		for _, k := range allKeys {
			db.expireMu.RLock()
			expAt, ok := db.expireMap[k]
			db.expireMu.RUnlock()
			if ok && now.After(expAt) {
				continue
			}
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
		db.expireMu.RLock()
		expAt, ok := db.expireMap[k]
		db.expireMu.RUnlock()
		if ok && now.After(expAt) {
			continue
		}
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
// 参数 key 是键名。
// 返回值为数据类型（字符串形式）和可能的错误。
func (db *DB) Type(key string) (string, error) {
	if db.isExpired(key) {
		_ = db.deleteExpiredKey(key)
		return "none", nil
	}
	_, err := db.bc.Get(key)
	if err != nil {
		if errors.Is(err, err_def.ErrKeyNotFound) {
			return "none", nil
		}
		return "none", err
	}
	return "string", nil
}

// Persist 取消指定键的过期时间。
// 参数 key 是键名。
// 返回值为可能的错误。
func (db *DB) Persist(key string) error {
	ex, err := db.Exists(key)
	if err != nil {
		return err
	}
	if !ex {
		return err_def.ErrKeyNotFound
	}
	db.expireMu.Lock()
	delete(db.expireMap, key)
	db.needFlush = db.needFlush || db.dbOpts.FlushTTLOnChange
	db.expireMu.Unlock()

	if db.dbOpts.FlushTTLOnChange {
		_ = db.saveTTLMetadata()
	}
	return nil
}

// NewWriteBatch 创建一个新的写入批处理对象。
// 参数 opts 是批处理选项。
// 返回值为 WriteBatch 实例。
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

// expirationWorker 是后台任务，定期检查并删除过期键。
// 参数 interval 是检查间隔时间。
func (db *DB) expirationWorker(interval time.Duration) {
	defer db.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
			db.evictExpiredKeys()

			db.expireMu.Lock()
			if db.needFlush {
				_ = db.saveTTLMetadata()
				db.needFlush = false
			}
			db.expireMu.Unlock()
		}
	}
}

// evictExpiredKeys 删除所有已过期的键。
func (db *DB) evictExpiredKeys() {
	now := time.Now()
	db.expireMu.Lock()
	defer db.expireMu.Unlock()

	for k, expAt := range db.expireMap {
		if now.After(expAt) {
			_ = db.bc.Del(k)
			delete(db.expireMap, k)
			db.needFlush = true
		}
	}
}

// isExpired 检查指定键是否已过期。
// 参数 key 是键名。
// 返回值为布尔值表示键是否已过期。
func (db *DB) isExpired(key string) bool {
	db.expireMu.RLock()
	expAt, ok := db.expireMap[key]
	db.expireMu.RUnlock()
	return ok && time.Now().After(expAt)
}

// deleteExpiredKey 删除指定的过期键。
// 参数 key 是键名。
// 返回值为可能的错误。
func (db *DB) deleteExpiredKey(key string) error {
	err := db.bc.Del(key)
	db.expireMu.Lock()
	delete(db.expireMap, key)
	db.needFlush = db.needFlush || db.dbOpts.FlushTTLOnChange
	db.expireMu.Unlock()

	if db.dbOpts.FlushTTLOnChange {
		_ = db.saveTTLMetadata()
	}
	return err
}

// loadTTLMetadata 加载 TTL 元数据文件。
// 返回值为可能的错误。
func (db *DB) loadTTLMetadata() error {
	f, err := os.Open(db.ttlPath)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			continue
		}
		k := parts[0]
		expNano, parseErr := strconv.ParseInt(parts[1], 10, 64)
		if parseErr != nil {
			continue
		}
		db.expireMap[k] = time.Unix(0, expNano)
	}
	return scanner.Err()
}

// saveTTLMetadata 保存 TTL 元数据到文件。
// 返回值为可能的错误。
func (db *DB) saveTTLMetadata() error {
	tmpFile := db.ttlPath + ".tmp"
	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := bufio.NewWriter(f)

	db.expireMu.RLock()
	for k, expAt := range db.expireMap {
		line := fmt.Sprintf("%s %d\n", k, expAt.UnixNano())
		if _, werr := writer.WriteString(line); werr != nil {
			db.expireMu.RUnlock()
			return werr
		}
	}
	db.expireMu.RUnlock()

	if err := writer.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmpFile, db.ttlPath)
}
