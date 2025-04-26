package redis

import (
	"FinKV/err_def"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type RHash struct {
	dw *DBWrapper
}

var hashPool = sync.Pool{
	New: func() interface{} {
		return &RHash{}
	},
}

func NewRHash(dw *DBWrapper) *RHash {
	rh := hashPool.Get().(*RHash)
	rh.dw = dw
	return rh
}

func (rh *RHash) Release() {
	hashPool.Put(rh)
}

// HSet (设置哈希值)
func (rh *RHash) HSet(key, field, value string) error {

	// 调用批量插入
	return rh.HMSet(key, map[string]string{field: value})
}

// HMSet (批量设置哈希值)
func (rh *RHash) HMSet(key string, fields map[string]string) error {
	return rh.batchSetFields(key, fields, false)
}

// HSetNX (设置哈希值，如果字段已经存在则返回错误)
func (rh *RHash) batchSetFields(key string, fields map[string]string, nx bool) error {
	if len(key) == 0 {
		return err_def.ErrEmptyKey
	}
	if len(fields) == 0 {
		return nil
	}

	wb := rh.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	lenKey := GetHashLenKey(key)
	currentLen, err := rh.HLen(key)
	if err != nil && !errors.Is(err, err_def.ErrKeyNotFound) {
		return err
	}

	newFields := 0
	for field, value := range fields {
		hashKey := GetHashFieldKey(key, field)

		if nx {
			exists, err := rh.dw.GetDB().Exists(hashKey)
			if err != nil {
				return err
			}
			if exists {
				continue
			}
		}

		if err := wb.Put(hashKey, value); err != nil {
			return err
		}

		if !nx {
			exists, err := rh.dw.GetDB().Exists(hashKey)
			if err != nil {
				return err
			}
			if !exists {
				newFields++
			}
		} else {
			newFields++
		}
	}

	if newFields > 0 {
		if err := wb.Put(lenKey, strconv.FormatInt(currentLen+int64(newFields), 10)); err != nil {
			return err
		}
	}

	return wb.Commit()
}

// HLen (获取哈希字段数量)
func (rh *RHash) HLen(key string) (int64, error) {
	if len(key) == 0 {
		return 0, err_def.ErrEmptyKey
	}

	val, err := rh.dw.GetDB().Get(GetHashLenKey(key))
	if err != nil {
		if errors.Is(err, err_def.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}

	return strconv.ParseInt(val, 10, 64)
}

// HGet (获取哈希字段值)
func (rh *RHash) HGet(key, field string) (string, error) {
	if len(key) == 0 || len(field) == 0 {
		return "", err_def.ErrEmptyKey
	}

	return rh.dw.GetDB().Get(GetHashFieldKey(key, field))
}

// HMGet (批量获取哈希字段值)
func (rh *RHash) HMGet(key string, fields ...string) (map[string]string, error) {
	if len(key) == 0 || len(fields) == 0 {
		return nil, err_def.ErrEmptyKey
	}

	result := make(map[string]string, len(fields))
	var errs []string
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, field := range fields {
		if len(field) == 0 {
			errs = append(errs, "empty field name")
			continue
		}

		wg.Add(1)
		go func(f string) {
			defer wg.Done()
			val, err := rh.dw.GetDB().Get(GetHashFieldKey(key, f))
			if err != nil {
				if !errors.Is(err, err_def.ErrKeyNotFound) {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("error getting field %s: %v", f, err))
					mu.Unlock()
				}
			}

			mu.Lock()
			result[f] = val
			mu.Unlock()
		}(field)
	}

	wg.Wait()

	if len(errs) > 0 {
		return result, fmt.Errorf("multiple errors occurred: %s", strings.Join(errs, "; "))
	}

	return result, nil
}

// HDel (删除哈希字段)
func (rh *RHash) HDel(key string, fields ...string) (int64, error) {
	if len(key) == 0 || len(fields) == 0 {
		return 0, err_def.ErrEmptyKey
	}

	wb := rh.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	var deleted int64
	for _, field := range fields {
		hashKey := GetHashFieldKey(key, field)
		exists, err := rh.dw.GetDB().Exists(hashKey)
		if err != nil {
			return 0, err
		}
		if !exists {
			continue
		}

		if err := wb.Delete(hashKey); err != nil {
			return 0, err
		}
		deleted++
	}

	if deleted > 0 {
		currLen, err := rh.HLen(key)
		if err != nil && !errors.Is(err, err_def.ErrKeyNotFound) {
			return 0, err
		}
		if err := wb.Put(GetHashLenKey(key), strconv.FormatInt(currLen-deleted, 10)); err != nil {
			return 0, err
		}

		if currLen == deleted {
			if err := wb.Delete(GetHashLenKey(key)); err != nil {
				return 0, err
			}
		}
	}

	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return deleted, nil
}

// HExists (判断哈希字段是否存在)
func (rh *RHash) HExists(key, field string) (bool, error) {
	if len(key) == 0 || len(field) == 0 {
		return false, err_def.ErrEmptyKey
	}

	ok, err := rh.dw.GetDB().Exists(GetHashFieldKey(key, field))
	if err != nil {
		if !strings.HasPrefix(err.Error(), "no value found") {
			return false, err
		}
		return false, nil
	}

	return ok, nil
}

// HKeys (获取哈希字段列表)
func (rh *RHash) HKeys(key string) ([]string, error) {
	if len(key) == 0 {
		return nil, err_def.ErrEmptyKey
	}

	pattern := fmt.Sprintf("%s:%s:*", HashPrefix, key)
	keys, err := rh.dw.GetDB().Keys(pattern)
	if err != nil {
		return nil, err
	}

	prefix := fmt.Sprintf("%s:%s:", HashPrefix, key)
	lenKey := GetHashLenKey(key)
	result := make([]string, 0, len(keys))

	for _, k := range keys {
		if k == lenKey {
			continue
		}
		field := strings.TrimPrefix(k, prefix)
		result = append(result, field)
	}

	return result, nil
}

// HVals (获取哈希字段值)
func (rh *RHash) HVals(key string) ([]string, error) {
	if len(key) == 0 {
		return nil, err_def.ErrEmptyKey
	}

	fields, err := rh.HKeys(key)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(fields))
	for _, field := range fields {
		val, err := rh.HGet(key, field)
		if err != nil {
			continue
		}
		result = append(result, val)
	}

	return result, nil
}

// HGetAll (获取哈希字段值)
func (rh *RHash) HGetAll(key string) (map[string]string, error) {
	if len(key) == 0 {
		return nil, err_def.ErrEmptyKey
	}

	fields, err := rh.HKeys(key)
	if err != nil {
		return nil, err
	}

	return rh.HMGet(key, fields...)
}

// HIncrBy (哈希字段自增)
func (rh *RHash) HIncrBy(key, field string, incr int64) (int64, error) {
	if len(key) == 0 || len(field) == 0 {
		return 0, err_def.ErrEmptyKey
	}

	hashKey := GetHashFieldKey(key, field)
	wb := rh.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	val, err := rh.dw.GetDB().Get(hashKey)
	if err != nil {
		if !errors.Is(err, err_def.ErrKeyNotFound) {
			return 0, err
		}
		if err := wb.Put(hashKey, strconv.FormatInt(incr, 10)); err != nil {
			return 0, err
		}

		if err := wb.Put(GetHashLenKey(key), "1"); err != nil {
			return 0, err
		}

		if err := wb.Commit(); err != nil {
			return 0, err
		}
		return incr, nil
	}

	current, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err_def.ErrValueNotInteger
	}

	result := current + incr
	if err := wb.Put(hashKey, strconv.FormatInt(result, 10)); err != nil {
		return 0, err
	}

	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return result, nil
}
