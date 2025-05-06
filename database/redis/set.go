package redis

import (
	"FinKV/err_def"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type RSet struct {
	dw *DBWrapper
}

var setPool = sync.Pool{
	New: func() interface{} {
		return &RSet{}
	},
}

func NewRSet(dw *DBWrapper) *RSet {
	rs := setPool.Get().(*RSet)
	rs.dw = dw
	return rs
}

func (rs *RSet) Release() {
	setPool.Put(rs)
}

// SAdd 批量添加word
func (rs *RSet) SAdd(key string, words ...string) (int64, error) {
	if len(key) == 0 {
		return 0, err_def.ErrEmptyKey
	}
	if len(words) == 0 {
		return 0, nil
	}

	wb := rs.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	var added int64
	setLenKey := GetSetLenKey(key)

	currLen := int64(0)
	if val, err := rs.dw.GetDB().Get(setLenKey); err == nil {
		if n, err := strconv.ParseInt(val, 10, 64); err == nil {
			currLen = n
		}
	}

	uniqueWords := make(map[string]struct{}, len(words))
	for _, member := range words {
		uniqueWords[member] = struct{}{}
	}

	for word := range uniqueWords {
		memberKey := GetSetMemberKey(key, word)
		exists, err := rs.dw.GetDB().Exists(memberKey)
		if err != nil && !strings.HasPrefix(err.Error(), "no value found") {
			return 0, err
		}
		if !exists {
			if err := wb.Put(memberKey, "1"); err != nil {
				return 0, err
			}
			added++
		}
	}

	if added > 0 {
		if err := wb.Put(setLenKey, strconv.FormatInt(currLen+added, 10)); err != nil {
			return 0, err
		}
	}

	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return added, nil
}

// SMembers 获取集合中所有元素
func (rs *RSet) SMembers(key string) ([]string, error) {
	if len(key) == 0 {
		return nil, err_def.ErrEmptyKey
	}

	prefix := fmt.Sprintf("%s:%s:", SetPrefix, key)
	keys, err := rs.dw.GetDB().Keys(prefix + "*")
	if err != nil {
		return nil, err
	}

	members := make([]string, 0, len(keys))
	for _, k := range keys {
		if strings.HasSuffix(k, "_len_") {
			continue
		}
		member := strings.TrimPrefix(k, prefix)
		members = append(members, member)
	}

	return members, nil
}

// SRem 批量删除word
func (rs *RSet) SRem(key string, words ...string) (int64, error) {
	if len(key) == 0 {
		return 0, err_def.ErrEmptyKey
	}
	if len(words) == 0 {
		return 0, nil
	}

	wb := rs.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	var removed int64
	setLenKey := GetSetLenKey(key)

	currLen := int64(0)
	if val, err := rs.dw.GetDB().Get(setLenKey); err == nil {
		if n, err := strconv.ParseInt(val, 10, 64); err == nil {
			currLen = n
		}
	}

	for _, member := range words {
		memberKey := GetSetMemberKey(key, member)
		exists, err := rs.dw.GetDB().Exists(memberKey)
		if err != nil {
			return 0, err
		}
		if exists {
			if err := wb.Delete(memberKey); err != nil {
				return 0, err
			}
			removed++
		}
	}

	if removed > 0 {
		newLen := currLen - removed
		if newLen > 0 {
			if err := wb.Put(setLenKey, strconv.FormatInt(newLen, 10)); err != nil {
				return 0, err
			}
		} else {
			if err := wb.Delete(setLenKey); err != nil {
				return 0, err
			}
		}
	}

	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return removed, nil
}

// SDiff 获取集合的差集
func (rs *RSet) SDiff(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	members, err := rs.SMembers(keys[0])
	if err != nil {
		return nil, err
	}

	if len(keys) == 1 {
		return members, nil
	}

	result := make(map[string]struct{})
	for _, member := range members {
		result[member] = struct{}{}
	}

	for _, key := range keys[1:] {
		otherMembers, err := rs.SMembers(key)
		if err != nil {
			return nil, err
		}
		for _, member := range otherMembers {
			delete(result, member)
		}
	}

	diff := make([]string, 0, len(result))
	for member := range result {
		diff = append(diff, member)
	}

	return diff, nil
}

// SUnion 获取集合的并集
func (rs *RSet) SUnion(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	result := make(map[string]struct{})

	for _, key := range keys {
		members, err := rs.SMembers(key)
		if err != nil {
			return nil, err
		}
		for _, member := range members {
			result[member] = struct{}{}
		}
	}

	union := make([]string, 0, len(result))
	for member := range result {
		union = append(union, member)
	}

	return union, nil
}
