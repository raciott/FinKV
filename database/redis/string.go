package redis

import (
	"FinKV/err_def"
	"strconv"
	"sync"
)

// RString 实现了Redis字符串类型的操作
type RString struct {
	dw *DBWrapper // 数据库包装器，提供底层存储访问
}

// 字符串操作对象池，用于减少内存分配和GC压力
var stringPool = sync.Pool{
	New: func() interface{} {
		return &RString{
			dw: &DBWrapper{}, // 创建默认的数据库包装器
		}
	},
}

// NewRString 创建一个新的RString实例
// dw参数提供底层存储访问能力
func NewRString(dw *DBWrapper) *RString {
	rs := stringPool.Get().(*RString) // 从对象池获取实例
	rs.dw = dw                        // 设置数据库包装器
	return rs
}

// Release 释放RString实例，将其放回对象池
func (rs *RString) Release() {
	stringPool.Put(rs) // 将实例放回对象池以便复用
}

// Set 设置键值对
// 实现Redis SET命令的功能
func (rs *RString) Set(key, value string) error {
	if len(key) == 0 { // 检查键是否为空
		return err_def.ErrEmptyKey // 返回空键错误
	}
	return rs.dw.GetDB().Put(GetStringKey(key), value) // 存储键值对
}

// Get 获取键对应的值
// 实现Redis GET命令的功能
func (rs *RString) Get(key string) (string, error) {
	if len(key) == 0 { // 检查键是否为空
		return "", err_def.ErrEmptyKey // 返回空键错误
	}
	return rs.dw.GetDB().Get(GetStringKey(key)) // 获取键对应的值
}

// Del 删除一个或多个键
// 实现Redis DEL命令的功能
func (rs *RString) Del(keys ...string) error {
	if len(keys) == 0 { // 检查是否提供了键
		return err_def.ErrEmptyKey // 返回空键错误
	}

	// 创建写批次以进行原子操作
	wb := rs.dw.GetDB().NewWriteBatch(nil)
	// 遍历所有键并删除
	for _, key := range keys {
		if err := wb.Delete(GetStringKey(key)); err != nil {
			return err // 删除失败返回错误
		}
	}

	return wb.Commit() // 提交写批次
}

func (rs *RString) Incr(key string) (int64, error) {
	return rs.IncrBy(key, 1) // 调用IncrBy实现，增量为1
}

// IncrBy 将键对应的值增加指定的整数
// 实现Redis INCRBY命令的功能
func (rs *RString) IncrBy(key string, value int64) (int64, error) {
	if len(key) == 0 { // 检查键是否为空
		return 0, err_def.ErrEmptyKey // 返回空键错误
	}

	strKey := GetStringKey(key) // 获取存储用的键
	// 创建写批次以进行原子操作
	wb := rs.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release() // 确保写批次最终被释放

	// 获取当前值
	val, err := rs.dw.GetDB().Get(strKey)
	if err != nil {
		// Key不存在时设为初始值
		if err := wb.Put(strKey, strconv.FormatInt(value, 10)); err != nil {
			return 0, err // 存储失败返回错误
		}
		if err := wb.Commit(); err != nil {
			return 0, err // 提交失败返回错误
		}
		return value, nil // 返回初始值
	}

	// 尝试将当前值转换为int64
	current, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err_def.ErrValueNotInteger // 值不是整数，返回错误
	}

	// 计算新值并存储
	result := current + value
	if err := wb.Put(strKey, strconv.FormatInt(result, 10)); err != nil {
		return 0, err // 存储失败返回错误
	}
	if err := wb.Commit(); err != nil {
		return 0, err // 提交失败返回错误
	}

	return result, nil
}
