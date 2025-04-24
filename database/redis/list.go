package redis

import (
	"FinKV/database/base"
	"FinKV/err_def"
	"errors"
	"strconv"
	"sync"
)

type RList struct {
	dw *DBWrapper   // 数据库包装器，提供底层存储访问
	mu sync.RWMutex // 用于阻塞操作
}

// list操作对象池，用于减少内存分配和GC压力
var listPool = sync.Pool{
	New: func() interface{} {
		return &RList{}
	},
}

// NewRList 创建一个RList实例
func NewRList(dw *DBWrapper) *RList {
	rl := listPool.Get().(*RList)
	rl.dw = dw
	return rl
}

// Release 释放RList实例
func (rl *RList) Release() {
	listPool.Put(rl)
}

// 获取列表长度
func (rl *RList) getListLen(key string) (int64, error) {
	lenStr, err := rl.dw.GetDB().Get(GetListLenKey(key))
	if err != nil {
		if errors.Is(err, err_def.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return strconv.ParseInt(lenStr, 10, 64)
}

func (rl *RList) setListLen(wb *base.WriteBatch, key string, length int64) error {
	return wb.Put(GetListLenKey(key), strconv.FormatInt(length, 10))
}

func (rl *RList) getListPointers(key string) (head, tail int64, err error) {
	headStr, err := rl.dw.GetDB().Get(GetListHeadKey(key))
	if err != nil && !errors.Is(err, err_def.ErrKeyNotFound) {
		return 0, 0, err
	}
	if errors.Is(err, err_def.ErrKeyNotFound) {
		headStr = "0" // 如果头部指针不存在，默认为0
	}

	tailStr, err := rl.dw.GetDB().Get(GetListTailKey(key))
	if err != nil && !errors.Is(err, err_def.ErrKeyNotFound) {
		return 0, 0, err
	}
	if errors.Is(err, err_def.ErrKeyNotFound) {
		tailStr = "0" // 如果尾部指针不存在，默认为0
	}

	// 将字符串转换为整数
	head, _ = strconv.ParseInt(headStr, 10, 64)
	tail, _ = strconv.ParseInt(tailStr, 10, 64)
	return head, tail, nil
}

// LPush 添加元素到列表的左侧
func (rl *RList) LPush(key string, values ...string) (int64, error) {
	// 没有值需要添加时直接返回
	if len(values) == 0 {
		return 0, nil
	}

	// 创建写入批处理
	wb := rl.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	// 获取当前列表长度
	length, err := rl.getListLen(key)
	if err != nil {
		return 0, err
	}

	// 获取列表头尾指针
	head, tail, err := rl.getListPointers(key)
	if err != nil {
		return 0, err
	}

	// 从左侧添加元素，头指针向左移动
	for _, value := range values {
		head--
		if err := wb.Put(GetListItemKey(key, head), value); err != nil {
			return 0, err
		}
		length++
	}

	// 更新头指针
	if err := wb.Put(GetListHeadKey(key), strconv.FormatInt(head, 10)); err != nil {
		return 0, err
	}

	// 如果是空列表，需要同时更新尾指针
	if tail == 0 {
		if err := wb.Put(GetListTailKey(key), strconv.FormatInt(head+int64(len(values))-1, 10)); err != nil {
			return 0, err
		}
	}

	// 更新列表长度
	if err := rl.setListLen(wb, key, length); err != nil {
		return 0, err
	}

	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return length, nil
}

// RPush 添加元素到列表的右侧
func (rl *RList) RPush(key string, values ...string) (int64, error) {
	if len(values) == 0 {
		return 0, nil
	}

	wb := rl.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	length, err := rl.getListLen(key)
	if err != nil {
		return 0, err
	}

	// 获取列表头尾指针
	head, tail, err := rl.getListPointers(key)
	if err != nil {
		return 0, err
	}

	// 如果是空列表，初始化头尾指针
	if length == 0 {
		head = 0
		tail = -1 // 设为-1是因为后面会先增加再使用
	}

	// 从右侧添加元素，尾指针向右移动
	for _, value := range values {
		tail++ // 尾指针加一，为新元素腾出位置
		if err := wb.Put(GetListItemKey(key, tail), value); err != nil {
			return 0, err
		}
		length++
	}

	// 更新尾指针
	if err := wb.Put(GetListTailKey(key), strconv.FormatInt(tail, 10)); err != nil {
		return 0, err
	}

	// 如果是新创建的列表，需要设置头指针
	if head == 0 && length == int64(len(values)) {
		if err := wb.Put(GetListHeadKey(key), "0"); err != nil {
			return 0, err
		}
	}

	if err := rl.setListLen(wb, key, length); err != nil {
		return 0, err
	}

	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return length, nil
}

// LPop 从列表左侧（头部）弹出一个元素
func (rl *RList) LPop(key string) (string, error) {
	wb := rl.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	length, err := rl.getListLen(key)
	if err != nil {
		return "", err
	}
	if length == 0 {
		return "", err_def.ErrKeyNotFound
	}

	// 获取列表头尾指针
	head, _, err := rl.getListPointers(key)
	if err != nil {
		return "", err
	}

	// 获取头部元素的值
	value, err := rl.dw.GetDB().Get(GetListItemKey(key, head))
	if err != nil {
		return "", err
	}

	// 删除头部元素
	if err := wb.Delete(GetListItemKey(key, head)); err != nil {
		return "", err
	}

	// 更新头指针和长度
	head++
	length--

	if length > 0 {
		// 列表仍有元素，更新头指针
		if err := wb.Put(GetListHeadKey(key), strconv.FormatInt(head, 10)); err != nil {
			return "", err
		}
	} else {
		// 列表已空，删除头尾指针
		if err := wb.Delete(GetListHeadKey(key)); err != nil {
			return "", err
		}
		if err := wb.Delete(GetListTailKey(key)); err != nil {
			return "", err
		}
	}

	// 更新列表长度(没有进行删除)
	if err := rl.setListLen(wb, key, length); err != nil {
		return "", err
	}

	if err := wb.Commit(); err != nil {
		return "", err
	}

	return value, nil
}

// RPop 从列表右侧（尾部）弹出一个元素
func (rl *RList) RPop(key string) (string, error) {
	wb := rl.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	length, err := rl.getListLen(key)
	if err != nil {
		return "", err
	}
	if length == 0 {
		return "", err_def.ErrKeyNotFound
	}

	// 获取列表头尾指针
	_, tail, err := rl.getListPointers(key)
	if err != nil {
		return "", err
	}

	value, err := rl.dw.GetDB().Get(GetListItemKey(key, tail))
	if err != nil {
		return "", err
	}

	if err := wb.Delete(GetListItemKey(key, tail)); err != nil {
		return "", err
	}

	tail--
	length--

	if length > 0 {
		if err := wb.Put(GetListTailKey(key), strconv.FormatInt(tail, 10)); err != nil {
			return "", err
		}
	} else {
		if err := wb.Delete(GetListHeadKey(key)); err != nil {
			return "", err
		}
		if err := wb.Delete(GetListTailKey(key)); err != nil {
			return "", err
		}
	}

	// 更新列表长度(没有进行删除)
	if err := rl.setListLen(wb, key, length); err != nil {
		return "", err
	}

	if err := wb.Commit(); err != nil {
		return "", err
	}

	return value, nil
}
