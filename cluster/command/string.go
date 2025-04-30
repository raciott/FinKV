package command

import (
	"FinKV/database"
	"fmt"
	"strconv"
)

// StringCmd 字符串命令结构体，用于处理所有字符串相关的操作

type StringCmd struct {
	BaseCmd // 继承基础命令结构体
}

// Apply 应用字符串命令到数据库
// 根据不同的命令方法执行相应的操作
func (c *StringCmd) Apply(db *database.FincasDB) error {
	// 根据命令方法类型执行不同的操作
	switch c.GetMethod() {
	case MethodSet: // SET 命令：设置键值对
		if len(c.Args) != 2 { // 检查参数数量是否正确
			return ErrArgsCount // 参数数量错误
		}
		return db.Set(string(c.Args[0]), string(c.Args[1])) // 设置键值对
	case MethodDel: // DEL 命令：删除键
		if len(c.Args) != 1 { // 检查参数数量是否正确
			return ErrArgsCount // 参数数量错误
		}
		return db.Del(string(c.Args[0])) // 删除指定键
	case MethodIncr: // INCR 命令：将键对应的值加1
		if len(c.Args) != 1 { // 检查参数数量是否正确
			return ErrArgsCount // 参数数量错误
		}
		_, err := db.Incr(string(c.Args[0])) // 将键对应的值加1
		return err
	case MethodIncrBy: // INCRBY 命令：将键对应的值增加指定的整数
		if len(c.Args) != 2 { // 检查参数数量是否正确
			return ErrArgsCount // 参数数量错误
		}
		val, err := strconv.ParseInt(string(c.Args[1]), 10, 64) // 将第二个参数转换为整数
		if err != nil {                                         // 转换失败
			return err // 返回错误
		}
		_, err = db.IncrBy(string(c.Args[0]), val) // 将键对应的值增加指定的整数
		return err
	case MethodDecr: // DECR 命令：将键对应的值减1
		if len(c.Args) != 1 { // 检查参数数量是否正确
			return ErrArgsCount // 参数数量错误
		}
		_, err := db.Decr(string(c.Args[0])) // 将键对应的值减1
		return err
	case MethodDecrBy: // DECRBY 命令：将键对应的值减少指定的整数
		if len(c.Args) != 2 { // 检查参数数量是否正确
			return ErrArgsCount // 参数数量错误
		}
		val, err := strconv.ParseInt(string(c.Args[1]), 10, 64) // 将第二个参数转换为整数
		if err != nil {                                         // 转换失败
			return err // 返回错误
		}
		_, err = db.DecrBy(string(c.Args[0]), val) // 将键对应的值减少指定的整数
		return err
	case MethodAppend: // APPEND 命令：将字符串追加到键对应的值的末尾
		if len(c.Args) != 2 { // 检查参数数量是否正确
			return ErrArgsCount // 参数数量错误
		}
		_, err := db.Append(string(c.Args[0]), string(c.Args[1])) // 将字符串追加到键对应的值的末尾
		return err
	case MethodGetSet: // GETSET 命令：设置键的新值并返回旧值
		if len(c.Args) != 2 { // 检查参数数量是否正确
			return ErrArgsCount // 参数数量错误
		}
		_, err := db.GetSet(string(c.Args[0]), string(c.Args[1])) // 设置键的新值并返回旧值
		return err
	case MethodSetNX: // SETNX 命令：只有当键不存在时才设置键值对
		if len(c.Args) != 2 { // 检查参数数量是否正确
			return ErrArgsCount // 参数数量错误
		}
		_, err := db.SetNX(string(c.Args[0]), string(c.Args[1])) // 只有当键不存在时才设置键值对
		return err
	case MethodMSet: // MSET 命令：同时设置多个键值对
		if len(c.Args) < 2 { // 检查参数数量是否足够
			return ErrArgsCount // 参数数量错误
		}
		if len(c.Args)%2 != 0 { // 检查参数数量是否为偶数
			return ErrSyntax // 语法错误
		}
		// 创建键值对映射
		kvPairs := make(map[string]string, len(c.Args)/2)
		// 遍历参数，每两个一组构成一个键值对
		for i := 0; i < len(c.Args); i += 2 {
			kvPairs[string(c.Args[i])] = string(c.Args[i+1])
		}
		return db.MSet(kvPairs) // 同时设置多个键值对
	default: // 未支持的方法
		return fmt.Errorf("unsoprted method in string command") // 返回错误信息
	}
}
