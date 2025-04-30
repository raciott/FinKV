package command

import (
	"FinKV/database"
	"encoding/json"
	"fmt"
)

// CmdTyp 表示命令类型的枚举
type CmdTyp uint8

const (
	// CmdString 字符串类型命令
	CmdString CmdTyp = iota
	// CmdList 列表类型命令
	CmdList
	// CmdHash 哈希表类型命令
	CmdHash
	// CmdSet 集合类型命令
	CmdSet
	// CmdZSet 有序集合类型命令
	CmdZSet
)

// MethodTyp 表示命令方法的枚举
type MethodTyp uint8

const (

	// MethodSet 设置键值对
	MethodSet MethodTyp = iota
	// MethodDel 删除键
	MethodDel
	// MethodIncr 将键对应的值加1
	MethodIncr
	// MethodIncrBy 将键对应的值加上指定的增量
	MethodIncrBy
	// MethodDecr 将键对应的值减1
	MethodDecr
	// MethodDecrBy 将键对应的值减去指定的减量
	MethodDecrBy
	// MethodAppend 将值追加到键对应的字符串末尾
	MethodAppend
	// MethodGetSet 设置新值并返回旧值
	MethodGetSet
	// MethodSetNX 只有在键不存在时设置键值对
	MethodSetNX
	// MethodMSet 批量设置键值对
	MethodMSet

	// MethodHSet 设置哈希表字段的值
	MethodHSet
	// MethodHMSet 批量设置哈希表字段的值
	MethodHMSet
	// MethodHDel 删除哈希表中的一个或多个字段
	MethodHDel
	// MethodHIncrBy 将哈希表中指定字段的值增加指定增量
	MethodHIncrBy
)

var (
	// ErrArgsCount 参数数量错误
	ErrArgsCount = fmt.Errorf("args count error")
	// ErrSyntax 语法错误
	ErrSyntax = fmt.Errorf("syntax error")
)

// Command 接口定义了命令的基本操作
type Command interface {
	// GetType 获取命令类型
	GetType() CmdTyp

	// GetMethod 获取命令方法
	GetMethod() MethodTyp

	// Apply 将命令应用到状态机
	Apply(db *database.FincasDB) error

	// Encode 将命令编码为字节数组
	Encode() ([]byte, error)
}

// BaseCmd 是所有命令的基础结构
type BaseCmd struct {
	Typ    CmdTyp    `json:"type"`   // 命令类型
	Method MethodTyp `json:"method"` // 命令方法
	Args   [][]byte  `json:"args"`   // 命令参数
}

// GetType 返回命令类型
func (c *BaseCmd) GetType() CmdTyp {
	return c.Typ
}

// GetMethod 返回命令方法
func (c *BaseCmd) GetMethod() MethodTyp {
	return c.Method
}

// Encode 将命令编码为JSON格式的字节数组
func (c *BaseCmd) Encode() ([]byte, error) {
	return json.Marshal(c)
}

// New 创建一个新的命令实例
func New(typ CmdTyp, method MethodTyp, args [][]byte) Command {
	switch typ {
	case CmdString:
		// 创建字符串命令
		return &StringCmd{
			BaseCmd: BaseCmd{
				Typ:    typ,
				Method: method,
				Args:   args,
			},
		}
	case CmdHash:
		// 创建哈希表命令
		return &HashCmd{
			BaseCmd: BaseCmd{
				Typ:    typ,
				Method: method,
				Args:   args,
			},
		}
	default:
		return nil
	}
}
