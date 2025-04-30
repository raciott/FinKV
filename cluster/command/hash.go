package command

import (
	"FinKV/database"
	"fmt"
	"strconv"
)

// HashCmd 哈希表命令结构体
type HashCmd struct {
	BaseCmd // 继承基础命令结构
}

// Apply 实现Command接口的Apply方法，将哈希表命令应用到数据库
func (c *HashCmd) Apply(db *database.FincasDB) error {
	switch c.GetMethod() {
	case MethodHSet:
		// HSET key field value - 设置哈希表字段的值
		if len(c.Args) != 3 {
			return ErrArgsCount
		}
		return db.HSet(string(c.Args[0]), string(c.Args[1]), string(c.Args[2]))
	case MethodHMSet:
		// HMSET key field1 value1 field2 value2 ... - 批量设置哈希表字段的值
		if len(c.Args) < 3 {
			return ErrArgsCount
		}
		if (len(c.Args)-1)%2 != 0 {
			return ErrSyntax
		}
		kvPairs := make(map[string]string, (len(c.Args)-1)/2)
		for i := 1; i < len(c.Args); i += 2 {
			kvPairs[string(c.Args[i])] = string(c.Args[i+1])
		}
		return db.HMSet(string(c.Args[0]), kvPairs)
	case MethodHDel:
		// HDEL key field [field ...] - 删除哈希表中的一个或多个字段
		if len(c.Args) < 2 {
			return ErrArgsCount
		}
		fields := make([]string, 0, len(c.Args))
		for _, arg := range c.Args {
			fields = append(fields, string(arg))
		}
		_, err := db.HDel(fields[0], fields[1:]...)
		return err
	case MethodHIncrBy:
		// HINCRBY key field increment - 将哈希表中指定字段的值增加指定增量
		if len(c.Args) != 3 {
			return ErrArgsCount
		}
		val, err := strconv.ParseInt(string(c.Args[2]), 10, 64)
		if err != nil {
			return err
		}
		_, err = db.HIncrBy(string(c.Args[0]), string(c.Args[1]), val)
		return err
	default:
		return fmt.Errorf("unsoprted method in hash command")
	}
}
