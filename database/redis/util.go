package redis

import "fmt"

var (
	StringPrefix = "string"
	HashPrefix   = "hash"
	ListPrefix   = "list"
	SetPrefix    = "set"
	ZSetPrefix   = "zset"
)

func GetStringKey(key string) string {
	return fmt.Sprintf("%s:%s", StringPrefix, key)
}

func GetHashFieldKey(key, field string) string {
	return fmt.Sprintf("%s:%s:%s", HashPrefix, key, field)
}

func GetHashLenKey(key string) string {
	return fmt.Sprintf("%s:%s:len", HashPrefix, key)
}

func GetListItemKey(key string, index int64) string {
	return fmt.Sprintf("%s:%s:%d", ListPrefix, key, index)
}

func GetListLenKey(key string) string {
	return fmt.Sprintf("%s:%s:_len_", ListPrefix, key)
}

func GetListHeadKey(key string) string {
	return fmt.Sprintf("%s:%s:head", ListPrefix, key)
}

func GetListTailKey(key string) string {
	return fmt.Sprintf("%s:%s:tail", ListPrefix, key)
}

func GetSetLenKey(key string) string {
	return fmt.Sprintf("%s:%s:_len_", SetPrefix, key)
}

func GetSetMemberKey(key, member string) string {
	return fmt.Sprintf("%s:%s:%s", SetPrefix, key, member)
}
