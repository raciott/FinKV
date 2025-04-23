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
