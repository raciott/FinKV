package handler

import (
	"FinKV/database"
	"FinKV/network/conn"
	"FinKV/network/protocol"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrWrongArgCount = errors.New("wrong number of arguments")
	ErrSyntax        = errors.New("syntax error")
)

type Handler struct {
	db *database.FincasDB
}

// New 创建handler
func New(db *database.FincasDB) *Handler {
	return &Handler{
		db: db,
	}
}

// Handle 处理命令
func (h *Handler) Handle(conn *conn.Connection, cmd *protocol.Command) error {
	switch strings.ToUpper(cmd.Name) {
	//system
	case "PING":
		return h.handlePing(conn, cmd)
	// string
	case "SET":
		return h.handleSet(conn, cmd)
	case "GET":
		return h.handleGet(conn, cmd)
	case "DEL":
		return h.handleDel(conn, cmd)
	case "INCR":
		return h.handleIncr(conn, cmd)
	case "INCRBY":
		return h.handleIncrBy(conn, cmd)
	case "DECR":
		return h.handleDecr(conn, cmd)
	case "DECRBY":
		return h.handleDecrBy(conn, cmd)
	case "APPEND":
		return h.handleAppend(conn, cmd)
	case "GETSET":
		return h.handleGetSet(conn, cmd)
	case "SETNX":
		return h.handleSetNX(conn, cmd)
	case "MSET":
		return h.handleMSet(conn, cmd)
	case "MGET":
		return h.handleMGet(conn, cmd)
	case "STRLEN":
		return h.handleStrLen(conn, cmd)
	// List commands
	case "LPUSH":
		return h.handleLPush(conn, cmd)
	case "RPUSH":
		return h.handleRPush(conn, cmd)
	case "LPOP":
		return h.handleLPop(conn, cmd)
	case "RPOP":
		return h.handleRPop(conn, cmd)
	case "LRANGE":
		return h.handleLRange(conn, cmd)
	case "LLEN":
		return h.handleLLen(conn, cmd)
	// Hash commands
	case "HSET":
		return h.handleHSet(conn, cmd)
	case "HGET":
		return h.handleHGet(conn, cmd)
	case "HMSET":
		return h.handleHMSet(conn, cmd)
	case "HMGET":
		return h.handleHMGet(conn, cmd)
	case "HDEL":
		return h.handleHDel(conn, cmd)
	case "HEXISTS":
		return h.handleHExists(conn, cmd)
	case "HKEYS":
		return h.handleHKeys(conn, cmd)
	case "HVALS":
		return h.handleHVals(conn, cmd)
	case "HGETALL":
		return h.handleHGetAll(conn, cmd)
	case "HLEN":
		return h.handleHLen(conn, cmd)
	// Set commands
	case "SADD":
		return h.handleSAdd(conn, cmd)
	case "SREM":
		return h.handleSRem(conn, cmd)
	case "SMEMBERS":
		return h.handleSMembers(conn, cmd)
	default:
		return nil
	}
}

// 返回pong,用于ping-pong测试连接
func (h *Handler) handlePing(conn *conn.Connection, cmd *protocol.Command) error {

	if len(cmd.Args) > 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	if len(cmd.Args) == 1 && string(cmd.Args[0]) != " " {
		return conn.WriteString(fmt.Sprintf("%s", string(cmd.Args[0])))
	}

	return conn.WriteString("PONG")
}

// 处理set请求
func (h *Handler) handleSet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	err := h.db.Set(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString("OK")
}

// 处理get请求
func (h *Handler) handleGet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := h.db.Get(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString(val)
}

// 处理del请求(支持批量删除)
func (h *Handler) handleDel(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 1 {
		return conn.WriteError(ErrSyntax)
	}

	keys := make([]string, 0, len(cmd.Args))
	for _, arg := range cmd.Args {
		keys = append(keys, string(arg))
	}

	err := h.db.Del(keys...)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString("OK")
}

// 处理incr请求(自动增加1)
func (h *Handler) handleIncr(conn *conn.Connection, cmd *protocol.Command) error {

	// 一个命令只能有一个参数
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.Incr(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

// 处理incrby请求(指定增加的量)
func (h *Handler) handleIncrBy(conn *conn.Connection, cmd *protocol.Command) error {
	// 一个命令要有两个参数
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid integer value: %s", string(cmd.Args[1])))
	}

	n, err := h.db.IncrBy(string(cmd.Args[0]), val)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

// 处理decr请求
func (h *Handler) handleDecr(conn *conn.Connection, cmd *protocol.Command) error {
	// 一个命令只能有一个参数
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.Decr(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

// 处理decrby请求
func (h *Handler) handleDecrBy(conn *conn.Connection, cmd *protocol.Command) error {
	// 一个命令要有两个参数
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid integer value: %s", string(cmd.Args[1])))
	}

	n, err := h.db.DecrBy(string(cmd.Args[0]), val)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

// 处理append请求
func (h *Handler) handleAppend(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.Append(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

// 处理getset请求
func (h *Handler) handleGetSet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := h.db.GetSet(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString(val)
}

// 处理setnx请求
func (h *Handler) handleSetNX(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	ok, err := h.db.SetNX(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	if ok {
		return conn.WriteInteger(1)
	}
	return conn.WriteInteger(0)
}

// 处理mset(批量设置)
func (h *Handler) handleMSet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	if len(cmd.Args)%2 != 0 {
		return conn.WriteError(ErrSyntax)
	}

	kvPairs := make(map[string]string, len(cmd.Args)/2)
	for i := 0; i < len(cmd.Args); i += 2 {
		kvPairs[string(cmd.Args[i])] = string(cmd.Args[i+1])
	}

	err := h.db.MSet(kvPairs)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString("OK")
}

// 处理mget(批量获取)
func (h *Handler) handleMGet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	keys := make([]string, 0, len(cmd.Args))
	for _, arg := range cmd.Args {
		keys = append(keys, string(arg))
	}

	kvMap, err := h.db.MGet(keys...)
	fmt.Println(kvMap)
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, key := range keys {
		res = append(res, []byte(kvMap[key]))
	}

	return conn.WriteArray(res)
}

// 处理strlen(获取字符串长度)
func (h *Handler) handleStrLen(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.StrLen(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

// 处理lpush(批量添加到列表头部)
func (h *Handler) handleLPush(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	var vals []string
	for _, arg := range cmd.Args[1:] {
		vals = append(vals, string(arg))
	}

	n, err := h.db.LPush(key, vals...)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

// 处理rpush(批量添加到列表尾部)
func (h *Handler) handleRPush(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	var vals []string
	for _, arg := range cmd.Args[1:] {
		vals = append(vals, string(arg))
	}

	n, err := h.db.RPush(key, vals...)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

// 处理lpop(从列表头部弹出)
func (h *Handler) handleLPop(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := h.db.LPop(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString(val)
}

// 处理rpop(从列表尾部弹出)
func (h *Handler) handleRPop(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := h.db.RPop(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString(val)
}

// 处理lrange(获取列表指定区间的元素)
func (h *Handler) handleLRange(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	start, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid start value %s", string(cmd.Args[1])))
	}
	end, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid end value %s", string(cmd.Args[2])))
	}

	vals, err := h.db.LRange(string(cmd.Args[0]), int(start), int(end))
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, val := range vals {
		res = append(res, []byte(val))
	}

	return conn.WriteArray(res)
}

// 处理llen(获取列表长度)
func (h *Handler) handleLLen(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.LLen(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

// 处理hset(设置哈希值)
func (h *Handler) handleHSet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	err := h.db.HSet(string(cmd.Args[0]), string(cmd.Args[1]), string(cmd.Args[2]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString("OK")
}

// 处理hget(获取哈希值)
func (h *Handler) handleHGet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := h.db.HGet(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString(val)
}

// 处理hmset(批量设置哈希值)
func (h *Handler) handleHMSet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	if (len(cmd.Args)-1)%2 != 0 {
		return conn.WriteError(ErrSyntax)
	}

	kvPairs := make(map[string]string, (len(cmd.Args)-1)/2)
	for i := 1; i < len(cmd.Args); i += 2 {
		kvPairs[string(cmd.Args[i])] = string(cmd.Args[i+1])
	}

	err := h.db.HMSet(string(cmd.Args[0]), kvPairs)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString("OK")
}

// 处理hmget(批量获取哈希值)
func (h *Handler) handleHMGet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	fields := make([]string, 0, len(cmd.Args))
	for _, arg := range cmd.Args {
		fields = append(fields, string(arg))
	}

	kvMap, err := h.db.HMGet(fields[0], fields[1:]...)
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for i := 1; i < len(fields); i++ {
		res = append(res, []byte(kvMap[fields[i]]))
	}

	return conn.WriteArray(res)
}

// 处理hdel(删除哈希值)
func (h *Handler) handleHDel(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	fields := make([]string, 0, len(cmd.Args))
	for _, arg := range cmd.Args {
		fields = append(fields, string(arg))
	}

	n, err := h.db.HDel(fields[0], fields[1:]...)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

// 处理hexists(判断哈希值是否存在)
func (h *Handler) handleHExists(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	ok, err := h.db.HExists(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	if ok {
		return conn.WriteInteger(1)
	}
	return conn.WriteInteger(0)
}

// 处理hkeys(获取所有哈希键)
func (h *Handler) handleHKeys(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	keys, err := h.db.HKeys(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, key := range keys {
		res = append(res, []byte(key))
	}

	return conn.WriteArray(res)
}

// 处理hvals(获取所有哈希值)
func (h *Handler) handleHVals(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	vals, err := h.db.HVals(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, val := range vals {
		res = append(res, []byte(val))
	}

	return conn.WriteArray(res)
}

// 处理hgetall(获取所有哈希键值对)
func (h *Handler) handleHGetAll(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	kvMap, err := h.db.HGetAll(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for key, val := range kvMap {
		res = append(res, []byte(key))
		res = append(res, []byte(val))
	}

	return conn.WriteArray(res)
}

// 处理hlen(获取哈希键的数量)
func (h *Handler) handleHLen(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.HLen(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

// 处理hincrby(对哈希值进行加法运算)
func (h *Handler) handleHIncrBy(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid value %s", string(cmd.Args[2])))
	}

	n, err := h.db.HIncrBy(string(cmd.Args[0]), string(cmd.Args[1]), val)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

// 处理sadd(添加元素到集合)
func (h *Handler) handleSAdd(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	var words []string
	for _, arg := range cmd.Args[1:] {
		words = append(words, string(arg))
	}

	n, err := h.db.SAdd(key, words...)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

// 处理smembers(获取集合中的所有元素)
func (h *Handler) handleSMembers(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	members, err := h.db.SMembers(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, member := range members {
		res = append(res, []byte(member))
	}

	return conn.WriteArray(res)
}

// 处理srem(从集合中删除元素)
func (h *Handler) handleSRem(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	var members []string
	for _, arg := range cmd.Args[1:] {
		members = append(members, string(arg))
	}

	n, err := h.db.SRem(key, members...)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}
