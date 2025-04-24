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
	case "PING":
		return h.handlePing(conn, cmd)
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
