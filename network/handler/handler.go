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

// 处理incr请求
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
