package conn

import (
	"FinKV/network/protocol"
	"context"
	"net"
	"sync"
)

type Connection struct { // 连接封装
	conn   net.Conn           // 底层连接
	parser *protocol.Parser   // RESP协议解析器
	writer *protocol.Writer   // RESP协议写入器
	ctx    context.Context    // 上下文
	cancel context.CancelFunc // 取消函数
	closed bool               // 关闭状态
	mu     sync.RWMutex       // 读写锁
}

// New 创建一个连接
func New(conn net.Conn) *Connection {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Connection{
		conn:   conn,
		parser: protocol.NewParser(conn),
		writer: protocol.NewWriter(conn),
		ctx:    ctx,
		cancel: cancel,
	}

	return c
}

// Close 关闭连接
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	c.cancel()
	return c.conn.Close()
}

// WriteString 写入字符串
func (c *Connection) WriteString(s string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.writer.WriteString(s)
	if err != nil {
		return err
	}

	return nil
}

// WriteError 写入错误
func (c *Connection) WriteError(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err = c.writer.WriteError(err)
	if err != nil {
		return err
	}

	return nil
}

// WriteInteger 写入整数
func (c *Connection) WriteInteger(n int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.writer.WriteInteger(n)
	if err != nil {
		return err
	}

	return nil
}

// ReadCommand 读取命令
func (c *Connection) ReadCommand() (*protocol.Command, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd, err := c.parser.Parse()

	if err != nil {

		return nil, err
	}

	return cmd, nil
}

// WriteArray 写入一堆数组
func (c *Connection) WriteArray(arr [][]byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.writer.WriteArray(arr)
	if err != nil {
		return err
	}

	return nil
}

// WriteBulk 写入一个数组
func (c *Connection) WriteBulk(b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.writer.WriteBulk(b)
	if err != nil {

		return err
	}

	return nil
}
