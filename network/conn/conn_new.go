package conn

import (
	"FinKV/network/protocol"
	"context"
	"net"
	"sync"
	"time"
)

type Stats struct { // 连接统计信息
	Created    time.Time // 创建时间
	LastActive time.Time // 最后活跃时间
	ReadBytes  int64     // 读取字节数
	WriteBytes int64     // 写入字节数
	ReadCmds   int64     // 读取命令数
	WriteCmds  int64     // 写入命令数
	Errors     int64     // 错误计数
}

type Connection struct { // 连接封装
	conn   net.Conn           // 底层连接
	parser *protocol.Parser   // RESP协议解析器
	writer *protocol.Writer   // RESP协议写入器
	stats  *Stats             // 统计信息
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
		stats: &Stats{
			Created:    time.Now(),
			LastActive: time.Now(),
		},
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
		c.stats.Errors++
		return err
	}

	c.stats.WriteCmds++
	c.stats.LastActive = time.Now()
	return nil
}

// WriteError 写入错误
func (c *Connection) WriteError(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	werr := c.writer.WriteError(err)
	if werr != nil {
		c.stats.Errors++
		return werr
	}

	c.stats.Errors++
	c.stats.LastActive = time.Now()
	return nil
}

func (c *Connection) WriteInteger(n int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.writer.WriteInteger(n)
	if err != nil {
		c.stats.Errors++
		return err
	}

	c.stats.WriteCmds++
	c.stats.LastActive = time.Now()
	return nil
}

// ReadCommand 读取命令
func (c *Connection) ReadCommand() (*protocol.Command, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd, err := c.parser.Parse()

	if err != nil {
		c.stats.Errors++
		return nil, err
	}

	c.stats.ReadCmds++
	c.stats.LastActive = time.Now()
	return cmd, nil
}

// Stats 获取统计信息
func (c *Connection) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return *c.stats
}

// WriteArray 写入数组
func (c *Connection) WriteArray(arr [][]byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.writer.WriteArray(arr)
	if err != nil {
		c.stats.Errors++
		return err
	}

	c.stats.WriteCmds++
	c.stats.LastActive = time.Now()
	return nil
}
