package server

import (
	"FinKV/cluster/command"
	"FinKV/cluster/node"
	"FinKV/config"
	"FinKV/database"
	"FinKV/network/conn"
	"FinKV/network/handler"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	Addr           string        // 地址
	IdleTimeout    time.Duration // 空闲超时
	MaxConnections int           // 最大连接数
	ReadTimeout    time.Duration // 读超时时间
	WriteTimeout   time.Duration // 写超时时间
}

type Server struct {
	cfg     *Config            // 网络配置
	db      *database.FincasDB // 数据库实例
	handler *handler.Handler   // 命令处理器

	listener net.Listener   // TCP监听器
	conns    sync.Map       // 活跃连接集合
	connWg   sync.WaitGroup // 连接等待组

	stats *Stats // 统计信息

	ctx     context.Context    // 上下文
	cancel  context.CancelFunc // 取消函数
	closed  bool               // 关闭状态
	closeMu sync.RWMutex       // 关闭锁

	metricsTicker *time.Ticker       // 指标收集定时器
	metricsCancel context.CancelFunc // 指标取消函数

	node *node.Node // 集群节点
}

// New 创建服务器
func New(db *database.FincasDB, address *string) (*Server, error) {
	var (
		addr           = ":8911"
		idleTimeout    = 5 * time.Second
		maxConnections = 1000
		readTimeout    = 10 * time.Second
		writeTimeout   = 10 * time.Second
	) // 如果没有进行配置，则使用默认配置

	if config.Get().Network.Addr != "" && *address == "" {
		addr = config.Get().Network.Addr
	} else if *address != "" {
		addr = *address
	}

	if config.Get().Network.IdleTimeout != 0 {
		idleTimeout = config.Get().Network.IdleTimeout * time.Second
	}
	if config.Get().Network.MaxConns != 0 {
		maxConnections = config.Get().Network.MaxConns
	}
	if config.Get().Network.ReadTimeout != 0 {
		readTimeout = config.Get().Network.ReadTimeout * time.Second
	}
	if config.Get().Network.WriteTimeout != 0 {
		writeTimeout = config.Get().Network.WriteTimeout * time.Second
	}

	cfg := &Config{
		Addr:           addr,
		IdleTimeout:    idleTimeout,
		MaxConnections: maxConnections,
		ReadTimeout:    readTimeout,
		WriteTimeout:   writeTimeout,
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &Server{
		cfg:     cfg,
		db:      db,
		handler: handler.New(db),
		stats:   &Stats{StartTime: time.Now()},
		ctx:     ctx,
		cancel:  cancel,
	}

	return s, nil
}

// Start 启动服务器
func (s *Server) Start(raftAddr, nodeID, joinAddr string) error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return fmt.Errorf("server is already closed")
	}
	s.closeMu.Unlock()

	// 创建TCP监听器
	listener, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("failed to create listener: %v", err)
	}
	s.listener = listener

	// 启动指标收集
	s.startMetricsCollection()

	log.Printf("listening on %s", s.cfg.Addr)

	if raftAddr != "" {
		conf := &node.Config{
			NodeID:   nodeID,
			RaftBind: raftAddr,
			JoinAddr: joinAddr,
			RaftDir:  fmt.Sprintf("raft_data/%s", nodeID),
		}

		// 初始化集群
		if err := s.initCluster(conf); err != nil {
			return err
		}

	}

	// 接受新连接
	for {
		netConn, err := listener.Accept()

		if err != nil {
			if s.closed {
				return nil
			}
			log.Printf("accept error: %v", err)
			continue
		} else {
			log.Printf("accept a new connection")
		}

		// 处理新连接
		go func() {
			err := s.handleConnection(s.ctx, netConn)
			if err != nil {
				log.Fatal(err)
			}
		}()
	}
}

// Stop 停止服务器
func (s *Server) Stop() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return fmt.Errorf("server already closed")
	}
	s.closed = true
	s.closeMu.Unlock()

	s.cancel()

	if s.metricsCancel != nil {
		s.metricsCancel()
	}

	// 关闭监听器
	if s.listener != nil {
		s.listener.Close()
	}

	// 关闭所有连接
	s.conns.Range(func(key, value interface{}) bool {
		if c, ok := value.(*conn.Connection); ok {
			c.Close()
		}
		return true
	})

	// 等待所有连接处理完成
	s.connWg.Wait()

	return nil
}

// 处理连接
func (s *Server) handleConnection(ctx context.Context, netConn net.Conn) error {
	if atomic.LoadInt64(&s.stats.ConnCount) >= int64(s.cfg.MaxConnections) {
		netConn.Close()
		return fmt.Errorf("max connections reached")
	}

	// 创建连接封装
	connection := conn.New(netConn)
	s.conns.Store(netConn, connection)
	s.stats.IncrConnCount()
	s.connWg.Add(1)

	defer func() {
		connection.Close()
		s.conns.Delete(netConn)
		s.stats.DecrConnCount()
		s.connWg.Done()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:

			start := time.Now()
			// 读取命令
			cmd, err := connection.ReadCommand()

			if err != nil {

				if err == io.EOF {
					log.Println("close a client connection")
					return nil
				}

				s.stats.IncrErrorCount()
				log.Printf("failed to read command: %v", err)
				// 如果是客户端断开连接或其他网络错误，优雅地关闭当前连接并返回
				return nil
			}

			// 禁止非Leader节点处理写操作
			cmdP, ok := isWriteCommand(cmd.Name)
			if ok && s.node != nil && !s.node.IsLeader() {
				leaderAddr := s.node
				return connection.WriteError(fmt.Errorf("redirect to leader: %v", leaderAddr))
			}

			// 处理命令
			if err := s.handler.Handle(connection, cmd); err != nil {
				s.stats.IncrErrorCount()
				log.Printf("failed to handler command: %v", err)
			} else if s.node != nil {
				err := s.node.Apply(command.New(cmdP.CmdType, cmdP.Method, cmd.Args))
				if err != nil {
					return fmt.Errorf("failed to apply command: %v", err)
				}
			}

			// 统计
			s.stats.IncrCmdCount()
			if time.Since(start) > time.Millisecond*10 {
				s.stats.IncrSlowCount()
			}
		}
	}
}

// 启动指标收集
func (s *Server) startMetricsCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	s.metricsCancel = cancel

	s.metricsTicker = time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.metricsTicker.C:
				s.collectMetrics()
			}
		}
	}()
}

// 收集指标
func (s *Server) collectMetrics() {
	var totalReadBytes int64
	var totalWriteBytes int64

	s.conns.Range(func(key, value interface{}) bool {
		if c, ok := value.(conn.Connection); ok {
			stats := c.Stats()
			atomic.AddInt64(&totalReadBytes, stats.ReadBytes)
			atomic.AddInt64(&totalWriteBytes, stats.WriteBytes)
		}
		return true
	})

	atomic.StoreInt64(&s.stats.BytesReceived, totalReadBytes)
	atomic.StoreInt64(&s.stats.BytesSent, totalWriteBytes)

	//log.Printf("Metrics: connections=%d commands=%d errors=%d slow_queries=%d bytes_recv=%d bytes_sent=%d",
	//	atomic.LoadInt64(&s.stats.ConnCount),
	//	atomic.LoadInt64(&s.stats.CmdCount),
	//	atomic.LoadInt64(&s.stats.ErrorCount),
	//	atomic.LoadInt64(&s.stats.SlowCount),
	//	totalReadBytes,
	//	totalWriteBytes,
	//)
}

func (s *Server) initCluster(conf *node.Config) error {
	n, err := node.New(s.db, conf)
	if err != nil {
		return fmt.Errorf("failed to create node: %v", err)
	}

	s.node = n

	log.Printf("success to init cluster")

	return nil
}

type cmdPair struct {
	CmdType command.CmdTyp
	Method  command.MethodTyp
}

func isWriteCommand(cmd string) (cmdPair, bool) {
	wCmds := map[string]cmdPair{
		"SET": {command.CmdString, command.MethodSet}, "DEL": {command.CmdString, command.MethodDel}, "INCR": {command.CmdString, command.MethodIncr}, "INCRBY": {command.CmdString, command.MethodIncrBy},
		"DECR": {command.CmdString, command.MethodDecr}, "DECRBY": {command.CmdString, command.MethodDecrBy}, "APPEND": {command.CmdString, command.MethodAppend}, "GETSET": {command.CmdString, command.MethodGetSet},
		"SETNX": {command.CmdString, command.MethodSetNX}, "MSET": {command.CmdString, command.MethodMSet},
		"HSET": {command.CmdHash, command.MethodHSet}, "HMSET": {command.CmdHash, command.MethodHMSet}, "HDEL": {command.CmdHash, command.MethodHDel}, "HINCRBY": {command.CmdHash, command.MethodHIncrBy},
	}
	val, ok := wCmds[strings.ToUpper(cmd)]
	return val, ok
}
