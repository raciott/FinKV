package node

import (
	"FinKV/cluster/command"
	"FinKV/cluster/fsm"
	"FinKV/cluster/raft"
	"FinKV/database"
	"fmt"
	"time"
)

// Node 表示一个集群节点，包含节点的ID、Raft实例、状态机、数据库和配置信息。
type Node struct {
	id            string             // 节点ID
	raft          *raft.Raft         // Raft实例
	fStateMachine *fsm.FSM           // 状态机
	db            *database.FincasDB // 数据库实例
	conf          *Config            // 节点配置
}

// Config 包含节点的配置信息，如节点ID、Raft目录、Raft绑定地址、加入地址和是否引导。
type Config struct {
	NodeID    string // 节点ID
	RaftDir   string // Raft数据存储目录
	RaftBind  string // Raft绑定地址
	JoinAddr  string // 加入集群的地址
	Bootstrap bool   // 是否引导集群
}

// New 创建一个新的节点实例，并初始化Raft。
func New(db *database.FincasDB, conf *Config) (*Node, error) {
	f := fsm.New(db)
	node := &Node{
		id:            conf.NodeID,
		fStateMachine: f,
		db:            db,
		conf:          conf,
	}

	// 初始化Raft
	if err := node.setupRaft(); err != nil {
		return nil, fmt.Errorf("failed to setup raft node: %v", err)
	}

	return node, nil
}

// ID 返回节点的ID。
func (n *Node) ID() string {
	return n.id
}

// setupRaft 初始化Raft实例，包括配置、传输、快照存储、日志存储和稳定存储。
func (n *Node) setupRaft() error {

	newRaft := raft.NewRaft(n.conf.RaftBind, n.id, n.fStateMachine)

	n.raft = newRaft

	go raft.StartRaftServer(n.raft, n.conf.JoinAddr)

	return nil
}

// Apply 应用一个命令到Raft集群，只有Leader节点可以执行此操作。
func (n *Node) Apply(cmd command.Command) error {
	if !n.IsLeader() {
		return fmt.Errorf("raft is not leader")
	}

	// 编码命令
	data, err := cmd.Encode()

	if err != nil {
		return fmt.Errorf("failed to encode command: %v", err)
	}

	// 应用命令到Raft集群
	err = n.raft.Apply(data, 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to apply command: %v", err)
	}

	return nil
}

// IsLeader 检查当前节点是否是Leader。
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// GetLeaderAddr 获取当前Leader节点的地址。
func (n *Node) GetLeaderAddr() string {
	addr := n.raft.CurrentLeader
	return addr
}

// Shutdown 关闭Raft节点。
func (n *Node) Shutdown() error {
	err := n.raft.Shutdown()
	if err != nil {
		return fmt.Errorf("failed to shutdown raft node: %v", err)
	}
	return nil
}
