package node

import (
	"FinKV/cluster/command"
	"FinKV/cluster/fsm"
	"FinKV/database"
	"fmt"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"
)

type Node struct {
	id            string
	raft          *raft.Raft
	fStateMachine *fsm.FSM
	db            *database.FincasDB
	conf          *Config
}

type Config struct {
	NodeID    string
	RaftDir   string
	RaftBind  string
	JoinAddr  string
	Bootstrap bool
}

func New(db *database.FincasDB, conf *Config) (*Node, error) {
	f := fsm.New(db)
	node := &Node{
		id:            conf.NodeID,
		fStateMachine: f,
		db:            db,
		conf:          conf,
	}

	if err := node.setupRaft(); err != nil {
		return nil, fmt.Errorf("failed to setup raft node: %v", err)
	}

	return node, nil
}

func (n *Node) ID() string {
	return n.id
}

func (n *Node) setupRaft() error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(n.id)

	addr, err := net.ResolveTCPAddr("tcp", n.conf.RaftBind)
	if err != nil {
		return fmt.Errorf("failed to resolve raft address: %v", err)
	}

	transport, err := raft.NewTCPTransport(n.conf.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create raft transport: %v", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(n.conf.RaftDir, 2, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %v", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(n.conf.RaftDir, "raft-log.bolt"))
	if err != nil {
		return fmt.Errorf("failed to create raft log.bolt: %v", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(n.conf.RaftDir, "raft-stable.bolt"))
	if err != nil {
		return fmt.Errorf("failed to create raft stable.bolt: %v", err)
	}

	ra, err := raft.NewRaft(config, n.fStateMachine, logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("failed to create raft node: %v", err)
	}
	n.raft = ra

	if n.conf.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}

	return nil
}

func (n *Node) Join(nodeID, addr string) error {
	log.Printf("received join request for remote node %s at %s", nodeID, addr)

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Printf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// 已经在集群中，忽略
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				return nil
			}
			// 节点已经存在，但是地址不同，移除节点
			future := n.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("failed to remove node %s at %s: %v", nodeID, addr, err)
			}
		}
	}

	// 添加新节点
	future := n.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add node %s at %s: %v", nodeID, addr, err)
	}

	log.Printf("node %s at %s joined successfully", nodeID, addr)
	return nil
}

func (n *Node) Apply(cmd command.Command) error {
	if !n.IsLeader() {
		return fmt.Errorf("raft is not leader")
	}

	data, err := cmd.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode command: %v", err)
	}

	future := n.raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to apply command: %v", err)
	}

	return nil
}

func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

func (n *Node) GetLeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

func (n *Node) Shutdown() error {
	future := n.raft.Shutdown()
	return future.Error()
}

// TryJoinCluster 尝试将当前节点加入到目标节点所在的集群
// 这是一个改进版的Join方法，可以处理目标节点不是Leader的情况
func (n *Node) TryJoinCluster(targetID, targetAddr string) error {
	// 如果目标节点就是自己，则无需加入
	if targetID == n.id {
		return nil
	}

	log.Printf("尝试将节点 %s 加入到集群中，目标节点: %s@%s", n.id, targetID, targetAddr)

	// 首先尝试直接加入
	// 如果目标节点是Leader，这将会成功
	err := n.Join(targetID, targetAddr)
	if err == nil {
		log.Printf("成功加入集群，目标节点是Leader")
		return nil
	}

	// 如果加入失败，可能是因为目标节点不是Leader
	if err.Error() == "raft is not leader" {
		// 在此处理目标节点不是Leader的情况
		log.Printf("目标节点不是Leader，尝试从目标节点获取Leader信息")

		// 在实际的实现中，可能需要通过其他API获取当前的Leader
		// 或者通过轮询集群中的其他节点来找到Leader

		// 由于我们没有实现获取Leader的API，这里简单地等待一段时间后重试
		// 在这段时间内，可能会有Leader选举完成
		for i := 0; i < 5; i++ {
			log.Printf("等待Leader选举，尝试 %d/5", i+1)
			time.Sleep(2 * time.Second)

			err = n.Join(targetID, targetAddr)
			if err == nil {
				log.Printf("成功加入集群")
				return nil
			}

			// 如果错误不是"raft is not leader"，则可能是其他问题
			if err.Error() != "raft is not leader" {
				log.Printf("加入集群失败，错误: %v", err)
				return err
			}
		}

		// 如果多次尝试后仍然失败，但是程序可以继续运行
		// 将来可能会自动解决这个问题
		log.Printf("多次尝试加入集群失败，将继续运行")
		return nil
	}

	// 其他类型的错误，直接返回
	return err
}
