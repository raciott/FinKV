package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

// Role 是一个新的类型，表示 Raft 节点的角色
type Role int

// 定义 Role 类型的常量
const (
	Follower Role = iota
	Candidate
	Leader
)

// AddPeerArgs 定义请求和响应结构
type AddPeerArgs struct {
	NewPeer string
}

type AddPeerReply struct {
	Peers []string
}

// Raft 结构
type Raft struct {
	Mu       sync.RWMutex // 互斥锁
	NodeID   string       // 节点ID
	RaftAddr string       // 当前节点地址

	Peers         []string   // 所有节点的地址
	CurrentTerm   int        // 当前 Term 号
	CurrentRole   Role       // 当前角色
	CurrentLeader string     // 当前 Leader 的地址
	Logs          []LogEntry // 日志
	CommitLength  int

	VotedFor     string   // 当前 Term 中给谁投了票
	VoteReceived []string // 收到的同意投票的地址

	SentLength  map[string]int // 每个节点日志复制的插入点
	AckedLength map[string]int // 每个节点已接收的日志长度
	Server      *http.Server

	ElectionTimer     *time.Timer  // 选举定时器
	HeartBeatTimer    *time.Ticker // 心跳计时器
	LastHeartBeatTime int64        // 上次收到心跳的时间
	Timeout           int          // 心跳超时时间

	LogFile string // 日志文件路径

	fsm FSM // 状态机接口
}

// NewRaft 创建并初始化一个新的 Raft 实例
func NewRaft(addr string, nodeID string, fsm FSM) *Raft {
	rf := &Raft{
		RaftAddr:          addr,
		NodeID:            nodeID,
		Peers:             []string{},
		CurrentTerm:       0,
		CurrentRole:       Follower,
		CurrentLeader:     "null",
		Logs:              []LogEntry{},
		VotedFor:          "null",
		VoteReceived:      []string{},
		CommitLength:      0,
		SentLength:        make(map[string]int),
		AckedLength:       make(map[string]int),
		ElectionTimer:     time.NewTimer(time.Duration(rand.Intn(5000)+5000) * time.Millisecond), // 设置随机过期时间，尽量避免出现多个选举
		HeartBeatTimer:    time.NewTicker(1000 * time.Millisecond),
		LastHeartBeatTime: time.Now().UnixMilli(),
		Timeout:           5,
		LogFile:           "",
		fsm:               fsm,
	}

	// 尝试从本地加载日志
	err := rf.LoadLogs()
	if err != nil {
		log.Printf("加载日志失败: %v\n", err)
	}

	return rf
}

// initRaft 初始化 Raft 实例
func (rf *Raft) initRaft() {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	rf.CurrentTerm += 1
	rf.CurrentRole = Candidate
	rf.VotedFor = rf.RaftAddr               // 先给自己投一票
	rf.VoteReceived = []string{rf.RaftAddr} // 投票列表中加入自己
}

// AddPeer 添加一个新的 peer
func (rf *Raft) AddPeer(args *AddPeerArgs, reply *AddPeerReply) error {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	// 检查是否已经存在该 peer
	for _, peer := range rf.Peers {
		if peer == args.NewPeer {
			fmt.Printf("Node %s: Peer %s already exists\n", rf.RaftAddr, args.NewPeer)
			reply.Peers = rf.Peers
			return nil
		}
	}

	// 如果不存在则添加
	rf.Peers = append(rf.Peers, args.NewPeer)
	if rf.CurrentRole == Leader {
		rf.SentLength[args.NewPeer] = len(rf.Logs)
		rf.AckedLength[args.NewPeer] = 0
		go rf.Replicating(args.NewPeer)
	}
	reply.Peers = rf.Peers

	// 打印更新后的 peers 列表
	log.Printf("Node %s updated peers: %v\n", rf.NodeID, rf.Peers)
	return nil
}

// resetElectionTimer 重置选举定时器
func (rf *Raft) resetElectionTimer() {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	timeout := time.Duration(rand.Intn(5000)+5000) * time.Millisecond
	rf.ElectionTimer.Stop()
	rf.ElectionTimer.Reset(timeout)
}

// ElectionTimerStart 启动选举定时器
func (rf *Raft) ElectionTimerStart() {

	for range rf.ElectionTimer.C {
		rf.Mu.Lock()
		if rf.CurrentRole != Candidate {
			rf.Mu.Unlock()
			continue
		}
		fmt.Println("选举超时")
		rf.Mu.Unlock()
		rf.initRaft()
	}
}

// HeartBeatTimerStart 启动心跳定时器
func (rf *Raft) HeartBeatTimerStart() {
	for range rf.HeartBeatTimer.C {
		rf.Mu.Lock()
		if rf.CurrentRole != Leader {
			rf.Mu.Unlock()
			continue
		}
		rf.Mu.Unlock()
		rf.SendHeartBeat()
	}
}

// RegisterNode 注册新节点
func (rf *Raft) RegisterNode(args *AddPeerArgs, reply *AddPeerReply) error {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	flag := false
	// 检查是否已经存在该 peer
	for _, peer := range rf.Peers {
		if peer == args.NewPeer {
			flag = true
			log.Printf("Node %s: Peer %s already exists\n", rf.NodeID, args.NewPeer)
			reply.Peers = rf.Peers
		}
	}

	// 如果不存在则添加
	if !flag {
		rf.Peers = append(rf.Peers, args.NewPeer)
		if rf.CurrentRole == Leader {
			rf.SentLength[args.NewPeer] = 0 // 从0开始，触发全量复制
			rf.AckedLength[args.NewPeer] = 0
			go rf.Replicating(args.NewPeer) // 使用修改后的Replicating方法，会自动选择全量复制
		}
		log.Printf("Node %s updated peers: %v\n", rf.NodeID, rf.Peers)
	}

	reply.Peers = rf.Peers
	for _, peer := range rf.Peers {
		if peer == args.NewPeer || peer == rf.RaftAddr {
			continue
		}
		client, err := rpc.DialHTTP("tcp", peer)
		if err != nil {
			log.Println("Dialing:", err)
			continue
		}

		addPeerArgs := &AddPeerArgs{NewPeer: args.NewPeer}
		var addPeerReply AddPeerReply
		err = client.Call("Raft.AddPeer", addPeerArgs, &addPeerReply)
		if err != nil {
			log.Println("RPC error:", err)
		}
	}
	return nil
}

func (r *Raft) State() Role {
	return r.CurrentRole
}
