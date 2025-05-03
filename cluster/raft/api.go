package raft

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

// StartRaftServer 启动Raft服务器及其相关组件
func StartRaftServer(rf *Raft, joinAddr string) {

	// 启动raft服务器
	go startRaftServer(rf)

	// 等待2秒，确保raft服务器启动成功
	time.Sleep(1 * time.Second)

	// 若是没有指定集群节点，以自身做作为集群节点
	if joinAddr == "" {
		joinAddr = rf.RaftAddr
	}

	// 创建RPC客户端(与集群节点进行连接)
	client, err := rpc.DialHTTP("tcp", joinAddr)
	if err != nil {
		log.Fatalf("Error connecting to central node: %s", err)
	}
	args := &AddPeerArgs{NewPeer: rf.RaftAddr}
	var reply AddPeerReply
	err = client.Call("Raft.RegisterNode", args, &reply)
	if err != nil {
		log.Fatalf("RPC error: %s", err)
	}

	// 获取集群节点列表并记录
	rf.Mu.Lock()
	rf.Peers = reply.Peers
	log.Printf("Node %s initial peers: %v\n", rf.NodeID, rf.Peers)
	rf.Mu.Unlock()

	go rf.HeartBeatTimerStart() // 启动心跳检测(用于查看从节点是否存活)
	go rf.ElectionTimerStart()  // 启动选举定时器
	rf.ElectionTimer.Stop()     // 停止选举定时器(暂时停止，依靠circle激活)
	go rf.circle()              // 启动心跳检测循环(用于查看leader是否存活)
}

// startRaftServer 启动Raft服务器
func startRaftServer(rf *Raft) {
	err := rpc.Register(rf)
	if err != nil {
		log.Panic(err)
	}

	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", rf.RaftAddr)
	if err != nil {
		log.Fatalf("Error starting server: %s", err)
	}

	// 创建 http.Server 实例并赋值给 rf.Server
	rf.Server = &http.Server{Handler: nil}

	go func() {
		err := rf.Server.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Println(err)
		}
	}()
}

// Apply 日志核心内容(方便外部调用) -- 获取命令后添加日志
func (rf *Raft) Apply(cmd []byte, timeout time.Duration) error {
	// 创建一个带有超时的 context
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel() // 确保 cancel 被调用，释放资源

	// 创建一个通道用于接收结果
	resultChan := make(chan error, 1)

	// 启动 goroutine 执行广播操作
	go func() {
		m := LogEntry{
			Command: cmd, // 创建一个日志条目
		}
		resultChan <- rf.Boradcast(m)
	}()

	// 等待结果或超时
	select {
	case err := <-resultChan:
		// 如果广播操作完成，返回结果
		return err
	case <-ctx.Done():
		// 如果超时，返回超时错误
		return errors.New("操作超时")
	}
}

// Shutdown 关闭Raft服务器
func (rf *Raft) Shutdown() interface{} {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	if rf.Server != nil {
		err := rf.Server.Shutdown(context.Background())
		if err != nil {
			return err
		}
	}
	return nil
}

// circle 循环进行选举
func (rf *Raft) circle() {
Circle:
	for {
		if rf.BecomeCandidate() {
			// 成为候选人节点后，向其他节点请求选票进行选举
			rf.Mu.Lock()
			args := &RequestVoteArgs{
				Term:         rf.CurrentTerm,
				NodeId:       rf.RaftAddr,
				LastLogIndex: len(rf.Logs),
				LastLogTerm:  0,
			}
			if len(rf.Logs) > 0 {
				args.LastLogTerm = rf.Logs[len(rf.Logs)-1].Term
			}
			rf.Mu.Unlock()

			rf.StartElection(args)

			rf.Mu.Lock()
			if rf.CurrentRole == Leader {
				// node成功被选举为leader
				log.Printf("Node %s:%s has become the leader\n", rf.NodeID, rf.RaftAddr)
				rf.Mu.Unlock()
				break
			} else {
				rf.Mu.Unlock()
			}
		} else {
			break
		}
	}

	// 进行心跳检测
	for {
		// 5秒检测一次
		time.Sleep(time.Millisecond * 5000)
		rf.Mu.Lock()

		if rf.CurrentRole != Leader && rf.LastHeartBeatTime != 0 && (time.Now().UnixMilli()-rf.LastHeartBeatTime) > int64(rf.Timeout*1000) {
			log.Printf("心跳检测超时，已超过%d秒\n", rf.Timeout)
			log.Println("即将重新开启选举")
			rf.CurrentRole = Follower
			rf.CurrentLeader = "null"
			rf.VotedFor = "null"
			rf.VoteReceived = []string{}
			rf.LastHeartBeatTime = 0
			rf.Mu.Unlock()
			goto Circle
		} else {
			rf.Mu.Unlock()
		}
	}
}
