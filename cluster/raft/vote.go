package raft

import (
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

type RequestVoteArgs struct {
	NodeId       string // 候选人的ID
	Term         int    // 发起请求节点的 Term
	LastLogIndex int    // 最新的日志索引
	LastLogTerm  int    // 最新日志的 Term
}

type RequestVoteReply struct {
	NodeId      string // Follower 的 ID
	Term        int    // 响应节点的 Term
	VoteGranted bool   // 是否同意投票
}

// BecomeCandidate 修改节点为候选人状态
func (rf *Raft) BecomeCandidate() bool {
	//休眠随机时间后，再开始成为候选人
	r := rand.Int63n(3000) + 1000
	time.Sleep(time.Duration(r) * time.Millisecond)

	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	if rf.CurrentRole == Follower && rf.CurrentLeader == "null" && rf.VotedFor == "null" {
		rf.CurrentRole = Candidate
		rf.VotedFor = rf.RaftAddr
		rf.CurrentTerm += 1
		rf.VoteReceived = append(rf.VoteReceived, rf.RaftAddr)

		// 本节点已变为候选人状态
		log.Println("This node has been changed to Candidate")
		return true
	} else {
		return false
	}
}

// StartElection 开始选举
func (rf *Raft) StartElection(args *RequestVoteArgs) {
	// 开始选举的时候才开始选举定时
	rf.resetElectionTimer()
	log.Print("Start to election Leader\n")

	var wg sync.WaitGroup
	var mu sync.Mutex

	// 决定选举是否中断
	cancelled := false

	for i := 0; i < len(rf.Peers); i++ {
		peer := rf.Peers[i]
		if peer == rf.RaftAddr { // 自己不参与选举(初始化时已经选了)
			continue
		}
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()

			if rf.CurrentRole != Candidate {
				return
			}
			log.Printf("节点 %s 向 %s 发起投票请求\n", rf.RaftAddr, peer)

			client, err := rpc.DialHTTP("tcp", peer)
			if err != nil {
				log.Println("Dialing error: ", err)
				rf.Mu.Lock()
				for i := 0; i < len(rf.Peers); i++ {
					if rf.Peers[i] == peer {
						rf.Peers = append(rf.Peers[:i], rf.Peers[i+1:]...)
						break
					}
				}

				rf.Mu.Unlock()
				return
			}
			var reply RequestVoteReply
			err = client.Call("Raft.ReplyVote", args, &reply)
			if err != nil {
				log.Println("RPC error: ", err)
				return
			}

			if rf.CollectVotes(&reply) {
				mu.Lock()
				cancelled = true
				mu.Unlock()
				return
			} else {
				rf.Mu.Lock()
				if rf.CurrentRole == Follower {
					log.Println("结束收集投票")
					rf.Mu.Unlock()
					return
				} else {
					rf.Mu.Unlock()
				}
			}
		}(peer)
	}

	wg.Wait()

	// 无论是否选举成功都结束选举定时
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	rf.ElectionTimer.Stop()

	if cancelled {
		// 选举成功
		log.Println("\033[32mThe election was successful\033[0m")
		for _, follower := range rf.Peers {
			// 跳过自己
			if follower == rf.RaftAddr {
				continue
			}
			rf.SentLength[follower] = len(rf.Logs)
			rf.AckedLength[follower] = 0
			go rf.Replicating(follower)
		}
	} else {
		// 选举失败
		log.Println("\033[38;5;214mElection defeat!!!\033[0m")
		rf.CurrentRole = Follower

		if rf.CurrentLeader == rf.RaftAddr {
			rf.CurrentLeader = "null"
		}

		if rf.VotedFor == rf.RaftAddr {
			rf.VotedFor = "null"
		}
		rf.VoteReceived = []string{}
	}
}

// ReplyVote 处理投票请求
func (rf *Raft) ReplyVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	log.Printf("节点 %s 收到来自 %s 的投票请求\n", rf.RaftAddr, args.NodeId)

	myLogTerm := 0
	if len(rf.Logs) > 0 {
		myLogTerm = rf.Logs[len(rf.Logs)-1].Term
	}

	// 判断投票条件
	logOK := args.LastLogTerm > myLogTerm || (args.LastLogTerm == myLogTerm && args.LastLogIndex >= len(rf.Logs))
	termOK := args.Term > rf.CurrentTerm || (args.Term == rf.CurrentTerm && (rf.VotedFor == "null" || rf.VotedFor == args.NodeId))

	if termOK && logOK {
		rf.CurrentTerm = args.Term
		rf.CurrentRole = Follower
		rf.VotedFor = args.NodeId
		log.Print("Agree to vote\n")
		reply.VoteGranted = true
	} else {
		log.Print("Refuse to vote\n")
		reply.VoteGranted = false
	}
	reply.NodeId = rf.RaftAddr
	reply.Term = rf.CurrentTerm
	return nil
}

// CollectVotes 收集投票
func (rf *Raft) CollectVotes(reply *RequestVoteReply) bool {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	if rf.CurrentRole == Candidate && reply.Term == rf.CurrentTerm && reply.VoteGranted {
		rf.VoteReceived = append(rf.VoteReceived, reply.NodeId)
		if len(rf.VoteReceived) > len(rf.Peers)/2 {
			rf.CurrentRole = Leader
			rf.CurrentLeader = rf.RaftAddr
			log.Println("已获得超过二分之一票数")
			return true
		}
	} else if reply.Term > rf.CurrentTerm {
		log.Println("存在更新的 Term")
		rf.CurrentTerm = reply.Term
		rf.CurrentRole = Follower
		rf.VotedFor = reply.NodeId

		return false
	}
	return false
}
