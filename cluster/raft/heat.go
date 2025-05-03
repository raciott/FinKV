package raft

import (
	"log"
	"net/rpc"
	"time"
)

type HeartBeatArgs struct {
	Term     int    // 领导者的任期
	LeaderID string // 领导者的ID
}

type HeartBeatReply struct {
	Term    int  // 跟随者的当前任期
	Success bool // 心跳回应
}

var DEBUG = false

func (rf *Raft) ReceiveHeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return nil
	}

	rf.CurrentTerm = args.Term
	rf.CurrentLeader = args.LeaderID
	rf.CurrentRole = Follower
	// 确保使用当前时间更新心跳时间
	rf.LastHeartBeatTime = time.Now().UnixMilli()
	reply.Term = rf.CurrentTerm
	reply.Success = true

	return nil
}

func (rf *Raft) SendHeartBeat() {
	rf.Mu.Lock()

	if DEBUG {
		log.Println(rf.RaftAddr + " start heartBeat")
	}

	if rf.CurrentRole != Leader {
		rf.LastHeartBeatTime = 1
		rf.Mu.Unlock()
		return
	}
	if len(rf.Peers) == 1 {
		rf.LastHeartBeatTime = 1
		rf.CurrentLeader = "null"
		rf.VotedFor = "null"
		rf.CurrentRole = Follower
		rf.Mu.Unlock()
		return
	}
	rf.Mu.Unlock()

	args := &HeartBeatArgs{
		Term:     rf.CurrentTerm,
		LeaderID: rf.RaftAddr,
	}

	for i := 0; i < len(rf.Peers); i++ {
		peer := rf.Peers[i]
		// 不向自己发送心跳
		if peer == rf.RaftAddr {
			continue
		}

		if DEBUG {
			log.Printf("%s send heartbeat to %s\n", rf.RaftAddr, peer)
		}

		go func(peer string, index int) {
			client, err := rpc.DialHTTP("tcp", peer)
			if err != nil {
				log.Println("Dialing error:", err)
				rf.Mu.Lock()
				// 安全地移除失效的节点
				for i := 0; i < len(rf.Peers); i++ {
					if rf.Peers[i] == peer {
						rf.Peers = append(rf.Peers[:i], rf.Peers[i+1:]...)
						break
					}
				}
				rf.Mu.Unlock()
				return
			}
			var reply HeartBeatReply
			err = client.Call("Raft.ReceiveHeartBeat", args, &reply)
			if err != nil {
				log.Println("RPC error:", err)
				return
			}

			rf.Mu.Lock()
			defer rf.Mu.Unlock()

			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.CurrentRole = Follower
				rf.VotedFor = "null"
				rf.CurrentLeader = "null"
			}
		}(peer, i)
	}
}
