package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net/rpc"
	"os"
)

type LogEntry struct {
	Term    int
	Index   int
	Command []byte
}

type LogRequestArgs struct {
	LeaderID     string     // Leader 的 ID
	CommitLength int        // Leader 已经提交的日志
	Term         int        // Leader 当前 Term 号
	LogLength    int        // 日志长度
	LogTerm      int        // 日志复制点的 Term
	Entries      []LogEntry // 日志列表
	IsFullSync   bool       // 是否为全量复制
}

type LogReplyArgs struct {
	NodeID      string // Follower 的 ID
	CurrentTerm int    // Follower 当前的 Term
	Ack         int    // 接收复制后的日志长度
	Flag        bool   // 是否接收复制
}

// Boradcast 广播日志
func (rf *Raft) Boradcast(newLog LogEntry) error {
	rf.Mu.Lock()
	// 如果当前节点是 Leader，则添加日志
	if rf.CurrentRole == Leader {
		// 设置日志的任期和索引
		newLog.Term = rf.CurrentTerm
		newLog.Index = len(rf.Logs) + 1
		rf.Logs = append(rf.Logs, newLog)

		// 创建一个通道用于接收错误
		errChan := make(chan error, 1)

		// 持久化日志到本地
		go func() {
			err := rf.SaveLogs()
			errChan <- err
		}()

		// 添加日志
		rf.Mu.Unlock()

		// 异步地将日志复制给其他所有节点
		for i := 0; i < len(rf.Peers); i++ {
			peer := rf.Peers[i]
			if peer == rf.RaftAddr {
				continue
			}
			go rf.Replicating(peer)
		}

		// 等待 goroutine 完成并检查错误
		if err := <-errChan; err != nil {
			return err
		}

	} else {
		rf.Mu.Unlock()
	}

	return nil
}

// AppendEntries 接收日志(丢弃之前的无效日志，并复制当前 Leader 的日志条目)
func (rf *Raft) AppendEntries(logLength int, leaderCommit int, entries *[]LogEntry) {
	// 检查是否需要截断现有日志条目
	if len(*entries) > 0 && len(rf.Logs) > logLength {
		if rf.Logs[logLength].Term != (*entries)[0].Term {
			rf.Logs = rf.Logs[:logLength-1]
		}
	}
	// 将新的日志条目追加到现有日志中
	if logLength+len(*entries) > len(rf.Logs) {
		startIndex := len(rf.Logs) - logLength
		rf.Logs = append(rf.Logs, (*entries)[startIndex:]...)

		// 持久化日志到本地
		go rf.SaveLogs()
	}

	// 更新提交日志的长度
	if leaderCommit >= rf.CommitLength {
		// 对新提交的日志条目应用到状态机
		if rf.fsm != nil {
			for i := rf.CommitLength; i <= leaderCommit; i++ {
				if i < len(rf.Logs) {
					err := rf.fsm.Apply(&rf.Logs[i])
					if err != nil {
						log.Println("FSM Apply Error:", err)
					}
				}
			}
		} else {
			log.Printf("Follower %s: FSM未设置，跳过应用日志到状态机\n", rf.NodeID)
		}
		for i := rf.CommitLength; i < leaderCommit; i++ {
			log.Println("Follower Commit Log ", i)
		}
		rf.CommitLength = leaderCommit
	}
}

// CommitEntries 提交日志
func (rf *Raft) CommitEntries() {
	minAcks := len(rf.Peers)/2 + 1
	ready := []int{}

	for i := 1; i <= len(rf.Logs); i++ {
		if rf.Acks(i) >= minAcks {
			ready = append(ready, i)
		}
	}
	maxReady := Max(ready)
	if len(ready) > 0 && maxReady > rf.CommitLength && rf.Logs[maxReady-1].Term == rf.CurrentTerm {
		for i := rf.CommitLength; i < maxReady; i++ {
			fmt.Println("Leader Commit Log ", i)
		}
		rf.CommitLength = maxReady
		fmt.Println(rf.RaftAddr, rf.CommitLength)

		// 持久化日志到本地
		go rf.SaveLogs()
	}
}

// Replicating 进行增量日志复制
func (rf *Raft) Replicating(peer string) error {
	rf.Mu.Lock()

	// 获取当前节点的日志索引
	i := rf.SentLength[peer]
	// 获取当前日志的末尾索引
	ei := len(rf.Logs) - 1
	prevLogTerm := 0

	// 如果节点没有任何日志记录，或者差距太大，使用全量复制
	if i == 0 || (ei-i > 100) { // 差距超过100条日志时使用全量复制
		rf.Mu.Unlock()
		return rf.FullSyncReplicating(peer)
	}

	// 选择需要复制的日志条目
	var entries []LogEntry
	if ei >= i {
		entries = rf.Logs[i : ei+1]
	} else {
		entries = []LogEntry{}
	}

	// 如果不是第一条，则获取前一条日志的任期
	if i > 0 {
		prevLogTerm = rf.Logs[i-1].Term
	}

	// 准备RPC请求参数
	args := &LogRequestArgs{
		LeaderID:     rf.RaftAddr,
		CommitLength: rf.CommitLength,
		Term:         rf.CurrentTerm,
		LogLength:    i,
		LogTerm:      prevLogTerm,
		Entries:      entries,
		IsFullSync:   false, // 标记为增量复制
	}
	rf.Mu.Unlock()

	// 和指定节点建立rpc连接
	client, err := rpc.DialHTTP("tcp", peer)
	if err != nil {
		log.Println("Dialing error: ", err)
		return nil
	}

	// 发送RPC请求
	var reply LogReplyArgs
	err = client.Call("Raft.Replying", args, &reply)
	if err != nil {
		log.Println("RPC error: ", err)
		return nil
	}

	return nil
}

// Replying Follower 回应日志复制
func (rf *Raft) Replying(args *LogRequestArgs, reply *LogReplyArgs) error {
	// 锁定 Raft 实例以防止并发修改
	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	// 如果请求中的任期比当前节点的任期更高，更新任期并将当前角色设为 Follower
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = "null"
		rf.CurrentRole = Follower
		rf.CurrentLeader = args.LeaderID
	}

	// 如果任期相同且当前节点角色为 Candidate，更新角色为 Follower
	if args.Term == rf.CurrentTerm && rf.CurrentRole == Candidate {
		rf.CurrentRole = Follower
		rf.CurrentLeader = args.LeaderID
	}

	// 准备响应
	reply.NodeID = rf.RaftAddr
	reply.CurrentTerm = rf.CurrentTerm
	reply.Ack = 0
	reply.Flag = false

	// 处理全量复制请求
	if args.IsFullSync {
		// 如果是全量复制且任期正确，直接替换本地日志
		if args.Term == rf.CurrentTerm {

			// 替换为新日志
			rf.Logs = args.Entries

			// 更新提交长度
			if args.CommitLength > rf.CommitLength {
				for i := rf.CommitLength; i < args.CommitLength; i++ {
					err := rf.fsm.Apply(&rf.Logs[i])
					if err != nil {
						log.Println("FSM Apply Error:", err)
					}
				}
				rf.CommitLength = args.CommitLength
			}

			// 持久化日志到本地
			go func() {
				err := rf.SaveLogs()
				if err != nil {
					log.Println("Error saving logs:", err)
				}
			}()

			// 设置响应
			reply.Ack = len(rf.Logs)
			reply.Flag = true

			log.Printf("节点 %s: 完成全量日志同步，共 %d 条日志\n", rf.NodeID, len(rf.Logs))
		}
	} else {

		// 增量复制的处理逻辑
		logOk := (len(rf.Logs) >= args.LogLength) && (args.LogLength == 0 || args.LogTerm == rf.Logs[args.LogLength-1].Term)

		if args.Term == rf.CurrentTerm && logOk {
			rf.AppendEntries(args.LogLength, args.CommitLength, &args.Entries)
			ack := args.LogLength + len(args.Entries)
			reply.Ack = ack
			reply.Flag = true
		}
	}

	// 远程调用leader的ReceivingAck进行回应处理
	client, err := rpc.DialHTTP("tcp", args.LeaderID)
	if err != nil {
		log.Println("Dialing error: ", err)
		return nil
	}
	flag := false // 占位
	err = client.Call("Raft.ReceivingAck", reply, &flag)
	if err != nil {
		log.Println("RPC error: ", err)
		return nil
	}
	return nil
}

// ReceivingAck 接收节点的回应
func (rf *Raft) ReceivingAck(reply *LogReplyArgs, flag *bool) error {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	// 检查响应的任期是否与当前节点的任期匹配，并且节点角色是 Leader
	if reply.CurrentTerm == rf.CurrentTerm && rf.CurrentRole == Leader {
		// 如果响应有效且确认号大于或等于当前确认长度，则更新发送和确认长度
		if reply.Flag && reply.Ack >= rf.AckedLength[reply.NodeID] {
			rf.SentLength[reply.NodeID] = reply.Ack
			rf.AckedLength[reply.NodeID] = reply.Ack
			log.Println("start to commit log")
			go rf.CommitEntries() // 提交日志的操作可以在这里被调用
		} else if rf.SentLength[reply.NodeID] > 0 {
			// 如果响应无效或确认号不足，减少发送长度并重新尝试复制日志
			rf.SentLength[reply.NodeID] = rf.SentLength[reply.NodeID] - 1
			go rf.Replicating(reply.NodeID)
		}
	} else if reply.CurrentTerm > rf.CurrentTerm {
		// 如果响应中的任期大于当前任期，更新节点状态为 Follower
		rf.CurrentTerm = reply.CurrentTerm
		rf.CurrentRole = Follower
		rf.VotedFor = "null"
	}
	return nil
}

// Acks Raft 9
func (rf *Raft) Acks(lengths int) int {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	ret := 0
	for _, peer := range rf.Peers {
		if (peer == rf.RaftAddr && len(rf.Logs) >= lengths) || rf.AckedLength[peer] >= lengths {
			ret += 1
		}
	}
	return ret
}

// SaveLogs 将日志持久化到本地文件
func (rf *Raft) SaveLogs() error {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	// 创建日志目录（如果不存在）
	logDir := "./raft_logs/" + rf.NodeID
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		err = os.MkdirAll(logDir, 0755)
		if err != nil {
			return fmt.Errorf("创建日志目录失败: %v", err)
		}
	}

	// 设置日志文件路径
	if rf.LogFile == "" {
		rf.LogFile = fmt.Sprintf("%s/%s", logDir, "raft-log.bolt")
	}

	// 使用gob进行编码实现日志压缩
	file, err := os.Create(rf.LogFile)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(rf.Logs)
	if err != nil {
		return fmt.Errorf("gob编码失败: %v", err)
	}

	//log.Printf("节点 %s: 成功将 %d 条日志持久化到 %s\n", rf.NodeID, len(rf.Logs), rf.LogFile)
	return nil
}

// LoadLogs 从本地文件加载日志
func (rf *Raft) LoadLogs() error {
	// 创建日志目录（如果不存在）
	logDir := "./raft_logs/" + rf.NodeID
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		err = os.MkdirAll(logDir, 0755)
		if err != nil {
			return fmt.Errorf("创建日志目录失败: %v", err)
		}
		return nil // 目录不存在，说明是首次运行，没有日志可加载
	}

	// 设置日志文件路径
	if rf.LogFile == "" {
		rf.LogFile = fmt.Sprintf("%s/%s", logDir, "raft-log.bolt")
	}

	// 检查日志文件是否存在
	if _, err := os.Stat(rf.LogFile); os.IsNotExist(err) {
		return nil // 文件不存在，说明是首次运行，没有日志可加载
	}

	// 读取文件内容
	data, err := os.ReadFile(rf.LogFile)
	if err != nil {
		return fmt.Errorf("读取日志文件失败: %v", err)
	}

	// 如果文件为空，则返回
	if len(data) == 0 {
		return nil
	}

	// 使用job解析Bolt数据
	var logs []LogEntry

	reader := bytes.NewReader(data)
	decoder := gob.NewDecoder(reader)
	err = decoder.Decode(&logs)
	if err != nil {
		return fmt.Errorf("gob 解码失败: %v", err)
	}

	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	// 更新日志
	rf.Logs = logs

	// 更新提交长度（假设所有加载的日志都已提交）
	if len(logs) > 0 {
		rf.CommitLength = len(logs)
	}

	log.Printf("%s 成功从 %s 加载 %d 条日志\n", rf.NodeID, rf.LogFile, len(rf.Logs))
	return nil
}

// FullSyncReplicating 进行全量日志复制
func (rf *Raft) FullSyncReplicating(peer string) error {
	rf.Mu.Lock()

	// 全量复制时，发送所有日志
	var entries []LogEntry
	if len(rf.Logs) > 0 {
		entries = rf.Logs[:]
	} else {
		entries = []LogEntry{}
	}

	// 准备RPC请求参数
	args := &LogRequestArgs{
		LeaderID:     rf.RaftAddr,
		CommitLength: rf.CommitLength,
		Term:         rf.CurrentTerm,
		LogLength:    0, // 从头开始
		LogTerm:      0, // 没有前一条日志
		Entries:      entries,
		IsFullSync:   true, // 标记为全量复制
	}
	rf.Mu.Unlock()

	// 和指定节点建立rpc连接
	client, err := rpc.DialHTTP("tcp", peer)
	if err != nil {
		log.Println("Dialing error: ", err)
		return nil
	}

	// 发送RPC请求
	var reply LogReplyArgs
	err = client.Call("Raft.Replying", args, &reply)
	if err != nil {
		log.Println("RPC error: ", err)
		return nil
	}

	return nil
}

// Max 函数
func Max(arr []int) int {
	if len(arr) == 0 {
		return 0
	}
	maxVal := arr[0]
	for _, val := range arr {
		if val > maxVal {
			maxVal = val
		}
	}
	return maxVal
}
