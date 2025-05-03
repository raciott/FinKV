package raft

/**
状态机接口-需要自己实现相关逻辑
*/

// FSM is implemented by clients to make use of the replicated log.
type FSM interface {
	Apply(*LogEntry) interface{}
}
