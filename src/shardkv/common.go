package shardkv

import "6.824/common"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Op struct {
	OP_TYPE  int
	OP_KEY   string
	OP_VALUE string
}

type OPResult struct {
	data string
	err  Err
	common.RequestInfo
}

const (
	OP_GET = iota
	OP_PUT
	OP_APPEND
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"

	common.RequestInfo
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string

	common.RequestInfo
}

type GetReply struct {
	Err   Err
	Value string
}
