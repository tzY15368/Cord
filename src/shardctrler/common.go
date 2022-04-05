package shardctrler

import (
	"6.824/common"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// A configuration -- an assignment of shards to groups.
// Please don't change this.

type opResult struct {
	err         Err
	requestInfo common.RequestInfo
	cfg         *Config
}
type Op struct {
	OP_TYPE int
	OP_DATA []byte
	common.RequestInfo
}

const (
	OP_JOIN = iota
	OP_LEAVE
	OP_MOVE
	OP_QUERY
)

const (
	OK             = "OK"
	ErrWrongLeader = Err("ErrWrongLeader")
	ErrTimeout     = Err("ErrTimeout")
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	common.RequestInfo
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	common.RequestInfo
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	common.RequestInfo
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	common.RequestInfo
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
