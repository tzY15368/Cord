package shardkv

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/common"
	"6.824/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrReConfigure  = "ErrReconfigure"
	ErrKeyNoLock    = "ErrKeyNoLock"
	ErrSeenConfig   = "ErrSeenConfig"
	ErrSeenTransfer = "ErrSeenTrans"
)

const (
	pollCFGInterval = 75 * time.Millisecond
)

type Op struct {
	OP_TYPE  int
	OP_KEY   string
	OP_VALUE string
	common.RequestInfo
}

// type OPCFGData struct {
// 	Config
// }

type opResult struct {
	data string
	err  Err
	common.RequestInfo
}

type replyable interface {
	SetValue(string)
	SetErr(Err)
}

const (
	OP_GET = iota
	OP_PUT
	OP_APPEND
	// newconfig 用于每一个实例，当发现新config时发opconfig广播，尝试开启transfer
	OP_NEWCONFIG
	// transfer 用于主动拉完数据后在本集群内广播，通知新数据
	OP_TRANSFER
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

func (par *PutAppendReply) SetValue(i string) {}
func (par *PutAppendReply) SetErr(e Err)      { par.Err = e }

type GetArgs struct {
	Key string

	common.RequestInfo
}

type GetReply struct {
	Err   Err
	Value string
}

func (gar *GetReply) SetValue(i string) { gar.Value = i }
func (gar *GetReply) SetErr(e Err)      { gar.Err = e }

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type CFGReply struct {
	Err Err
}

func (cgr *CFGReply) SetValue(i string) {}
func (cfg *CFGReply) SetErr(e Err)      { cfg.Err = e }

type MigrateArgs struct {
	Shard     int
	ConfigNum int
	common.RequestInfo
}

type MigrateReply struct {
	Err  Err
	Data map[string]string
}

func (mgr *MigrateReply) SetValue(i string) {}
func (mgr *MigrateReply) SetErr(e Err)      { mgr.Err = e }

type internalReply struct {
	Err Err
}

func (itr *internalReply) SetValue(i string) {}
func (itr *internalReply) SetErr(e Err)      { itr.Err = e }
