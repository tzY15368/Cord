package shardctrler

import (
	"bytes"
	"sync"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/logging"
	"6.824/raft"
	"github.com/sirupsen/logrus"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	logger *logrus.Entry

	inSnapshot int32

	notify  map[int]chan opResult
	ack     map[int64]int64
	configs []Config // indexed by config num
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	data := new(bytes.Buffer)
	encoder := labgob.NewEncoder(data)
	err := encoder.Encode(&args.Servers)
	if err != nil {
		panic(err)
	}
	op := Op{
		OP_TYPE:     OP_JOIN,
		OP_DATA:     data.Bytes(),
		RequestInfo: args.RequestInfo,
	}
	// ...apply
	_, _err := sc.proposeAndApply(op)
	if _err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	if _err != OK {
		reply.Err = _err
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	data := new(bytes.Buffer)
	encoder := labgob.NewEncoder(data)
	err := encoder.Encode(&args.GIDs)
	if err != nil {
		panic(err)
	}
	op := Op{
		OP_TYPE:     OP_LEAVE,
		OP_DATA:     data.Bytes(),
		RequestInfo: args.RequestInfo,
	}
	// ...apply
	_, _err := sc.proposeAndApply(op)
	if _err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	if _err != OK {
		reply.Err = _err
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	data := new(bytes.Buffer)
	encoder := labgob.NewEncoder(data)
	err := encoder.Encode(&args.GID)
	if err != nil {
		panic(err)
	}
	err = encoder.Encode(&args.Shard)
	if err != nil {
		panic(err)
	}
	op := Op{
		OP_TYPE:     OP_MOVE,
		OP_DATA:     data.Bytes(),
		RequestInfo: args.RequestInfo,
	}
	// ...apply
	_, _err := sc.proposeAndApply(op)
	if _err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	if _err != OK {
		reply.Err = _err
		return
	}

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	data := new(bytes.Buffer)
	encoder := labgob.NewEncoder(data)
	err := encoder.Encode(&args.Num)
	if err != nil {
		panic(err)
	}
	op := Op{
		OP_TYPE:     OP_QUERY,
		OP_DATA:     data.Bytes(),
		RequestInfo: args.RequestInfo,
	}
	res, _err := sc.proposeAndApply(op)

	if _err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	if _err != OK {
		reply.Err = _err
		return
	}
	reply.Config = *res
	sc.logger.WithField("reply", reply).Debug("sc: query: reply")
}

// appendConfig not thread safe
// returns the newly appended config for update
func (sc *ShardCtrler) appendConfig() *Config {
	newCfg := sc.configs[len(sc.configs)-1].clone()
	sc.configs = append(sc.configs, newCfg)
	return &newCfg
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.logger = logging.GetLogger("sc", logrus.DebugLevel).WithField("id", me)
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.notify = make(map[int]chan opResult)
	sc.ack = make(map[int64]int64)
	// Your code here.
	go sc.applyMsgHandler()
	return sc
}
