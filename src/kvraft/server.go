package kvraft

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/logging"
	"6.824/raft"
	"github.com/sirupsen/logrus"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// GET/PUTAPPEND
	OpType string
	OpKey  string

	// optional
	OPValue string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	logger  *logrus.Entry

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dataStore KVInterface

	applyHandler *ApplyHandler
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{
		OpType: "GET",
		OpKey:  args.Key,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// 等applyChan实际拿到majority, may block indefinitely?
	// 需要它内置超时
	ok := kv.applyHandler.waitForMajorityOnIndex(index)
	if !ok {
		reply.Err = ErrTimeout
		return
	}

	// 实际向本地kvstore查数据
	result, err := kv.dataStore.Get(op.OpKey)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			reply.Err = ErrNoKey
		} else {
			kv.logger.WithError(err).Error("kv: get: unexpected error")
		}
	}
	reply.Value = result
	kv.logger.WithField("reply", fmt.Sprintf("%+v", reply)).Debug("kv: get: result")
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		OpType:  args.Op,
		OpKey:   args.Key,
		OPValue: args.Value,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// 见Get
	kv.applyHandler.waitForMajorityOnIndex(index)

	// 实际向本地kvstore查数据
	var err error
	switch op.OpType {
	case "PUT":
		err = kv.dataStore.Put(op.OpKey, op.OPValue)
	case "APPEND":
		err = kv.dataStore.Append(op.OpKey, op.OPValue)
	}

	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			reply.Err = ErrNoKey
		} else {
			kv.logger.WithError(err).Error("kv: putappend: unexpected error")
		}
	}
	kv.logger.Debug("kv: putappend: ok")
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	_logger := logging.GetLogger("kv", logrus.DebugLevel)
	kv.logger = _logger.WithField("id", kv.me)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.dataStore = NewKVStore()
	kv.applyHandler = NewApplyHandler(kv.applyCh, len(servers), _logger, me)
	_logger.Panic("fail")
	return kv
}
