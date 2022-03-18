package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"6.824/common"
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

	RequestInfo
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

	applychListener *ApplychListener
}

// propose command to raft
func (kv *KVServer) proposeAndApply(op Op, replier ReplyInterface) string {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		replier.SetReplyErr(ErrWrongLeader)
		return ""
	}
	kv.logger.WithField("index", index).Debug("propose: start is ok")
	opResult := kv.applychListener.waitForApplyOnIndex(index)
	replier.SetReplyErr(opResult.err)
	kv.logger.WithField("index", index).Debug("propose: apply is done")
	return opResult.data
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{
		OpType:      OP_GET,
		OpKey:       args.Key,
		RequestInfo: args.RequestInfo,
	}

	result := kv.proposeAndApply(op, reply)
	reply.Value = result

	kv.logger.WithField("reply", fmt.Sprintf("%+v", reply)).
		Debug(fmt.Sprintf("kv: %s: result", op.OpType))
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		OpType:      args.Op,
		OpKey:       args.Key,
		OPValue:     args.Value,
		RequestInfo: args.RequestInfo,
	}
	kv.proposeAndApply(op, reply)
	kv.logger.WithField("reply", fmt.Sprintf("%+v", reply)).
		Debug(fmt.Sprintf("kv: %s: result", op.OpType))
	reply.RV = 123
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

// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	_logger := logging.GetLogger("kv", common.KVServerLogLevel)
	kv.logger = _logger.WithField("id", kv.me)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.dataStore = NewKVStore(logging.GetLogger("kvstore", common.KVStoreLogLevel).WithField("id", me))
	kv.applychListener = NewApplyChListener(kv.applyCh, kv.logger, kv.dataStore, kv.rf)
	return kv
}
