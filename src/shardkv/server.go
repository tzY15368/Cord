package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"6.824/common"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/logging"
	"6.824/raft"
	"6.824/shardctrler"
	"github.com/sirupsen/logrus"
)

type ShardKV struct {
	mu            sync.RWMutex
	me            int
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	make_end      func(string) *labrpc.ClientEnd
	gid           int
	ctrlers       []*labrpc.ClientEnd
	maxraftstate  int // snapshot if log grows this big
	ctlClerk      *shardctrler.Clerk
	config        shardctrler.Config
	logger        *logrus.Entry
	notify        map[int]chan opResult
	ack           map[int64]int64
	data          map[string]string
	inSnapshot    int32
	clientID      int64
	requestID     int64
	shardLocks    []int32
	migrateNotify map[int]chan int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		OP_TYPE:     OP_GET,
		OP_KEY:      args.Key,
		RequestInfo: args.RequestInfo,
	}
	kv.proposeAndApply(op, reply)
	kv.logger.WithField("reply", fmt.Sprintf("%+v", reply)).
		Debug("skv: get: result")
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		OP_KEY:      args.Key,
		OP_VALUE:    args.Value,
		RequestInfo: args.RequestInfo,
	}
	switch {
	case args.Op == "Put":
		op.OP_TYPE = OP_PUT
	case args.Op == "Append":
		op.OP_TYPE = OP_APPEND
	default:
		kv.logger.Panic("invalid op", args.Op)
	}
	kv.proposeAndApply(op, reply)
	kv.logger.WithField("reply", fmt.Sprintf("%+v", reply)).
		Debug("skv: putappend: result")
}

// dumpShardLocks not atomic, not thread safe
func (kv *ShardKV) dumpShardLocks() []int {
	res := make([]int, len(kv.shardLocks))
	for i := 0; i < len(kv.shardLocks); i++ {
		res[i] = int(atomic.LoadInt32(&kv.shardLocks[i]))
	}
	return res
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	// check if locked first, if not, reply with ErrNoLock
	if atomic.LoadInt32(&kv.shardLocks[args.Shard]) == 0 {
		reply.Err = ErrKeyNoLock
		kv.logger.WithFields(logrus.Fields{
			"shard":      args.Shard,
			"shardlocks": kv.dumpShardLocks(),
		}).Debug("skv: migrate: errkeynolock")
		return
	}
	// kv.mu.Lock()
	// if args.ConfigNum > int(atomic.LoadInt32(&kv.nextMigrateIndex)) {
	// 	kv.logger.WithFields(logrus.Fields{
	// 		"inComingNum": args.ConfigNum,
	// 		"kv.cfg.num":  kv.config.Num,
	// 	}).Debug("skv: migrate: waiting for older migrate messages")
	// 	reply.Err = ErrKeyNoLock
	// 	kv.mu.Unlock()
	// 	return
	// }
	// kv.mu.Unlock()
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err := encoder.Encode(args.Data)
	if err != nil {
		panic(err)
	}
	op := Op{
		OP_TYPE:     OP_MIGRATE,
		OP_KEY:      fmt.Sprintf("%d", args.Shard),
		OP_VALUE:    buf.String(),
		RequestInfo: args.RequestInfo,
	}

	kv.proposeAndApply(op, reply)
	kv.logger.WithField("reply", fmt.Sprintf("%+v", reply)).
		Debug("skv: migrate: result")
	kv.migrateNotify[args.ConfigNum] <- args.Shard
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.logger = logging.GetLogger("skv", common.ShardKVLogLevel).WithField("id-gid", fmt.Sprintf("%d-%d", me, gid))
	kv.ctlClerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.notify = make(map[int]chan opResult)
	kv.ack = make(map[int64]int64)
	kv.data = make(map[string]string)
	kv.clientID = nrand()
	kv.requestID = 0
	kv.shardLocks = make([]int32, shardctrler.NShards)
	kv.migrateNotify = make(map[int]chan int)
	go kv.pollCFG()
	go kv.applyMsgHandler()
	return kv
}
