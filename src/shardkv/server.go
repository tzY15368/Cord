package shardkv

import (
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
	mu                 sync.RWMutex
	me                 int
	rf                 *raft.Raft
	applyCh            chan raft.ApplyMsg
	make_end           func(string) *labrpc.ClientEnd
	gid                int
	ctrlers            []*labrpc.ClientEnd
	maxraftstate       int // snapshot if log grows this big
	ctlClerk           *shardctrler.Clerk
	config             shardctrler.Config
	logger             *logrus.Entry
	notify             map[int]chan opResult
	ack                map[int64]int64
	data               map[string]string
	clientID           int64
	requestID          int64
	msgCount           int64
	shardCFGVersion    []int32
	maxCFGVersion      int32
	maxTransferVersion int32
	cfgVerAlignedCond  *sync.Cond
	// map[版本号]数据
	outboundData map[int]map[string]string
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
func (kv *ShardKV) dumpShardVersion() []int32 {
	// res := make([]int, len(kv.shardCFGVersion))
	// for i := 0; i < len(kv.shardCFGVersion); i++ {
	// 	res[i] = int(atomic.LoadInt32(&kv.shardCFGVersion[i]))
	// }
	// return res
	return kv.shardCFGVersion
}

// 变成拉数据，这里要填充kvstore数据到reply.data
func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.shardVersionIsNew(args.Shard) || int32(args.ConfigNum) > atomic.LoadInt32(&kv.maxCFGVersion) {
		reply.Err = ErrReConfigure
		kv.logger.WithFields(logrus.Fields{
			"configNum": args.ConfigNum,
			"shard":     args.Shard,
			"version":   kv.dumpShardVersion(),
		}).Debug("skv: migrate: version is not new, retry later")
		return
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		reply.Data = make(map[string]string)
		// for key := range kv.data {
		// 	if key2shard(key) == args.Shard {
		// 		reply.Data[key] = kv.data[key]
		// 	}
		// }
		out, ok := kv.outboundData[args.ConfigNum]
		if ok {
			for key := range out {
				if key2shard(key) == args.Shard {
					reply.Data[key] = out[key]
				}
			}
		}
		kv.logger.WithFields(logrus.Fields{
			"args.shard": args.Shard,
			"num":        args.ConfigNum,
			"replyData":  reply.Data,
		}).Debug("skv: migrate: sending reply")
		reply.Err = OK
	}
	// 因为之前每个实例收到config广播的逻辑时间是一致的，打出的outbounddata
	// 快照也应该是一致的，这里不需要额外的op

	kv.logger.WithField("reply", fmt.Sprintf("%+v", reply.Err)).WithField("len", len(reply.Data)).
		Debug("skv: migrate: result")
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
	kv.rf.SetGID(gid)
	kv.logger = logging.GetLogger("skv", common.ShardKVLogLevel).WithField("id", fmt.Sprintf("%d-%d", gid, me))
	kv.ctlClerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.notify = make(map[int]chan opResult)
	kv.ack = make(map[int64]int64)
	kv.data = make(map[string]string)
	kv.clientID = nrand()
	kv.requestID = 0
	kv.shardCFGVersion = make([]int32, shardctrler.NShards)
	kv.cfgVerAlignedCond = sync.NewCond(&kv.mu)
	kv.outboundData = make(map[int]map[string]string)
	go kv.pollCFG()
	go kv.applyMsgHandler()
	return kv
}
