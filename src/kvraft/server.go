package kvraft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/common"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/logging"
	"6.824/raft"
	"github.com/sirupsen/logrus"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// GET/PUTAPPEND
	OpType string
	OpKey  string

	// optional
	OPValue string

	common.RequestInfo
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	logger  *logrus.Entry

	maxraftstate int // snapshot if log grows this big
	inSnapshot   int32

	// Your definitions here.
	dataStore KVInterface
	notify    map[int]chan OPResult
	// applychListener *ApplychListener
}

func (kv *KVServer) applyEntry(index int) OPResult {
	kv.mu.Lock()
	ch, ok := kv.notify[index]
	if !ok {
		ch = make(chan OPResult, 1)
		kv.notify[index] = ch
	}
	kv.mu.Unlock()

	select {
	case result := <-ch:
		return result
	case <-time.After(common.ApplyCHTimeout):
		return OPResult{err: ErrTimeout}
	}
}

func (kv *KVServer) applyMsgHandler() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			shouldSnapshot := kv.shouldIssueSnapshot()
			opResult, dumpData := kv.dataStore.EvalOp(op, shouldSnapshot)
			if shouldSnapshot {
				go func(index int, data []byte) {
					atomic.StoreInt32(&kv.inSnapshot, 1)
					kv.rf.Snapshot(index, data)
					atomic.StoreInt32(&kv.inSnapshot, 0)
				}(msg.CommandIndex, dumpData)
			}
			kv.mu.Lock()
			ch, ok := kv.notify[msg.CommandIndex]
			if ok {
				select {
				case <-ch: // drain bad data
				default:
				}
			} else {
				ch = make(chan OPResult, 1)
				kv.notify[msg.CommandIndex] = ch
			}
			kv.mu.Unlock()
			ch <- opResult
		} else if msg.SnapshotValid {
			kv.logger.Warn("apply: applying snapshot")
			err := kv.dataStore.Load(msg.Snapshot)
			if err != nil {
				kv.logger.Panic("apply: load snapshot error", err)
			}
		} else {
			kv.logger.Panic("apply: invalid command", msg)
		}
	}
}

// propose command to raft
func (kv *KVServer) proposeAndApply(op Op, replier ReplyInterface) string {

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		replier.SetReplyErr(ErrWrongLeader)
		kv.logger.WithField("op", op).Debug("early return: not leader")
		return ""
	}
	kv.logger.WithField("index", index).Debug("propose: start is ok")
	doneChan := make(chan OPResult, 1)
	lostLeaderChan := make(chan struct{}, 1)
	var opEndResult OPResult
	go func() {
		// opResult := kv.applychListener.waitForApplyOnIndex(index)
		opResult := kv.applyEntry(index)
		if !opResult.requestInfo.Equals(&op.RequestInfo) {
			lostLeaderChan <- struct{}{}
			kv.logger.WithField("index", index).Warn("propose: different content on index")
		} else {

			// 这里需要额外检查op确实是提交的op
			doneChan <- opResult
			term, state := kv.rf.GetState()
			kv.logger.WithFields(logrus.Fields{
				"opResult": opResult,
				"op":       op,
				"term":     term,
				"state":    state,
				"index":    index,
			}).Debug("got opresult")
		}
	}()
	// var run int32 = 1
	// go func() {
	// 	for atomic.LoadInt32(&run) == 1 {
	// 		_, isLeader := kv.rf.GetState()
	// 		if !isLeader {
	// 			kv.logger.Warn("kv: lost leadership")
	// 			lostLeaderChan <- struct{}{}
	// 			break
	// 		}
	// 		time.Sleep(10 * time.Millisecond)
	// 	}
	// }()
	select {
	case opEndResult = <-doneChan:
		//atomic.StoreInt32(&run, 0)
		break
	case <-lostLeaderChan:
		opEndResult.err = ErrWrongLeader
	}
	replier.SetReplyErr(opEndResult.err)
	kv.logger.WithField("index", index).Debug("propose: apply is done")
	return opEndResult.data
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

func (kv *KVServer) shouldIssueSnapshot() bool {
	if kv.maxraftstate == -1 || atomic.LoadInt32(&kv.inSnapshot) == 1 {
		return false
	}
	rfSize := kv.rf.GetStateSize()
	if kv.maxraftstate-rfSize < common.StateSizeDiff {

		kv.logger.WithFields(logrus.Fields{
			"raftStateSize": rfSize,
			"maxraftState":  kv.maxraftstate,
		}).Debug("kv: should issue snapshot")
		return true
	}
	return false
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
	//kv.applychListener = NewApplyChListener(kv.applyCh, kv.logger, kv.dataStore, kv.rf)

	kv.notify = make(map[int]chan OPResult)
	go kv.applyMsgHandler()
	return kv
}
