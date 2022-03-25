package shardkv

import (
	"fmt"
	"sync/atomic"
	"time"

	"6.824/common"
	"6.824/shardctrler"
)

// isKeyServed thread safe
func (kv *ShardKV) isKeyServed(key string) bool {
	kv.mu.Lock()
	shard := key2shard(key)
	kv.shardLocks[shard].Lock()
	ans := kv.config.Shards[shard] == kv.gid
	kv.shardLocks[shard].Unlock()
	return ans
}

func (kv *ShardKV) evalCFGOp(op *Op) opResult {
	_, cfgOp := loadReconfigString(op.OP_KEY)
	delta := shardctrler.NewDiffCfg(op.OP_VALUE)
	var lockedShards []int
	lockedShards = append(lockedShards, delta.FromMe(kv.gid)...)
	lockedShards = append(lockedShards, delta.ToMe(kv.gid)...)
	kv.logger.WithField("lockedKeys", lockedShards)
	for v := range lockedShards {
		if cfgOp == shardctrler.CFG_LOCK {
			kv.shardLocks[v].Lock()
		} else {
			kv.shardLocks[v].Unlock()
		}
	}
	return opResult{
		err:         OK,
		RequestInfo: op.RequestInfo,
	}
}

func (kv *ShardKV) pollCFG() {
	for {
		cfg := kv.ctlClerk.Query(-1)
		kv.mu.Lock()
		oldCfg := kv.config
		if cfg.Num != oldCfg.Num {
			kv.logger.WithField("old", oldCfg.Num).
				WithField("new", cfg.Num).Debug("skv: poll: found new cfg")
			go kv.reconfig(oldCfg, cfg)
			kv.config = cfg
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func genReconfigString(cfg *shardctrler.Config, op int) string {
	return fmt.Sprintf("__cfg_%d_%d", op, cfg.Num)
}

// returns newConfig.Num, op (CFG_LOCK/CFG_UNLOCK)
func loadReconfigString(s string) (int, int) {
	var num int
	var op int
	n, err := fmt.Sscanf(s, "__cfg_%d_%d", &op, &num)
	if n != 2 || err != nil {
		panic(err)
	}
	return num, op
}

func (kv *ShardKV) reconfig(old shardctrler.Config, _new shardctrler.Config) {

	// 确保目标group也已经了解这个reshard操作
	/* 每个kvserver都应该执行一样的操作
	所有kvserver都poll
	 发现reconfig后隐式进入start状态，不是leader的自然无法发起proposal，主动退出，

	leader广播op-cfg后主动挂起delta key的get/putappend，
		算delta后加锁的key需要记下来，后面的请求一律挂起
		算delta进行打包transfer op-trans（call对方leader的），
		op-trans需要在对方组内广播，广播完optrans才可返回
		op-trans相当于自带确认，返回即意味着对方group已经准备解锁相关key


		-- 对key的解锁全部是接收方解锁，即接收方在回复完optrans后解锁，发送方在发完optrans后解锁

		op-trans返回后原group的需要再发一个op-cfg通知本组成员删除相关key，且更改记下来的key？
	本质上op-trans在发起方是2PL，加锁的是delta中增加的和减少的key

	op-trans需要同时等拿完所有期待的shard，并发完所有不要的shard之后才算完成（返回），
	然后才可进行unlock
	删除是在unlock时做的

	对方leader在发现config更改后广播op-cfg，主动挂起相应delta key的get/putappend，
	收包op-trans应用后再发op-ack确认，恢复服务


	*/
	time.Sleep(pollCFGInterval / 2 * 3)
	delta := _new.DiffOld(&old)
	kv.logger.WithField("delta", fmt.Sprintf("%+v", delta)).Debug("skv: reconfig: got delta")
	// 算diff，广播给本集群
	op := Op{
		OP_TYPE:  OP_CFG,
		OP_KEY:   genReconfigString(&_new, shardctrler.CFG_LOCK),
		OP_VALUE: delta.Dump(),
		RequestInfo: common.RequestInfo{
			ClientID:  kv.clientID,
			RequestID: atomic.AddInt64(&kv.requestID, 1),
		},
	}
	reply := CFGReply{}
	kv.proposeAndApply(op, &reply)
	if reply.Err != OK {
		return
	}

}
