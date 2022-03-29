package shardkv

import (
	"sync/atomic"
	"time"

	"6.824/common"
	"6.824/shardctrler"
	"github.com/sirupsen/logrus"
)

// shardVersionIsNew not thread safe
func (kv *ShardKV) shardVersionIsNew(shard int) bool {
	//return atomic.LoadInt32(&kv.shardCFGVersion[shard]) == atomic.LoadInt32(&kv.maxCFGVersion)
	return kv.shardCFGVersion[shard] == kv.maxCFGVersion
}

// shouldServeKey thread safe
func (kv *ShardKV) shouldServeKey(key string) Err {
	kv.logger.WithFields(logrus.Fields{
		"version": kv.dumpShardVersion(),
		"key":     key,
	}).Debug("svCFG: shouldServeKey")
	shard := key2shard(key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Shards[shard] != kv.gid {
		return ErrWrongGroup
	}
	if !kv.shardVersionIsNew(shard) {
		return ErrReConfigure
	}
	return OK
}

func (kv *ShardKV) evalCFGOP(op *Op) opResult {

	cfg := shardctrler.LoadCFG(op.OP_VALUE)
	kv.mu.Lock()
	oldConfig := kv.config
	if cfg.Num <= int(atomic.LoadInt32(&kv.maxCFGVersion)) {
		kv.logger.Warn("ignoring old nums, possible restart", cfg.Num, oldConfig.Num)
		kv.mu.Unlock()
		return opResult{
			err: ErrSeenConfig,
		}
	} else {
		swapped := atomic.CompareAndSwapInt32(&kv.maxCFGVersion, int32(cfg.Num-1), int32(cfg.Num))
		if !swapped {
			panic("no swap")
		}
	}
	kv.config = cfg
	diff := cfg.DiffOld(&oldConfig)
	tome := diff.ToMe(kv.gid)

	// diff完了以后加锁?
	// 不tome的shard可以直接bump版本号
	for i := 0; i < shardctrler.NShards; i++ {
		if _, ok := tome[i]; !ok {
			// swapped := atomic.CompareAndSwapInt32(&kv.shardCFGVersion[i], int32(cfg.Num-1), int32(cfg.Num))
			// if !swapped {
			// 	kv.logger.Panic("no swap", cfg.Num, atomic.LoadInt32(&kv.shardCFGVersion[i]))
			// }
			if kv.shardCFGVersion[i] != int32(cfg.Num)-1 {
				kv.logger.Panic("no swap", kv.shardCFGVersion[i], int32(cfg.Num))
			} else {
				kv.shardCFGVersion[i] = int32(cfg.Num)
			}
		}
	}
	kv.mu.Unlock()

	kv.logger.WithField("version", kv.dumpShardVersion()).Debug("svCFG: evalCFG: updated version")
	// 加完锁不能只leader发op-transfer，
	// 因为如果在等对方返回的时候失去了leader数据就丢了，且无法恢复
	// 只能一个group里所有人都给目标机器发
	go kv.handleTransfer(tome, oldConfig, cfg)
	return opResult{
		err:         OK,
		RequestInfo: op.RequestInfo,
	}
}

func (kv *ShardKV) pollCFG() {
	for {
		kv.mu.Lock()
		oldCfg := kv.config
		kv.mu.Unlock()
		cfg := kv.ctlClerk.Query(oldCfg.Num + 1)
		if cfg.Num != oldCfg.Num {
			kv.logger.WithField("old", oldCfg.Num).
				WithField("new", cfg.Num).Debug("skv: poll: found new cfg")
			//go kv.reconfig(oldCfg, cfg)
			kv.handleNewConfig(oldCfg, cfg)
			//kv.config = cfg
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) handleNewConfig(oldCFG shardctrler.Config, newCFG shardctrler.Config) {
	// delta 推到evalCFGOP里计算
	kv.logger.WithField("old", oldCFG.Num).WithField("new", newCFG.Num).Debug("skv: handling new config")
	op := Op{
		OP_TYPE:  OP_NEWCONFIG,
		OP_VALUE: newCFG.Dump(),
		RequestInfo: common.RequestInfo{
			ClientID:  kv.clientID,
			RequestID: atomic.AddInt64(&kv.requestID, 1),
		},
	}
	reply := internalReply{}
	kv.proposeAndApply(op, &reply)
	if reply.Err == OK {
		kv.logger.WithFields(logrus.Fields{
			"newNum": newCFG.Num,
			"reply":  reply,
		}).Debug("svCFG: handleNewConfig: proposed")
	} else if reply.Err == ErrSeenConfig {
		panic("invalid seq conf")
	}
}
