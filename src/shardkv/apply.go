package shardkv

import (
	"time"

	"6.824/common"
)

func (kv *ShardKV) proposeAndApply(op Op, replier replyable) {
	// 先检查是否是正确的group
	kv.mu.RLock()
	wrongSv := !kv.isKeyServed(op.OP_KEY)
	kv.mu.RUnlock()
	if wrongSv {
		replier.SetErr(ErrWrongGroup)
		return
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		replier.SetErr(ErrWrongLeader)
		return
	}
	kv.logger.WithField("index", index).Debug("propose: start is ok")
	doneChan := make(chan opResult, 1)
	lostLeaderChan := make(chan struct{}, 1)
	// var runCheckLeader int32 = 1
	go func() {
		opRes := kv.applyEntry(index)
		if !opRes.RequestInfo.Equals(&op.RequestInfo) {
			lostLeaderChan <- struct{}{}
			kv.logger.WithField("index", index).Warn("propose: different content on index")
		} else {
			doneChan <- opRes
		}
	}()
	select {
	case res := <-doneChan:
		replier.SetErr(res.err)
		replier.SetValue(res.data)
	case <-lostLeaderChan:
		replier.SetErr(ErrWrongLeader)
	}
}

func (kv *ShardKV) applyEntry(index int) opResult {
	kv.mu.Lock()
	ch, ok := kv.notify[index]
	if !ok {
		ch = make(chan opResult, 1)
		kv.notify[index] = ch
	}
	kv.mu.Unlock()

	select {
	case result := <-ch:
		return result
	case <-time.After(common.ApplyCHTimeout):
		return opResult{err: ErrWrongLeader}
	}
}

func (kv *ShardKV) applyMsgHandler() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			opRes := kv.evalOp(msg.CommandIndex, op)
			kv.mu.Lock()
			ch, ok := kv.notify[msg.CommandIndex]
			if ok {
				select {
				case <-ch: // drain bad data
				default:
				}
			} else {
				ch = make(chan opResult, 1)
				kv.notify[msg.CommandIndex] = ch
			}
			kv.mu.Unlock()
			ch <- opRes
		} else if msg.SnapshotValid {
			kv.logger.Warn("apply: applying snapshot")
			// todo: term和index不用吗?
			kv.loadSnapshot(msg.Snapshot)
		} else {
			kv.logger.Panic("apply: invalid msg", msg)
		}
	}
}
