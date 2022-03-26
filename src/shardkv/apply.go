package shardkv

import (
	"fmt"
	"time"

	"6.824/common"
	"github.com/sirupsen/logrus"
)

func (kv *ShardKV) proposeAndApply(op Op, replier replyable) {
	// 1: 如果是CFG，直接进入start
	// 2:检查是否是正确的group
	var wrongSv bool
	if op.OP_TYPE == OP_CFG {
		goto DirectStart
	}
	wrongSv = !kv.isKeyServed(op.OP_KEY)
	if wrongSv {
		replier.SetErr(ErrWrongGroup)
		return
	}

	if kv.isKeyLocked(op.OP_KEY) {
		replier.SetErr(ErrReConfigure)
		return
	}

DirectStart:
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		replier.SetErr(ErrWrongLeader)
		return
	}
	kv.logger.WithFields(logrus.Fields{
		"index": index,
		"op":    fmt.Sprintf("%+v", op),
	}).Debug("skv: propose: start is ok")
	doneChan := make(chan opResult, 1)
	lostLeaderChan := make(chan struct{}, 1)
	// var runCheckLeader int32 = 1
	go func() {
		opRes := kv.applyEntry(index)
		if !opRes.RequestInfo.Equals(&op.RequestInfo) {
			lostLeaderChan <- struct{}{}
			kv.logger.WithField("index", index).Warn("skv: propose: different content on index")
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
			var opRes opResult
			if op.OP_TYPE == OP_CFG {
				// 给对应key加锁或解锁
				opRes = kv.evalCFGOp(&op)
			} else if op.OP_TYPE == OP_MIGRATE {

			} else {
				opRes = kv.evalOp(msg.CommandIndex, op)
			}
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
