package shardctrler

import (
	"sync/atomic"
	"time"

	"6.824/common"
)

func (sc *ShardCtrler) proposeAndApply(op Op) (*Config, Err) {

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return nil, ErrWrongLeader
	}
	sc.logger.WithField("index", index).Debug("propose: start is ok")
	doneChan := make(chan opResult, 1)
	lostLeaderChan := make(chan struct{}, 1)
	go func() {
		opRes := sc.applyEntry(index)
		if !opRes.requestInfo.Equals(&op.RequestInfo) || opRes.err == ErrTimeout {
			lostLeaderChan <- struct{}{}
		} else {
			doneChan <- opRes
		}
	}()
	var run int32 = 1
	go func() {
		for atomic.LoadInt32(&run) == 1 {
			_, isLeader := sc.rf.GetState()
			if !isLeader {
				sc.logger.Warn("kv: lost leadership")
				lostLeaderChan <- struct{}{}
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	select {
	case opRes := <-doneChan:
		atomic.StoreInt32(&run, 0)
		return opRes.cfg, OK
	case <-lostLeaderChan:
		atomic.StoreInt32(&run, 0)
		return nil, ErrWrongLeader
	}
}

func (sc *ShardCtrler) applyEntry(index int) opResult {

	sc.mu.Lock()
	ch, ok := sc.notify[index]
	if !ok {
		ch = make(chan opResult, 1)
		sc.notify[index] = ch
	}
	sc.mu.Unlock()

	select {
	case result := <-ch:
		return result
	case <-time.After(common.ApplyCHTimeout):
		return opResult{err: ErrTimeout}
	}
}

func (sc *ShardCtrler) applyMsgHandler() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			opRes := sc.evalOp(op)
			sc.mu.Lock()
			ch, ok := sc.notify[msg.CommandIndex]
			if ok {
				select {
				case <-ch: // drain bad data
				default:
				}
			} else {
				ch = make(chan opResult, 1)
				sc.notify[msg.CommandIndex] = ch
			}
			sc.mu.Unlock()
			ch <- opRes
		} else if msg.SnapshotValid {
			sc.logger.Warn("apply: applying snapshot")
			sc.loadSnapshot(msg.Snapshot)
		} else {
			sc.logger.Panic("apply: invalid command", msg)
		}
	}
}
