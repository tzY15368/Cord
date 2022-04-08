package cord

import (
	"reflect"
	"sync/atomic"

	"6.824/cord/kv"
	"6.824/proto"
)

func (cs *CordServer) tryStartSnapshot() bool {
	if cs.maxRaftState == -1 {
		return false
	}
	if cs.maxRaftState*9/10 > int64(cs.rf.GetStateSize()) {
		swapped := atomic.CompareAndSwapInt32(&cs.inSnapshot, 0, 1)
		if swapped {
			return true
		}
	}
	return false
}

func (cs *CordServer) handleApply() {
	for msg := range cs.applyChan {
		if msg.CommandValid {
			args, ok := msg.Command.(proto.ServiceArgs)
			if !ok {
				panic("conversion error:" + reflect.TypeOf(msg.Command).String())
			}
			var res *kv.EvalResult
			if !*args.Linearizable {
				res = cs.kvStore.EvalGETUnserializable(&args)
			} else {
				ss := cs.tryStartSnapshot()
				var dump *[]byte
				res, dump = cs.kvStore.EvalCMD(&args, ss)
				if ss {
					cs.rf.Snapshot(msg.CommandIndex, *dump)
				}
			}
			cs.mu.Lock()
			ch, ok := cs.notify[int64(msg.CommandIndex)]
			if ok {
				select {
				case <-ch:
				default:
				}
			} else {
				ch = make(chan *kv.EvalResult, 1)
				cs.notify[int64(msg.CommandIndex)] = ch
			}
			cs.mu.Unlock()
			ch <- res
		} else {
			panic("no snapshot yet")
		}
	}
}
