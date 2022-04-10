package cord

import (
	"fmt"
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
			fmt.Println("inbound command: ", msg.CommandIndex)
			var res *kv.EvalResult
			var dump *[]byte
			ss := cs.tryStartSnapshot()
			res, dump = cs.kvStore.EvalCMD(&args, ss)
			if ss {
				cs.rf.Snapshot(msg.CommandIndex, *dump)
				swapped := atomic.CompareAndSwapInt32(&cs.inSnapshot, 1, 0)
				if !swapped {
					panic("no swap")
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
