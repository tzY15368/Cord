package cord

import (
	"errors"
	"time"

	"6.824/cord/kv"
	"6.824/proto"
)

var ErrRetry = errors.New("Err retry request")
var ErrNotLeader = errors.New("Err no leader")
var ErrApplyTimeout = errors.New("Err apply timeout")

func (cs *CordServer) waitForApply(index int64) *kv.EvalResult {
	cs.mu.Lock()
	ch, ok := cs.notify[index]
	if !ok {
		ch = make(chan *kv.EvalResult, 1)
		cs.notify[index] = ch
	}
	cs.mu.Unlock()
	select {
	case result := <-ch:
		return result
	case <-time.After(time.Duration(cs.bootConfig.ApplyTimeoutMil) * time.Millisecond):
		return &kv.EvalResult{Err: ErrApplyTimeout}
	}
}

func (cs *CordServer) propose(args proto.ServiceArgs) *kv.EvalResult {

	index, _, isLeader := cs.rf.Start(args)
	if !isLeader {
		return &kv.EvalResult{Err: ErrNotLeader}
	}

	doneChan := make(chan *kv.EvalResult, 1)
	lostLeaderChan := make(chan struct{}, 1)
	go func() {
		res := cs.waitForApply(int64(index))
		if res.Info.ClientID != args.Info.ClientID || res.Info.RequestID != args.Info.RequestID {
			cs.logger.WithField("index", index).Warn("server: propose: different info on index")
			lostLeaderChan <- struct{}{}
		} else {
			doneChan <- res
		}
	}()
	select {
	case reply := <-doneChan:
		return reply
	case <-lostLeaderChan:
		return &kv.EvalResult{Err: ErrNotLeader}
	}

}
