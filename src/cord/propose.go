package cord

import (
	"errors"
	"fmt"
	"time"

	"6.824/cord/intf"
	"6.824/proto"
)

var ErrRetry = errors.New("err retry request")
var ErrNotLeader = errors.New("err no leader")
var ErrApplyTimeout = errors.New("err apply timeout")

type ProposeResult struct {
	err  error
	info *proto.RequestInfo
}

func (p *ProposeResult) GetClientID() int64 {
	if p.info != nil {
		return p.info.ClientID
	}
	return -1
}
func (p *ProposeResult) GetRequestID() int64 {
	if p.info != nil {
		return p.info.RequestID
	}
	return -1
}
func (p *ProposeResult) GetError() error {
	return p.err
}
func (p *ProposeResult) GetData() map[string]string {
	return nil
}
func (p *ProposeResult) AwaitWatches() map[string]string {
	return nil
}

func (cs *CordServer) waitForApply(index int64) intf.IEvalResult {
	cs.mu.Lock()
	ch, ok := cs.notify[index]
	if !ok {
		ch = make(chan intf.IEvalResult, 1)
		cs.notify[index] = ch
	}
	cs.mu.Unlock()
	select {
	case result := <-ch:
		return result
	case <-time.After(time.Duration(cs.bootConfig.ApplyTimeoutMil) * time.Millisecond):
		return &ProposeResult{err: ErrApplyTimeout}
	}
}

func (cs *CordServer) propose(args proto.ServiceArgs) intf.IEvalResult {
	if !args.Linearizable {
		fmt.Println("warning: unlinearizable read")
		res, _ := cs.kvStore.EvalCMD(&args, false, true)
		return res
	}
	index, _, isLeader := cs.rf.Start(args)
	if !isLeader {
		return &ProposeResult{err: ErrNotLeader}
	}

	doneChan := make(chan intf.IEvalResult, 1)
	lostLeaderChan := make(chan struct{}, 1)
	go func() {
		res := cs.waitForApply(int64(index))
		if res.GetClientID() != args.Info.RequestID || res.GetRequestID() != args.Info.RequestID {
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
		return &ProposeResult{err: ErrNotLeader}
	}

}
