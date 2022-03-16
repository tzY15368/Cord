package kvraft

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"6.824/raft"
	"github.com/sirupsen/logrus"
)

type IndexListener struct {
	majorityChan chan struct{}
	count        int32
}

type ApplyHandler struct {
	timeout time.Duration
	applyCh chan raft.ApplyMsg

	// applyMap map log index to IndexListener
	applyMap map[int]*IndexListener

	mu sync.Mutex

	//
	peerCount int
	logger    *logrus.Entry
}

var ErrApplyTimeout = errors.New("error timeout")

func NewApplyHandler(applyCh chan raft.ApplyMsg, peerCount int) *ApplyHandler {
	ah := &ApplyHandler{
		timeout:   10 * time.Second,
		applyCh:   applyCh,
		peerCount: peerCount,
		applyMap:  make(map[int]*IndexListener),
		logger:    logrus.WithField("applyHandler", ""),
	}
	go ah.handleApplies()
	return ah
}

func (ah *ApplyHandler) cleanApplyMap() {

}

func (ah *ApplyHandler) getIndexListener(index int) *IndexListener {
	ah.mu.Lock()
	defer ah.mu.Unlock()
	listener, ok := ah.applyMap[index]
	if ok {
		return listener
	} else {
		_listener := &IndexListener{
			// 新知识：如果是unbuffered chan，有写必有读，否则写也阻塞
			majorityChan: make(chan struct{}, 1),
			count:        0,
		}
		ah.applyMap[index] = _listener
		return _listener
	}
}

func (ah *ApplyHandler) dropIndexListener(index int) {
	ah.mu.Lock()
	defer ah.mu.Unlock()
	delete(ah.applyMap, index)
}

func (ah *ApplyHandler) handleApplies() {
	for msg := range ah.applyCh {
		if msg.CommandValid {
			listener := ah.getIndexListener(msg.CommandIndex)
			val := atomic.AddInt32(&listener.count, 1)
			//listener.count++
			if int(val) == (ah.peerCount+1)/2 {
				listener.majorityChan <- struct{}{}
			}
			if int(listener.count) == ah.peerCount {
				ah.dropIndexListener(msg.CommandIndex)
			}
		} else {
			ah.logger.Warn("handleApplies: command not valid")
		}
	}
}

// waitForMajorityOnIndex thread safe
// returns true for success, false for fail
func (ah *ApplyHandler) waitForMajorityOnIndex(index int) bool {
	listener := ah.getIndexListener(index)
	select {
	case <-time.After(ah.timeout):
		return false
	case <-listener.majorityChan:
		return true
	}
}
