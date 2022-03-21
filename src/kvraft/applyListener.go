package kvraft

import (
	"sync"
	"time"

	"6.824/common"
	"6.824/raft"
	"github.com/sirupsen/logrus"
)

type ApplychListener struct {
	target    chan raft.ApplyMsg
	mu        sync.Mutex
	readyMap  map[int]chan OPResult
	logger    *logrus.Entry
	timeout   time.Duration
	dataStore KVInterface
	rf        *raft.Raft
}

func NewApplyChListener(target chan raft.ApplyMsg, logger *logrus.Entry, dataStore KVInterface, rf *raft.Raft) *ApplychListener {
	al := &ApplychListener{
		target:    target,
		readyMap:  make(map[int]chan OPResult),
		logger:    logger,
		timeout:   common.ApplyCHTimeout,
		dataStore: dataStore,
		rf:        rf,
	}
	al.logger.Debug("apply: started listener")
	go al.handleApplies()
	return al
}

func (al *ApplychListener) getReadyChanOnIndex(index int) chan OPResult {
	al.mu.Lock()
	defer al.mu.Unlock()
	c, ok := al.readyMap[index]
	if ok {
		return c
	}
	al.logger.WithField("index", index).Debug("apply: new listener on index")
	al.readyMap[index] = make(chan OPResult, 1)
	return al.readyMap[index]
}
func (al *ApplychListener) dropIndex(index int) {
	al.mu.Lock()
	defer al.mu.Unlock()
	delete(al.readyMap, index)
}

func (al *ApplychListener) handleApplies() {
	for msg := range al.target {
		if msg.CommandValid {
			al.logger.WithField("msg", msg).Debug("apply: got msg")
			start := time.Now()
			readyChan := al.getReadyChanOnIndex(msg.CommandIndex)
			select {
			case <-readyChan: // drain bad data
			default:
			}
			op := msg.Command.(Op)
			opResult := al.dataStore.EvalOp(op)
			readyChan <- opResult
			al.logger.WithField("time", time.Since(start)).WithFields(logrus.Fields{
				"op": op.OpKey,
			}).Debug("evalOp: time took")

		} else {
			al.logger.Warn("apply: command not valid")
		}
	}
}

func (al *ApplychListener) waitForApplyOnIndex(index int) OPResult {
	start := time.Now()
	defer al.logger.WithField("time", time.Since(start)).Info("apply: waitFor took")
	readyc := al.getReadyChanOnIndex(index)
	select {
	case <-time.After(al.timeout):
		al.logger.WithField("index", index).Warn("apply: wait timeout on index")
		return OPResult{
			err: ErrTimeout,
		}
	case result := <-readyc:
		al.dropIndex(index)
		al.logger.WithField("index", index).Debug("apply: wait ok on index")
		return result
	}
}
