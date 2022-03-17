package kvraft

import (
	"fmt"
	"sync"
	"time"

	"6.824/raft"
	"github.com/sirupsen/logrus"
)

type ApplychListener struct {
	target    chan raft.ApplyMsg
	mu        sync.Mutex
	readyMap  map[int]chan struct{}
	logger    *logrus.Entry
	timeout   time.Duration
	dataStore KVInterface
	rf        *raft.Raft
}

func NewApplyChListener(target chan raft.ApplyMsg, logger *logrus.Entry, dataStore KVInterface, rf *raft.Raft) *ApplychListener {
	al := &ApplychListener{
		target:    target,
		readyMap:  make(map[int]chan struct{}),
		logger:    logger,
		timeout:   10 * time.Second,
		dataStore: dataStore,
		rf:        rf,
	}
	al.logger.Debug("apply: started listener")
	go al.handleApplies()
	return al
}

func (al *ApplychListener) getReadyChanOnIndex(index int) chan struct{} {
	al.mu.Lock()
	defer al.mu.Unlock()
	c, ok := al.readyMap[index]
	if ok {
		return c
	}
	al.logger.WithField("index", index).Debug("apply: new listener on index")
	al.readyMap[index] = make(chan struct{}, 1)
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
			readyChan := al.getReadyChanOnIndex(msg.CommandIndex)
			_, isLeader := al.rf.GetState()
			if isLeader {
				readyChan <- struct{}{}
			} else {
				al.dropIndex(msg.CommandIndex)
				op := msg.Command.(Op)
				var err error
				switch op.OpType {
				case OP_APPEND:
					err = al.dataStore.Append(op.OpKey, op.OPValue)
				case OP_GET:
					_, err = al.dataStore.Get(op.OpKey)
				case OP_PUT:
					err = al.dataStore.Put(op.OpKey, op.OPValue)
				}
				al.dataStore.HandleError(err, nil, "apply: follower: "+op.OpType)
				al.logger.WithField("op", fmt.Sprintf("%+v", op)).Debug("apply: follower: applyok")
			}

		} else {
			al.logger.Warn("apply: command not valid")
		}
	}
}

func (al *ApplychListener) waitForApplyOnIndex(index int) bool {
	readyc := al.getReadyChanOnIndex(index)
	select {
	case <-time.After(al.timeout):
		al.logger.WithField("index", index).Warn("apply: wait timeout on index")
		return false
	case <-readyc:
		al.dropIndex(index)
		al.logger.WithField("index", index).Debug("apply: wait ok on index")
		return true
	}
}
