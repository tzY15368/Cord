package raft

import (
	"sync"
)

type mutex struct {
	rf *Raft
	mu sync.Mutex
}

func makeLock(rf *Raft) *mutex {
	mu := &mutex{
		rf: rf,
		mu: sync.Mutex{},
	}
	return mu
}

func (m *mutex) Lock() {

	//logrus.Warn("getting lock:", string(debug.Stack()))
	m.mu.Lock()
	if m.rf.state == STATE_LEADER {

		//logrus.Warn("got lock:", string(debug.Stack()))
	}
}

func (m *mutex) Unlock() {
	if m.rf.state == STATE_LEADER {
		//logrus.Warn("releasing lock:", string(debug.Stack()))
	}

	//debug.PrintStack()
	m.mu.Unlock()
	//logrus.Warn("released lock,", runtime.NumGoroutine(), string(debug.Stack()))
}
