package raft

import (
	"runtime/debug"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type mutex struct {
	rf          *Raft
	mu          sync.Mutex
	lockAt      time.Time
	stack       string
	releaseChan chan struct{}
}

func makeLock(rf *Raft) *mutex {
	mu := &mutex{
		rf:          rf,
		mu:          sync.Mutex{},
		releaseChan: make(chan struct{}),
	}
	return mu
}

func (m *mutex) Lock() {

	//logrus.Warn("getting lock:", string(debug.Stack()))
	m.mu.Lock()
	// m.lockAt = time.Now()
	// m.stack = string(debug.Stack())
	if false && m.rf.state == STATE_LEADER {

		logrus.Warnf("[%d]got lock at %s:%s", m.rf.me, time.Now(), string(debug.Stack()))
	}
	// go func() {
	// 	select {
	// 	case <-time.After(70 * time.Millisecond):
	// 		logrus.Panic("lock held for too long", m.stack)
	// 	case <-m.releaseChan:
	// 		return
	// 	}

	// }()
}

func (m *mutex) Unlock() {
	if false && m.rf.state == STATE_LEADER {
		logrus.Warnf("[%d]releasing lock at %s:%s", m.rf.me, time.Now(), string(debug.Stack()))
	}
	//debug.PrintStack()
	//m.releaseChan <- struct{}{}
	m.mu.Unlock()
	//logrus.Warn("released lock,", runtime.NumGoroutine(), string(debug.Stack()))
}
