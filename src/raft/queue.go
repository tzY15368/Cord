package raft

import (
	"sync"
)

type msgQueue struct {
	data     []ApplyMsg
	target   chan ApplyMsg
	notEmpty sync.Cond
}

func NewQueue(dumpTarget chan ApplyMsg) *msgQueue {
	q := &msgQueue{
		data:     make([]ApplyMsg, 0),
		notEmpty: *sync.NewCond(new(sync.Mutex)),
		target:   dumpTarget,
	}
	go q.dumpMsg()
	return q
}

func (q *msgQueue) dumpMsg() {

	for {
		v := q.get()
		q.target <- v
	}
}

func (q *msgQueue) size() int {
	q.notEmpty.L.Lock()
	defer q.notEmpty.L.Unlock()
	return len(q.data)
}

func (q *msgQueue) get() ApplyMsg {
	q.notEmpty.L.Lock()
	for len(q.data) == 0 {
		q.notEmpty.Wait()
	}
	msg := q.data[0]
	q.data = q.data[1:]
	q.notEmpty.L.Unlock()
	return msg
}

func (q *msgQueue) put(am ApplyMsg) {
	q.notEmpty.L.Lock()
	defer q.notEmpty.L.Unlock()
	q.data = append(q.data, am)
	if len(q.data) == 1 {
		q.notEmpty.Signal()
	}
}
