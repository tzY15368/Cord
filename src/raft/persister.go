package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"os"
	"sync"
)

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
	fd        *os.File
	write     bool
}

func MakePersister() *Persister {
	write := false
	var fd *os.File
	var err error
	if write {

		fd, err = os.OpenFile(randstring(5), os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			panic(err)
		}
	}
	return &Persister{fd: fd, write: write}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(state)
	if ps.write {

		ps.fd.Write(state)
	}
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.write {

		var b []byte
		ps.fd.ReadAt(b, 0)
	}
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.write {

		ps.fd.Write(state)
		ps.fd.Write(snapshot)
	}
	ps.raftstate = clone(state)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
