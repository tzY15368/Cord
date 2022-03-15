package raft

import (
	"bytes"
	"time"

	"6.824/labgob"
)

// persist not thread safe
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// unsafe, use inside locks
	w := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w)
	// state
	e2.Encode(rf.currentTerm)
	e2.Encode(rf.votedFor)
	e2.Encode(rf.log)

	// snapshot state
	e2.Encode(rf.lastIncludedTerm)
	e2.Encode(rf.lastIncludedIndex)
	rf.persister.SaveRaftState(w.Bytes())
	rf.persister.SaveStateAndSnapshot(w.Bytes(), rf.snapshot)
	rf.logger.Debug("save persist ok")
	// .WithField("at", time.Now())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist() {
	raftState := rf.persister.ReadRaftState()
	raftSnapshot := rf.persister.ReadSnapshot()
	r := bytes.NewBuffer(raftState)
	d := labgob.NewDecoder(r)

	var term, votedFor int
	var logs []LogEntry
	var lastIncludedTerm, lastIncludedIndex int

	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&lastIncludedTerm) != nil && d.Decode(&lastIncludedIndex) != nil {
		rf.logger.Warn("persist: read persist failed")
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.snapshot = raftSnapshot
		rf.mu.Unlock()
		rf.logger.WithField("at", time.Now()).Info("persist: read persist ok")
	}
}
