package raft

import (
	"bytes"

	"6.824/labgob"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// unsafe, use inside locks
	w := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w)
	e2.Encode(rf.currentTerm)
	e2.Encode(rf.votedFor)
	e2.Encode(rf.log)
	rf.persister.SaveRaftState(w.Bytes())
	rf.logger.Debug("save persist ok")
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term, votedFor int
	var logs []LogEntry

	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		rf.logger.Warn("persist: read persist failed")
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = logs
		rf.mu.Unlock()
		rf.logger.Info("persist: read persist ok")
	}
}
