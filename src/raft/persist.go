package raft

import (
	"bytes"
	"time"

	"6.824/labgob"
	"github.com/sirupsen/logrus"
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

	// snapshot state
	e2.Encode(rf.lastIncludedTerm)
	e2.Encode(rf.lastIncludedIndex)

	// log

	e2.Encode(rf.log)

	rf.persister.SaveStateAndSnapshot(w.Bytes(), rf.snapshot)
	rf.logger.WithFields(logrus.Fields{
		"lastIncludedIndex": rf.lastIncludedIndex,
	}).Debug("save persist ok")
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

	if err := d.Decode(&term); err != nil {
		rf.logger.WithError(err).Warn("persist: read persist failed")
		return
	}
	if err := d.Decode(&votedFor); err != nil {
		rf.logger.WithError(err).Warn("persist: read persist failed")
		return
	}
	if err := d.Decode(&lastIncludedTerm); err != nil {
		rf.logger.WithError(err).Warn("persist: read persist failed")
		return
	}
	if err := d.Decode(&lastIncludedIndex); err != nil {
		rf.logger.WithError(err).Warn("persist: read persist failed")
		return
	}
	if err := d.Decode(&logs); err != nil {
		rf.logger.WithError(err).Warn("persist: read persist failed")
		return
	}
	rf.mu.Lock()
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.log = logs
	// 由于重启后直接读的snapshot，里面的lastincludedindex==commitindex==lastapplied
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.snapshot = raftSnapshot
	rf.mu.Unlock()
	rf.logger.WithField("at", time.Now()).WithFields(logrus.Fields{
		"currentTerm":       term,
		"votedFor":          votedFor,
		"logs":              logs,
		"lastIncludedIndex": lastIncludedIndex,
		"lastIncludedTerm":  lastIncludedTerm,
	}).Info("persist: read persist ok")
}
