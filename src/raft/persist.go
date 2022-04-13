package raft

import (
	"fmt"
	"time"

	"6.824/proto"
	"github.com/sirupsen/logrus"
)

type IPersistable interface {
	ReadRaftState() []byte

	RaftStateSize() int

	SaveStateAndSnapshot([]byte, []byte)

	ReadSnapshot() []byte

	SnapshotSize() int
}

// persist not thread safe
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// unsafe, use inside locks
	// w := new(bytes.Buffer)
	// e2 := labgob.NewEncoder(w)
	// // state
	// e2.Encode(rf.pState.CurrentTerm)
	// e2.Encode(rf.pState.VotedFor)

	// // snapshot state
	// e2.Encode(rf.pState.LastIncludedTerm)
	// e2.Encode(rf.pState.LastIncludedIndex)

	// // log

	// e2.Encode(rf.pState.Log)
	//data := w.Bytes()

	data, err := rf.pState.Marshal()
	if err != nil {
		panic(err)
	}

	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
	// rf.logger.WithFields(logrus.Fields{
	// 	"lastIncludedIndex": rf.pState.LastIncludedIndex,
	// }).Debug("save persist ok")
	// .WithField("at", time.Now())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	raftState := rf.persister.ReadRaftState()
	raftSnapshot := rf.persister.ReadSnapshot()
	//r := bytes.NewBuffer(raftState)
	//d := labgob.NewDecoder(r)
	if len(raftState) == 0 {
		rf.logger.Warn("prev raft state is empty")
		return
	}
	s := &proto.PersistedState{}
	err := s.Unmarshal(raftState)
	if err != nil {
		rf.logger.WithError(err).Warn("persist: read persist failed")
		return
	}
	rf.pState = s

	// var term, votedFor int
	// var logs []*proto.LogEntry
	// var lastIncludedTerm, lastIncludedIndex int

	// if err := d.Decode(&term); err != nil {
	// 	rf.logger.WithError(err).Warn("persist: read persist failed")
	// 	return
	// }
	// if err := d.Decode(&votedFor); err != nil {
	// 	rf.logger.WithError(err).Warn("persist: read persist failed")
	// 	return
	// }
	// if err := d.Decode(&lastIncludedTerm); err != nil {
	// 	rf.logger.WithError(err).Warn("persist: read persist failed")
	// 	return
	// }
	// if err := d.Decode(&lastIncludedIndex); err != nil {
	// 	rf.logger.WithError(err).Warn("persist: read persist failed")
	// 	return
	// }
	// if err := d.Decode(&logs); err != nil {
	// 	rf.logger.WithError(err).Warn("persist: read persist failed")
	// 	return
	// }
	//rf.pState.CurrentTerm = term
	//rf.pState.VotedFor = votedFor
	//rf.pState.Log = logs
	// 由于重启后直接读的snapshot，里面的lastincludedindex==commitindex==lastapplied
	//rf.pState.LastIncludedIndex = lastIncludedIndex
	//rf.pState.LastIncludedTerm = lastIncludedTerm
	rf.lastApplied = int(s.LastIncludedIndex)
	rf.commitIndex = int(s.LastIncludedIndex)
	rf.snapshot = raftSnapshot
	if rf.pState.LastIncludedIndex > 0 {
		msg := ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      rf.snapshot,
			SnapshotTerm:  int(rf.pState.LastIncludedTerm),
			SnapshotIndex: int(rf.pState.LastIncludedIndex),
		}
		rf.applyMsgQueue.put(msg)
	}
	rf.logger.WithField("at", time.Now()).WithFields(logrus.Fields{
		"pstate": fmt.Sprintf("%+v", s),
	}).Info("persist: read persist ok")
}
