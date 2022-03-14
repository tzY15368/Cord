package raft

import (
	"bytes"

	"6.824/labgob"
)

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// 拿到的是已经snapshot完，序列化完的状态，直接发出去即可
	b := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(b)
	var cmd interface{}
	d.Decode(&cmd)
	rf.logger.WithField("raw", cmd).WithField("index", index).Info("snapshot: got snapshot")
	rf.logger.Info("getting lock")
	// 心跳超时也是因为他先block了leader的lock，导致leader的其他操作也拿不到lock，自然会超时
	//rf.mu.Lock()
	//panic("got lock")

	//defer rf.mu.Unlock()
	// rf.dumpLog()
	// if index > rf.commitIndex {
	// 	rf.logger.WithFields(logrus.Fields{
	// 		"index": index, "commitIndex": rf.commitIndex,
	// 	}).Warn("snapshot: failed due to invalid index")
	// }
	// baseIndex := rf.log[0].Index
	// realOffset := index - baseIndex + 1
	// if realOffset+1 > len(rf.log) {
	// 	rf.logger.WithField("index", index).WithField("lastlogindex", rf.getLastLogIndex()).
	// 		Panic("snapshot: invalid index")
	// }
	// rf.log = rf.log[realOffset:]
	// rf.dumpLog()
	// rf.logger.WithFields(logrus.Fields{
	// 	"base": baseIndex, "len": realOffset,
	// }).Info("compacted logs")
}
