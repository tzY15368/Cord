package raft

import (
	"time"

	"github.com/sirupsen/logrus"
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
	// 拿到的是已经snapshot完，序列化完的状态,在2d里就是一个int，
	// 存在本地持久状态里，遇到过于落后的follower发出去即可

	rf.logger.WithFields(logrus.Fields{
		"raw":   len(snapshot),
		"index": index,
	}).Info("snapshot: got snapshot")
	rf.logger.Debug("snapshot: getting lock")
	rf.mu.Lock()
	//panic("got lock")
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.dumpLog()
	// commitIndex始终小于等于maxIndex，所以不用担心
	baseIndex := rf.getBaseLogIndex()
	// baseIndex := rf.log[0].Index
	fields := logrus.Fields{
		"index": index, "commitIndex": rf.commitIndex, "baseIndex": baseIndex,
	}
	if index > rf.commitIndex {
		rf.logger.WithFields(fields).Panic("snapshot: failed due to invalid index")
	} else {
		rf.logger.WithFields(fields).Info("snapshot: params:")
	}
	newIndex := index - baseIndex
	offset := newIndex - 1
	if newIndex == 0 {
		offset = 0
	}
	if offset < 0 {
		rf.dumpLog()
		rf.logger.WithFields(logrus.Fields{
			"lastIncludedIindex": rf.lastIncludedIndex,
		}).Warn("snapshot: offset < 0, doing absolute nothing")
		return
	}
	lastIncludedEntry := rf.log[offset]

	// handle snapshot
	rf.lastIncludedIndex = lastIncludedEntry.Index
	rf.lastIncludedTerm = lastIncludedEntry.Term
	rf.snapshot = snapshot

	// log compaction
	rf.log = rf.log[newIndex:]
	if len(rf.log) == 0 {
		rf.logger.Panic("snapshot: must have at least 1 elem in log")
	}
	rf.dumpLog()
	rf.logger.WithFields(logrus.Fields{
		"base": baseIndex, "len": newIndex, "lastIncludedIndex": lastIncludedEntry.Index,
	}).Info("compacted logs")
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 同步log，然后把snapshot里的状态发给applychan
	rf.logger.Info("snapshot: installing snapshot")
	//	baseIndex := rf.log[0].Index
	baseIndex := rf.getBaseLogIndex()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.logger.WithField("newTerm", rf.currentTerm).
			Info("snapshot: became follower due to higher client term")
	}
	reply.Term = rf.currentTerm
	if args.LastIncludedIndex <= baseIndex {
		rf.logger.WithFields(logrus.Fields{
			"lastincludedindex": args.LastIncludedIndex,
			"baseIndex":         baseIndex,
		}).Warn("lastincludedindex <= baseindex, doing nothing")
		return
	}
	rf.snapshot = args.Data
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.currentTerm = args.Term
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	// todo: ignore leaderid for now

	rf.applyMsgQueue.put(ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.Term,
		SnapshotIndex: args.LastIncludedIndex,
	})
	rf.log = make([]LogEntry, 0)
	rf.dumpLog()
	rf.logger.Info("installed snapshot")
	// persist after state change
	rf.persist()
	// baseIndex和lastLogIndex
	// baseindex是当前log的第一条，用于后面append确定index
	// baseIndex应该就是lastincludedindex
	// lastlogindex是当前实例已有的最后一条，也是lastincludedindex
}

// 如果appendentries发现对方nextindex过低，发snapshot,并阻塞??
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if args.LastIncludedIndex*args.LastIncludedTerm == 0 {
		rf.logger.WithFields(logrus.Fields{
			"lastincludedindex": rf.lastIncludedIndex,
			"lastincludedterm":  rf.lastIncludedTerm,
		}).Panic("snapshot: invalid last included")
	}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.logger.WithField("at", time.Now()).Info("sendinstallsnapshot successful")
	defer rf.mu.Unlock()
	defer rf.persist()
	if ok {
		// sendinstallsnapshot需要在返回后处理nextindex【server】的变化
		if reply.Term > rf.currentTerm {
			rf.logger.WithFields(logrus.Fields{
				"reply.term":  reply.Term,
				"currentTerm": rf.currentTerm,
			}).Warn("snapshot: reply.term > term, waiting for appendentry to handle")
			return false
		}
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		rf.logger.WithField("nextindex", rf.nextIndex[server]).Info("snapshot: result")
	}
	return ok
}
