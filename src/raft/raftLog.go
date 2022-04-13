package raft

import (
	"fmt"

	"6.824/common"
	"6.824/proto"
	"github.com/sirupsen/logrus"
)

// type LogEntry struct {
// 	Term    int
// 	Index   int
// 	Command interface{}
// }

func (rf *Raft) hasConflictLog(leaderLog []*proto.LogEntry, localLog []*proto.LogEntry) bool {
	for i := 0; i < len(leaderLog) && i < len(localLog); i++ {
		if leaderLog[i].Term != localLog[i].Term {
			return true
		}
	}
	return false
}

// dumpLog not thread safe
func (rf *Raft) dumpLog() {
	var logSlice []*proto.LogEntry
	if len(rf.pState.Log) > 10 {
		logSlice = rf.pState.Log[len(rf.pState.Log)-4:]
	} else {
		logSlice = rf.pState.Log
	}
	rf.logger.WithFields(logrus.Fields{
		"actualLength":      len(rf.pState.Log),
		"lastApplied":       rf.lastApplied,
		"commitIndex":       rf.commitIndex,
		"lastIncludedIndex": rf.pState.LastIncludedIndex,
	}).Debugf("log: %+v", logSlice)
}

func (rf *Raft) dumpLogFields() *logrus.Entry {
	logString := ""
	if len(rf.pState.Log) > 3 {
		logString += fmt.Sprintf("%+v", rf.pState.Log[0])
		logString += "..."
		logString += fmt.Sprintf("%+v", rf.pState.Log[len(rf.pState.Log)-2:])
	} else {
		logString = fmt.Sprintf("%+v", rf.pState.Log)
	}
	return rf.logger.WithFields(logrus.Fields{
		"lastIncludedIndex": rf.pState.LastIncludedIndex,
		"actualLen":         len(rf.pState.Log),
		"log":               logString,
	})
}

func (rf *Raft) SafeDumpLogFields() *logrus.Entry {
	rf.mu.Lock()

	defer rf.mu.Unlock()
	return rf.dumpLogFields()
}

func (rf *Raft) commitLog() {
	rf.mu.Lock()
	//	baseIndex := rf.pState.Log[0].Index
	rf.unsafeCommitLog()
	rf.mu.Unlock()
}

func (rf *Raft) unsafeCommitLog() {
	baseIndex := rf.getBaseLogIndex()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			CommandIndex: i,
			Command:      common.DecodeCommand(rf.pState.Log[i-baseIndex].Command),
		}
		rf.logger.WithField("msg", msg).Debug("commit: msg=")
		rf.applyMsgQueue.put(msg)
		// rf.chanApply <- msg
	}
	// todo: this is safe as long as channel writes are FIFO. otherwise there may be replays
	rf.lastApplied = rf.commitIndex

}

// get lastlog term not thread safe
func (rf *Raft) getLastLogTerm() int {
	if len(rf.pState.Log) == 0 {
		if rf.pState.LastIncludedTerm == 0 {
			rf.logger.Panic("log: invalid log state:term")
		} else {
			rf.logger.
				WithField("lastIncludedTerm", rf.pState.LastIncludedTerm).
				Debug("no existing logs, returning lastincludedTerm")
		}
		return int(rf.pState.LastIncludedTerm)
	}
	return int(rf.pState.Log[len(rf.pState.Log)-1].Term)
}

// getlastlogindex not thread safe
func (rf *Raft) getLastLogIndex() int {
	if len(rf.pState.Log) == 0 {
		if rf.pState.LastIncludedIndex == 0 {
			rf.logger.Panic("log: invalid log state:index")
		} else {
			rf.logger.
				WithField("lastIncludedIndex", rf.pState.LastIncludedIndex).
				Debug("no existing logs, returning lastincludedIndex")
		}
		return int(rf.pState.LastIncludedIndex)
	}
	return int(rf.pState.Log[len(rf.pState.Log)-1].Index)
}

// 具体是什么？还得仔细想下
// 目的是正确处理空log的情况
func (rf *Raft) getBaseLogIndex() int {
	if len(rf.pState.Log) == 0 {
		if rf.pState.LastIncludedIndex == 0 {
			rf.logger.Panic("log: invalid log state:index")
		} else {
			rf.logger.
				WithField("lastIncludedIndex", rf.pState.LastIncludedIndex).
				Debug("no existing logs, returning lastincludedIndex")
		}
		return int(rf.pState.LastIncludedIndex)
	}
	return int(rf.pState.Log[0].Index)
}

// getLogTermAtOffset not thread safe
func (rf *Raft) getLogTermAtOffset(offset int) int {
	if len(rf.pState.Log) == 0 {
		if offset != 0 && rf.pState.LastIncludedTerm == 0 {
			rf.logger.WithFields(logrus.Fields{
				"index": offset, "lastIncludedTerm": rf.pState.LastIncludedTerm,
			}).Panic("invalid state")
		}
		return int(rf.pState.LastIncludedTerm)
	}
	return int(rf.pState.Log[offset].Term)
}

// getLogIndexAtOffset not thread safe
func (rf *Raft) getLogIndexAtOffset(offset int) int {
	if len(rf.pState.Log) == 0 {
		if offset != 0 && rf.pState.LastIncludedIndex == 0 {
			rf.logger.WithFields(logrus.Fields{
				"index": offset, "lastIncludedindex": rf.pState.LastIncludedIndex,
			}).Panic("invalid state")
		}
		return int(rf.pState.LastIncludedIndex)
	}
	return int(rf.pState.Log[offset].Index)
}
