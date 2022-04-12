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
	if len(rf.log) > 10 {
		logSlice = rf.log[len(rf.log)-4:]
	} else {
		logSlice = rf.log
	}
	rf.logger.WithFields(logrus.Fields{
		"actualLength":      len(rf.log),
		"lastApplied":       rf.lastApplied,
		"commitIndex":       rf.commitIndex,
		"lastIncludedIndex": rf.lastIncludedIndex,
	}).Debugf("log: %+v", logSlice)
}

func (rf *Raft) dumpLogFields() *logrus.Entry {
	logString := ""
	if len(rf.log) > 3 {
		logString += fmt.Sprintf("%+v", rf.log[0])
		logString += "..."
		logString += fmt.Sprintf("%+v", rf.log[len(rf.log)-2:])
	} else {
		logString = fmt.Sprintf("%+v", rf.log)
	}
	return rf.logger.WithFields(logrus.Fields{
		"lastIncludedIndex": rf.lastIncludedIndex,
		"actualLen":         len(rf.log),
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
	//	baseIndex := rf.log[0].Index
	rf.unsafeCommitLog()
	rf.mu.Unlock()
}

func (rf *Raft) unsafeCommitLog() {
	baseIndex := rf.getBaseLogIndex()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			CommandIndex: i,
			Command:      common.DecodeCommand(rf.log[i-baseIndex].Command),
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
	if len(rf.log) == 0 {
		if rf.lastIncludedTerm == 0 {
			rf.logger.Panic("log: invalid log state:term")
		} else {
			rf.logger.
				WithField("lastIncludedTerm", rf.lastIncludedTerm).
				Debug("no existing logs, returning lastincludedTerm")
		}
		return rf.lastIncludedTerm
	}
	return int(rf.log[len(rf.log)-1].Term)
}

// getlastlogindex not thread safe
func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		if rf.lastIncludedIndex == 0 {
			rf.logger.Panic("log: invalid log state:index")
		} else {
			rf.logger.
				WithField("lastIncludedIndex", rf.lastIncludedIndex).
				Debug("no existing logs, returning lastincludedIndex")
		}
		return rf.lastIncludedIndex
	}
	return int(rf.log[len(rf.log)-1].Index)
}

// 具体是什么？还得仔细想下
// 目的是正确处理空log的情况
func (rf *Raft) getBaseLogIndex() int {
	if len(rf.log) == 0 {
		if rf.lastIncludedIndex == 0 {
			rf.logger.Panic("log: invalid log state:index")
		} else {
			rf.logger.
				WithField("lastIncludedIndex", rf.lastIncludedIndex).
				Debug("no existing logs, returning lastincludedIndex")
		}
		return rf.lastIncludedIndex
	}
	return int(rf.log[0].Index)
}

// getLogTermAtOffset not thread safe
func (rf *Raft) getLogTermAtOffset(offset int) int {
	if len(rf.log) == 0 {
		if offset != 0 && rf.lastIncludedTerm == 0 {
			rf.logger.WithFields(logrus.Fields{
				"index": offset, "lastIncludedTerm": rf.lastIncludedTerm,
			}).Panic("invalid state")
		}
		return rf.lastIncludedTerm
	}
	return int(rf.log[offset].Term)
}

// getLogIndexAtOffset not thread safe
func (rf *Raft) getLogIndexAtOffset(offset int) int {
	if len(rf.log) == 0 {
		if offset != 0 && rf.lastIncludedIndex == 0 {
			rf.logger.WithFields(logrus.Fields{
				"index": offset, "lastIncludedindex": rf.lastIncludedIndex,
			}).Panic("invalid state")
		}
		return rf.lastIncludedIndex
	}
	return int(rf.log[offset].Index)
}
