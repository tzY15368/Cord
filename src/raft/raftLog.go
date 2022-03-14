package raft

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

func (rf *Raft) hasConflictLog(leaderLog []LogEntry, localLog []LogEntry) bool {
	for i := 0; i < len(leaderLog) && i < len(localLog); i++ {
		if leaderLog[i].Term != localLog[i].Term {
			return true
		}
	}
	return false
}

// dumpLog not thread safe
func (rf *Raft) dumpLog() {
	rf.logger.Infof("log: %+v", rf.log)
}

func (rf *Raft) commitLog() {
	rf.mu.Lock()
	baseIndex := rf.log[0].Index

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			CommandIndex: i,
			Command:      rf.log[i-baseIndex].Command,
		}
		rf.applyMsgQueue.put(msg)
		// rf.chanApply <- msg
	}
	// todo: this is safe as long as channel writes are FIFO. otherwise there may be replays
	rf.lastApplied = rf.commitIndex
	rf.mu.Unlock()
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}
