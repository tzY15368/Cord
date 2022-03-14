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

func (rf *Raft) commitLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	baseIndex := rf.log[0].Index
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.chanApply <- ApplyMsg{
			CommandIndex: i,
			CommandValid: true,
			Command:      rf.log[i-baseIndex].Command,
		}
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}
