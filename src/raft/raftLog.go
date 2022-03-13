package raft

type LogEntry struct {
	Term    int
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

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.chanApply <- ApplyMsg{CommandIndex: i, CommandValid: true, Command: rf.log[i].Command}
	}
	rf.lastApplied = rf.commitIndex
}
