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
