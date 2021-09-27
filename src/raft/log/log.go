package raftlog

type LogEntry struct {
	Term  int
	Index int
}
