package raft

import "github.com/sirupsen/logrus"

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false

	if args.Term < rf.currentTerm {
		// reject requests with stale term number
		reply.Term = rf.currentTerm
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		return
	}

	if args.Term > rf.currentTerm {
		// become follower and update current term
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// confirm heartbeat to refresh timeout
	rf.chanHeartbeat <- true

	reply.Term = rf.currentTerm

	// optimization
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		return
	}

	// 待优化
	baseIndex := rf.log[0].Index

	if args.PrevLogIndex >= baseIndex && args.PrevLogTerm != rf.log[args.PrevLogIndex-baseIndex].Term {
		// if entry log[prevLogIndex] conflicts with new one, there may be conflict entries before.
		// we can bypass all entries during the problematic term to speed up.
		term := rf.log[args.PrevLogIndex-baseIndex].Term
		for i := args.PrevLogIndex - 1; i >= baseIndex && rf.log[i-baseIndex].Term == term; i-- {
			reply.NextTryIndex = i + 1
		}
	} else if args.PrevLogIndex >= baseIndex-1 {
		// otherwise log up to prevLogIndex are safe.
		// we can merge lcoal log and entries from leader, and apply log if commitIndex changes.
		var restLog []LogEntry
		rf.log, restLog = rf.log[:args.PrevLogIndex-baseIndex+1], rf.log[args.PrevLogIndex-baseIndex+1:]
		if rf.hasConflictLog(restLog, args.Entries) || len(restLog) < len(args.Entries) {
			rf.log = append(rf.log, args.Entries...)
		} else {
			rf.log = append(rf.log, restLog...)
		}

		reply.Success = true
		reply.NextTryIndex = args.PrevLogIndex + len(args.Entries)

		if args.LeaderCommit > rf.commitIndex {
			if rf.getLastLogIndex() < args.LeaderCommit {
				rf.commitIndex = rf.getLastLogIndex()
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			go rf.commitLog()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer rf.panicHandler()
	if !ok || rf.state != STATE_LEADER || args.Term != rf.currentTerm {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
		//rf.persist()
		return ok
	}
	if reply.Success {
		if len(args.Entries) > 0 {
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	} else {
		rf.nextIndex[server] = reply.NextTryIndex
	}

	baseIndex := rf.log[0].Index

	for N := rf.getLastLogIndex(); N > rf.commitIndex && rf.log[N-baseIndex].Term == rf.currentTerm; N-- {
		// find if there exists an N to update commitIndex
		count := 1

		if rf.log[N-baseIndex].Term == rf.currentTerm {
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
		}

		if count > len(rf.peers)/2 {
			if rf.commitIndex < N {
				rf.commitIndex = N
				go rf.commitLog()
			}
			break
		}
	}

	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.panicHandler()
	if rf.state != STATE_LEADER {
		rf.logger.Panic("invalid state??")
	}
	baseIndex := rf.log[0].Index

	for server := range rf.peers {
		if server != rf.me && rf.state == STATE_LEADER {
			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[server] - 1
			if args.PrevLogIndex >= baseIndex {
				args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].Term
			}
			if rf.nextIndex[server] <= rf.getLastLogIndex() {
				rf.logger.WithFields(logrus.Fields{
					"nextindex": rf.nextIndex[server],
					"baseIndex": baseIndex,
				}).Debug("broadcast: entries diff:")
				if rf.nextIndex[server] < baseIndex {
					rf.logger.Warn("nextindex < baseIndex, skipping")
					continue
				}
				args.Entries = rf.log[rf.nextIndex[server]-baseIndex:]
			}
			args.LeaderCommit = rf.commitIndex

			go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
		}
	}
}
