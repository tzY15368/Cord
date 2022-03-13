package raft

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me && rf.state == STATE_LEADER {
			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[server] - 1
			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			}
			if rf.nextIndex[server] <= rf.getLastLogIndex() {
				args.Entries = rf.log[rf.nextIndex[server]:]
			}
			args.LeaderCommit = rf.commitIndex

			go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	me := rf.me
	state := rf.state
	rf.mu.Unlock()

	for server := range rf.peers {
		if server != me && state == STATE_CANDIDATE {
			go rf.sendRequestVote(server, args, &RequestVoteReply{})
		}
	}
}
