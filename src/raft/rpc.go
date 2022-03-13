package raft

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		// reject request with stale term number
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		// become follower and update current term
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		// vote for the candidate
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.chanGrantVote <- true
	}
}

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
	if args.PrevLogIndex > 0 && args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		term := rf.log[args.PrevLogIndex].Term
		for reply.NextTryIndex = args.PrevLogIndex - 1; reply.NextTryIndex >= 0 && rf.log[reply.NextTryIndex].Term == term; reply.NextTryIndex-- {
		}
		reply.NextTryIndex++
	} else {
		var restLog []LogEntry
		rf.log, restLog = rf.log[:args.PrevLogIndex+1], rf.log[args.PrevLogIndex+1:]

		if rf.hasConflictLog(args.Entries, restLog) || len(restLog) < len(args.Entries) {
			rf.log = append(rf.log, args.Entries...)
		} else {
			rf.log = append(rf.log, restLog...)
		}

		reply.Success = true
		reply.NextTryIndex = args.PrevLogIndex

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

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
