package raft

import "github.com/sirupsen/logrus"

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.panicHandler()
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
				if rf.nextIndex[server]-baseIndex < 0 {
					rf.logger.Panic("what to do here?")
				}
				args.Entries = rf.log[rf.nextIndex[server]-baseIndex:]
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
