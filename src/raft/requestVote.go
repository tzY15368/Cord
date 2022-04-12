package raft

import (
	"fmt"

	"6.824/proto"
	"github.com/sirupsen/logrus"
)

// isUPToDate not thread safe
func (rf *Raft) isUpToDate(candidateTerm int, candidateIndex int) bool {
	term, index := rf.getLastLogTerm(), rf.getLastLogIndex()
	return candidateTerm > term || (candidateTerm == term && candidateIndex >= index)
}
func (rf *Raft) sendRequestVote(server int, args *proto.RequestVoteArgs, reply *proto.RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if ok {
		if rf.state != STATE_CANDIDATE || rf.currentTerm != int(args.Term) {
			// invalid request
			rf.logger.Warn("rpc: invalid request")
			return ok
		}

		if rf.currentTerm < int(reply.Term) {
			// revert to follower state and update current term
			rf.state = STATE_FOLLOWER
			rf.currentTerm = int(reply.Term)
			rf.votedFor = -1
			rf.logger.WithField("newTerm", rf.currentTerm).Info("appendEntry: became follower due to higher client term")
		}

		if reply.VoteGranted {
			rf.voteCount++
			if rf.voteCount > len(rf.peers)/2 {
				// win the election and become leader
				rf.state = STATE_LEADER
				rf.chanWinElect <- true
			}
		}
	}

	return ok
}

func (rf *Raft) RequestVote(args *proto.RequestVoteArgs, reply *proto.RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if int(args.Term) < rf.currentTerm {
		// reject request with stale term number
		reply.Term = int64(rf.currentTerm)
		reply.VoteGranted = false
		return
	}

	if int(args.Term) > rf.currentTerm {
		// become follower and update current term
		rf.state = STATE_FOLLOWER
		rf.currentTerm = int(args.Term)
		rf.votedFor = -1
		rf.logger.WithField("newTerm", rf.currentTerm).Info("appendEntry: became follower due to higher client term")
	}

	reply.Term = int64(rf.currentTerm)
	reply.VoteGranted = false

	logIsUpToDate := rf.isUpToDate(int(args.LastLogTerm), int(args.LastLogIndex))
	if (rf.votedFor == -1 || rf.votedFor == int(args.CandidateID)) && logIsUpToDate {
		// vote for the candidate
		rf.votedFor = int(args.CandidateID)
		reply.VoteGranted = true
		rf.chanGrantVote <- true
	}
	rf.logger.WithFields(logrus.Fields{
		"voteGranted":  reply.VoteGranted,
		"votedFor":     rf.votedFor,
		"args":         fmt.Sprintf("%+v", args),
		"lastLogTerm":  rf.getLastLogTerm(),
		"lastLogIndex": rf.getLastLogIndex(),
	}).Debug("vote status")
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	args := &proto.RequestVoteArgs{
		Term:         int64(rf.currentTerm),
		CandidateID:  int64(rf.me),
		LastLogIndex: int64(rf.getLastLogIndex()),
		LastLogTerm:  int64(rf.getLastLogTerm()),
	}
	me := rf.me
	state := rf.state
	rf.mu.Unlock()

	for server := range rf.peers {
		if server != me && state == STATE_CANDIDATE {
			go rf.sendRequestVote(server, args, &proto.RequestVoteReply{})
		}
	}
}
