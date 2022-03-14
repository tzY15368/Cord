package raft

import (
	"math/rand"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"github.com/sirupsen/logrus"
)

// import "bytes"
// import "labgob"

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	//mu        sync.Mutex          // Lock to protect shared access to this peer's state
	mu        *mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state     int
	voteCount int

	// Persistent state on all servers.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers.
	commitIndex int
	lastApplied int

	// Volatile state on leaders.
	nextIndex  []int
	matchIndex []int

	// Channels between raft peers.
	chanApply     chan ApplyMsg
	chanGrantVote chan bool
	chanWinElect  chan bool
	chanHeartbeat chan bool

	// misc
	isKilled      int32
	logger        *logrus.Entry
	applyMsgQueue *msgQueue
	// snapshot
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := (rf.state == STATE_LEADER)
	return term, isleader
}

func (rf *Raft) isUpToDate(candidateTerm int, candidateIndex int) bool {
	term, index := rf.getLastLogTerm(), rf.getLastLogIndex()
	return candidateTerm > term || (candidateTerm == term && candidateIndex >= index)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if ok {
		if rf.state != STATE_CANDIDATE || rf.currentTerm != args.Term {
			// invalid request
			rf.logger.Warn("rpc: invalid request")
			return ok
		}

		if rf.currentTerm < reply.Term {
			// revert to follower state and update current term
			rf.state = STATE_FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, index := 0, 0
	isLeader := (rf.state == STATE_LEADER)

	// TODO(problem): leader doesn't have data log
	if isLeader {
		term = rf.currentTerm
		index = rf.getLastLogIndex() + 1
		rf.logger.WithField("command", command).Info("start: got command")
		rf.log = append(rf.log, LogEntry{Index: index, Term: term, Command: command})
		go rf.broadcastAppendEntries()
	}

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	// Your code here, if desired.
	atomic.StoreInt32(&rf.isKilled, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.isKilled) == 1
}

// func (rf *Raft) followerTicker() {
// RestartFollower:
// 	<-rf.becomeFollower
// 	for !rf.killed() {
// 		select {
// 		case <-rf.chanGrantVote:
// 		case <-rf.chanHeartbeat:
// 		case <-time.After(time.Millisecond * time.Duration(rand.Intn(200)+300)):
// 			rf.mu.Lock()
// 			rf.logger.Warn("heartbeat timeout")
// 			rf.state = STATE_CANDIDATE
// 			rf.becomeCandidate <- struct{}{}
// 			rf.mu.Unlock()
// 			goto RestartFollower
// 		}
// 	}
// }

// func (rf *Raft) candidateTicker() {
// RestartCandidate:
// 	<-rf.becomeCandidate
// 	rf.mu.Lock()
// 	rf.currentTerm++
// 	rf.votedFor = rf.me
// 	rf.voteCount = 1
// 	rf.persist()
// 	rf.mu.Unlock()
// 	go rf.broadcastRequestVote()
// 	for !rf.killed() {
// 		select {

// 		case <-rf.chanHeartbeat:
// 			rf.mu.Lock()
// 			rf.state = STATE_FOLLOWER
// 			rf.mu.Unlock()
// 			rf.becomeFollower <- struct{}{}
// 			goto RestartCandidate
// 		case <-rf.chanWinElect:
// 			rf.logger.Info("election: became leader")
// 			rf.mu.Lock()
// 			rf.state = STATE_LEADER
// 			rf.becomeLeader <- struct{}{}
// 			nextIndex := rf.getLastLogIndex() + 1
// 			for i := range rf.nextIndex {
// 				rf.nextIndex[i] = nextIndex
// 			}
// 			rf.mu.Unlock()
// 			goto RestartCandidate
// 		case <-time.After(electionTimeout):
// 			rf.becomeCandidate <- struct{}{}
// 			goto RestartCandidate
// 		}
// 	}
// }

// func (rf *Raft) leaderTicker() {
// 	for range rf.becomeLeader {
// 		rf.mu.Lock()
// 		state := rf.state
// 		rf.mu.Unlock()
// 		for !rf.killed() && state == STATE_LEADER {
// 			go rf.broadcastAppendEntries()
// 			time.Sleep(hearbeatInterval)
// 		}
// 	}
// }

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case STATE_FOLLOWER:
			select {
			case <-rf.chanGrantVote:
			case <-rf.chanHeartbeat:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(200)+300)):
				rf.mu.Lock()
				rf.logger.Warn("heartbeat timeout")
				rf.state = STATE_CANDIDATE
				rf.mu.Unlock()
			}
		case STATE_LEADER:
			go rf.broadcastAppendEntries()
			time.Sleep(hearbeatInterval)
		case STATE_CANDIDATE:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.persist()
			rf.mu.Unlock()
			go rf.broadcastRequestVote()

			select {
			case <-rf.chanHeartbeat:
				rf.mu.Lock()
				rf.state = STATE_FOLLOWER
				rf.mu.Unlock()
			case <-rf.chanWinElect:
				rf.logger.Info("election: became leader")
				rf.mu.Lock()
				// rf.nextIndex = make([]int, len(rf.peers))
				// rf.matchIndex = make([]int, len(rf.peers))
				nextIndex := rf.getLastLogIndex() + 1
				for i := range rf.nextIndex {
					rf.nextIndex[i] = nextIndex
				}
				rf.mu.Unlock()
			case <-time.After(electionTimeout):
			}
		}
		//time.Sleep(30 * time.Millisecond)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = makeLock(rf)
	// Your initialization code here (2A, 2B, 2C).
	rf.state = STATE_FOLLOWER
	rf.voteCount = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.chanApply = applyCh
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanWinElect = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.logger = logrus.WithField("id", rf.me)
	rf.applyMsgQueue = NewQueue(rf.chanApply)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
