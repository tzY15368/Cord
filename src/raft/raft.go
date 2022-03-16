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

	snapshot          []byte
	lastIncludedTerm  int
	lastIncludedIndex int
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
		// go rf.broadcastAppendEntries()
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

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		diff := time.Millisecond * time.Duration(rand.Intn(200)+300)
		switch state {
		case STATE_FOLLOWER:
			select {
			case <-rf.chanGrantVote:
			case <-rf.chanHeartbeat:
			case <-time.After(diff):
				rf.mu.Lock()
				rf.logger.WithField("now", time.Now()).WithField("diff", diff).Warn("heartbeat timeout")
				rf.state = STATE_CANDIDATE
				rf.mu.Unlock()
			}
		case STATE_LEADER:
			go rf.broadcastAppendEntries()
			time.Sleep(hearbeatInterval)
		case STATE_CANDIDATE:
			rf.mu.Lock()
			rf.currentTerm++
			rf.logger.WithField("term", rf.currentTerm).Info("election: became candidate")
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
				rf.mu.Lock()
				rf.logger.WithField("term", rf.currentTerm).Info("election: became leader")
				// rf.nextIndex = make([]int, len(rf.peers))
				// rf.matchIndex = make([]int, len(rf.peers))
				nextIndex := rf.getLastLogIndex() + 1
				for i := range rf.nextIndex {
					rf.nextIndex[i] = nextIndex
				}
				rf.mu.Unlock()
			case <-time.After(electionTimeout):
				rf.mu.Lock()
				rf.logger.WithField("term", rf.currentTerm).Info("election: timeout")
				rf.mu.Unlock()
				additionalSleepTime := time.Duration(rand.Intn(30)) * time.Millisecond
				time.Sleep(additionalSleepTime)
				rf.logger.WithField("sleepTime", additionalSleepTime).Warn("election timeout, extra sleep")
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
	rf.readPersist()

	go rf.ticker()

	return rf
}
