package raft

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync/atomic"
	"time"

	"6.824/common"
	"6.824/labrpc"
	"6.824/logging"
	"6.824/proto"
	"github.com/sirupsen/logrus"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	//mu        sync.Mutex          // Lock to protect shared access to this peer's state
	mu        *mutex
	peers     []Callable   // RPC end points of all peers
	persister IPersistable // Object to hold this peer's persisted state
	me        int          // this peer's index into peers[]
	useLABRPC bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state     int
	voteCount int

	// Persistent state on all servers.
	pState *proto.PersistedState
	// currentTerm       int
	// votedFor          int
	// log               []*proto.LogEntry
	// lastIncludedTerm  int
	// lastIncludedIndex int

	// Volatile state on all servers.
	commitIndex int
	lastApplied int
	leaderID    int

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

	snapshot []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := int(rf.pState.CurrentTerm)
	isleader := (rf.state == STATE_LEADER)
	return term, isleader
}

func (rf *Raft) GetStateSize() int {
	size := rf.persister.RaftStateSize()
	return size
}

// start returns index, term, isLeader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()

	term, index := 0, 0
	isLeader := (rf.state == STATE_LEADER)
	if isLeader {
		term = int(rf.pState.CurrentTerm)
		index = rf.getLastLogIndex() + 1
		rf.logger.WithField("command", command).WithField("at", time.Now()).Info("start: got command")
		rf.pState.Log = append(rf.pState.Log, &proto.LogEntry{
			Index:   int64(index),
			Term:    int64(term),
			Command: common.EncodeCommand(command),
		})
		go rf.broadcastAppendEntries()
		rf.mu.Unlock()
	} else {
		leader := rf.leaderID
		rf.mu.Unlock()
		if rf.useLABRPC {
			goto Return
		} else {
			fmt.Println("isn't leader, consulting", rf.leaderID)
		}
		reply := StartReply{}
		if leader == rf.me {
			goto Return
		}
		ok := rf.peers[leader].Call("Raft.Start", command, &reply)
		if !ok {
			fmt.Println("consult fail (net)")
			return -1, -1, false
		}
		return reply.Index, reply.Term, reply.IsLeader

	}
Return:
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	// Your code here, if desired.
	atomic.StoreInt32(&rf.isKilled, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.isKilled) == 1
}

func (rf *Raft) SetGID(gid int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if gid == -1 {
		rf.logger = logging.GetLogger("ctlrf", logrus.InfoLevel).WithField("id", rf.me)
	} else {
		rf.logger = rf.logger.WithField("id", fmt.Sprintf("%d-%d", gid, rf.me))
	}
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
			rf.pState.CurrentTerm++
			rf.logger.WithField("term", rf.pState.CurrentTerm).Info("election: became candidate")
			rf.pState.VotedFor = int64(rf.me)
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
				rf.logger.WithFields(logrus.Fields{
					"term": rf.pState.CurrentTerm,
					"at":   time.Now(),
				}).Info("election: became leader")
				// rf.nextIndex = make([]int, len(rf.peers))
				// rf.matchIndex = make([]int, len(rf.peers))
				nextIndex := rf.getLastLogIndex() + 1
				for i := range rf.nextIndex {
					rf.nextIndex[i] = nextIndex
				}
				rf.mu.Unlock()
			case <-time.After(electionTimeout):
				rf.mu.Lock()
				rf.logger.WithField("term", rf.pState.CurrentTerm).Info("election: timeout")
				rf.mu.Unlock()
				additionalSleepTime := time.Duration(rand.Intn(30)) * time.Millisecond
				time.Sleep(additionalSleepTime)
				rf.logger.WithField("sleepTime", additionalSleepTime).Warn("election timeout, extra sleep")
			}
		}
		//time.Sleep(30 * time.Millisecond)
	}
}

type Callable interface {
	Call(string, interface{}, interface{}) bool
}

func Make(ipeers interface{}, me int,
	persister IPersistable, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	peers, ok := ipeers.([]*labrpc.ClientEnd)
	if ok {
		fmt.Println("using labrpc")
		rf.useLABRPC = true
		iends := make([]Callable, len(peers))
		for i := range iends {
			iends[i] = peers[i]
		}
		rf.peers = iends
	}
	peers2, ok2 := ipeers.([]*GRPCClient)
	if ok2 {
		fmt.Println("using grpc")
		iclis := make([]Callable, len(peers2))
		for i := range peers2 {
			if i != me && peers2[i] == nil {
				panic("nil client")
			}
			iclis[i] = peers2[i]
		}
		rf.peers = iclis
	}

	if len(rf.peers) == 0 {
		panic("startup failed" + "type:" + fmt.Sprintf("%+v", reflect.TypeOf(ipeers)))
	}

	rf.persister = persister
	rf.me = me
	rf.mu = makeLock(rf)
	// Your initialization code here (2A, 2B, 2C).
	rf.state = STATE_FOLLOWER
	rf.voteCount = 0
	rf.pState = &proto.PersistedState{
		CurrentTerm:       0,
		VotedFor:          -1,
		Log:               []*proto.LogEntry{{Term: 0}},
		LastIncludedTerm:  0,
		LastIncludedIndex: 0,
	}
	// rf.pState.CurrentTerm = 0
	// rf.pState.VotedFor = -1
	// rf.pState.Log = append(rf.pState.Log, &proto.LogEntry{Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.chanApply = applyCh
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanWinElect = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	_logger := logging.GetLogger("raft", common.RaftLogLevel)

	rf.logger = _logger.WithField("id", rf.me)
	rf.applyMsgQueue = NewQueue(rf.chanApply)
	// initialize from state persisted before a crash
	rf.readPersist()

	go rf.ticker()

	return rf
}
