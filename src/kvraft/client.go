package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/common"
	"6.824/labrpc"
	"6.824/logging"
	"github.com/sirupsen/logrus"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	logger    *logrus.Logger
	leader    int32
	clientID  int64
	requestID int64
}

func (ck *Clerk) getLeader() int {
	return int(atomic.LoadInt32(&ck.leader))
}
func (ck *Clerk) setLeader(id int) {
	atomic.StoreInt32(&ck.leader, int32(id))
	ck.logger.WithField("id", id).Info("leader redirect")
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.logger = logging.GetLogger("kv", common.KVServerLogLevel)
	// starts with leader = 0 by default
	ck.leader = 0
	ck.requestID = 0
	ck.clientID = nrand()
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{
		Key: key,
		RequestInfo: common.RequestInfo{
			ClientID:  ck.clientID,
			RequestID: atomic.AddInt64(&ck.requestID, 1),
		},
	}
	reply := &GetReply{}
	leader := ck.getLeader()
	for i := 0; true; i++ {
		_leaderID := (leader + i) % len(ck.servers)
		ck.logger.WithField("target", _leaderID).Info("clerk: send get")
		ok := ck.servers[_leaderID].Call("KVServer.Get", args, reply)
		if !ok {
			ck.logger.WithField("id", leader).Warn("clerk: get: unreachable kvserver, incrmenting")
			continue
		}
		ck.logger.WithFields(logrus.Fields{
			"id":    _leaderID,
			"args":  fmt.Sprintf("%+v", args),
			"reply": fmt.Sprintf("%+v", reply),
		}).Info("clerk: GET: got reply")
		switch {
		case string(reply.Err) == ErrOK.Error():
			ck.setLeader(_leaderID)
			return reply.Value
		case string(reply.Err) == ErrWrongLeader.Error():
			ck.logger.Debug("clerk: wrong leader")
		case string(reply.Err) == ErrKeyNotFound.Error():
			ck.setLeader(_leaderID)
			return ""
		case string(reply.Err) == ErrUnexpected.Error():
			ck.logger.WithField("leader", _leaderID).Panic("err unexpected")
		case string(reply.Err) == ErrTimeout.Error():
			ck.logger.WithField("leader", _leaderID).Warn("no agreement")
		default:
			ck.logger.Panic("default")
		}

		// if i != 0 && i%len(ck.servers) != 0 {
		// 	ck.logger.Debug("clerk: no leaders, sleeping 500ms")
		// 	time.Sleep(500 * time.Millisecond)
		// }
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	start := time.Now()
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		RequestInfo: common.RequestInfo{
			ClientID:  ck.clientID,
			RequestID: atomic.AddInt64(&ck.requestID, 1),
		},
	}
	reply := &PutAppendReply{}
	leader := ck.getLeader()
	for i := 0; true; i++ {
		_leaderID := (leader + i) % len(ck.servers)
		ck.logger.WithFields(logrus.Fields{
			"target": _leaderID,
			"key":    key,
			"value":  value,
		}).Info("clerk: send putappend")
		ok := ck.servers[_leaderID].Call("KVServer.PutAppend", args, reply)
		if !ok {
			ck.logger.WithField("id", leader).Warn("clerk: get: unreachable kvserver, incrmenting")
			continue
		}
		ck.logger.WithFields(logrus.Fields{
			"id":    _leaderID,
			"args":  fmt.Sprintf("%+v", args),
			"reply": fmt.Sprintf("%+v", reply),
		}).Info("clerk: PUTAPPEND: got reply")
		switch {
		case string(reply.Err) == ErrOK.Error():
			ck.setLeader(_leaderID)
			ck.logger.WithField("time", time.Since(start)).Debug("clerk: putappend took time")
			return
		case string(reply.Err) == ErrWrongLeader.Error():
			ck.logger.Debug("clerk: wrong leader")
		case string(reply.Err) == ErrKeyNotFound.Error():
			ck.logger.Panic("invalid reply: errrkeynotfound")
			// this should never happen for putappend
			return
		case string(reply.Err) == ErrUnexpected.Error():
			ck.logger.WithField("leader", _leaderID).Panic("err unexpected")
		case string(reply.Err) == ErrTimeout.Error():
			ck.logger.WithField("leader", _leaderID).Warn("no agreement")
		default:
			ck.logger.Panic("default")
		}
		// if i != 0 && i%len(ck.servers) != 0 {
		// 	ck.logger.Debug("clerk: no leaders, sleeping 500ms")
		// 	time.Sleep(500 * time.Millisecond)
		// }
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "PUT")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "APPEND")
}
