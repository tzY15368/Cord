package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/logging"
	"github.com/sirupsen/logrus"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	logger *logrus.Logger
	leader int32
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
	ck.logger = logging.GetLogger("kv", logrus.DebugLevel)
	// starts with leader = 0 by default
	ck.leader = 0
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
	args := &GetArgs{Key: key}
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
		}).Info("got reply")
		switch {
		case reply.Err == OK:
			ck.setLeader(_leaderID)
			return reply.Value
		case reply.Err == ErrWrongLeader:
			ck.logger.Debug("wrong leader")
		case reply.Err == ErrNoKey:
			ck.setLeader(_leaderID)
			return ""
		case reply.Err == ErrUnexpected:
			ck.logger.WithField("leader", _leaderID).Panic("err unexpected")
		case reply.Err == ErrTimeout:
			ck.logger.WithField("leader", _leaderID).Panic("no agreement")
		}
		if i != 0 && i%len(ck.servers) == 0 {
			ck.logger.Warn("no leaders found, waiting 500ms")
			time.Sleep(500 * time.Millisecond)
		}
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
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	reply := &PutAppendReply{}
	leader := ck.getLeader()
	for i := 0; true; i++ {
		_leaderID := (leader + i) % len(ck.servers)
		ck.logger.WithField("target", _leaderID).Info("clerk: send putappend")
		ok := ck.servers[_leaderID].Call("KVServer.PutAppend", args, reply)
		if !ok {
			ck.logger.WithField("id", leader).Warn("clerk: get: unreachable kvserver, incrmenting")
			continue
		}
		ck.logger.WithFields(logrus.Fields{
			"id":    _leaderID,
			"args":  fmt.Sprintf("%+v", args),
			"reply": fmt.Sprintf("%+v", reply),
		}).Info("got reply")
		switch {
		case reply.Err == OK:
			ck.setLeader(_leaderID)
			return
		case reply.Err == ErrWrongLeader:
			ck.logger.Debug("wrong leader")
		case reply.Err == ErrNoKey:
			ck.setLeader(_leaderID)
			return
		case reply.Err == ErrUnexpected:
			ck.logger.WithField("leader", _leaderID).Panic("err unexpected")
		case reply.Err == ErrTimeout:
			ck.logger.WithField("leader", _leaderID).Panic("no agreement")
		}
		if i != 0 && i%len(ck.servers) == 0 {
			ck.logger.Warn("no leaders found, waiting 500ms")
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "PUT")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "APPEND")
}
