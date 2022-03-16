package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"6.824/labrpc"
	"github.com/sirupsen/logrus"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	logger *logrus.Entry
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
	ck.logger = logrus.WithField("clerk", "clerk")
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
Retry:
	leader := ck.getLeader()
	i := 0
	defer ck.setLeader((leader + i) % len(ck.servers))
	for ok := ck.servers[leader+i].Call("KVServer.PutAppend", args, reply); i < len(ck.servers); i++ {
		if !ok {
			ck.logger.WithField("id", leader).Warn("clerk: get: unreachable kvserver")
			continue
		}
		switch reply.Err {
		case OK:
			return reply.Value
		case ErrWrongLeader:
			continue
		case ErrNoKey:
			return ""
		case ErrUnexpected:
			ck.logger.Panic("clerk: unexpected failure")
		case ErrTimeout:
			ck.logger.WithField("serverid", leader+i).Panic("clerk: server timeout")
		}
	}
	ck.logger.Warn("clerk: tried all servers, new round of retry")
	goto Retry
	// You will have to modify this function.
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
Retry:
	leader := ck.getLeader()
	i := 0
	defer ck.setLeader((leader + i) % len(ck.servers))
	for ok := ck.servers[leader+i].Call("KVServer.PutAppend", args, reply); i < len(ck.servers); i++ {
		if !ok {
			ck.logger.WithField("id", leader).Warn("clerk: get: unreachable kvserver, incrmenting")
			continue
		}
		switch reply.Err {
		case OK:
			return
		case ErrWrongLeader:
			continue
		case ErrNoKey:
			return
		case ErrUnexpected:
			ck.logger.Panic("clerk: unexpected failure")
		case ErrTimeout:
			ck.logger.WithField("serverid", leader+i).Panic("clerk: server timeout")
		}
	}
	ck.logger.Warn("clerk: tried all servers")
	goto Retry
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
