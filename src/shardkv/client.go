package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/common"
	"6.824/labrpc"
	"6.824/logging"
	"6.824/shardctrler"
	"github.com/sirupsen/logrus"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//

type Clerk struct {
	sm        *shardctrler.Clerk
	config    shardctrler.Config
	make_end  func(string) *labrpc.ClientEnd
	clientID  int64
	requestID int64
	// map gid to actual leader servername offset in group
	groupLeader map[int]int
	logger      *logrus.Entry
	mu          sync.Mutex
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.clientID = nrand()
	ck.requestID = 0
	ck.groupLeader = make(map[int]int)
	ck.logger = logging.GetLogger("skv", common.ShardKVLogLevel).WithField("id", ck.clientID)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
		RequestInfo: common.RequestInfo{
			ClientID:  ck.clientID,
			RequestID: atomic.AddInt64(&ck.requestID, 1),
		},
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for {
				ck.mu.Lock()
				var serverName string
				if serverIdx, ok2 := ck.groupLeader[gid]; ok2 {
					serverName = servers[serverIdx]
				} else {
					ck.groupLeader[gid] = 0
					serverName = servers[0]
				}
				ck.mu.Unlock()
				sv := ck.make_end(serverName)
				reply := GetReply{}
				ok := sv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				} else {
					ck.logger.WithField("err", reply.Err).Debug("sck: get: got error")
					if reply.Err == ErrWrongLeader {
						ck.mu.Lock()
						ck.groupLeader[gid] = (ck.groupLeader[gid] + 1) % len(servers)
						ck.mu.Unlock()
						continue
					}
					if reply.Err == ErrWrongGroup {
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.mu.Lock()
		ck.config = ck.sm.Query(-1)
		ck.logger.WithField("config", ck.config).Debug("sck: get: got new config")
		ck.mu.Unlock()
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		RequestInfo: common.RequestInfo{
			ClientID:  ck.clientID,
			RequestID: atomic.AddInt64(&ck.requestID, 1),
		},
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for {
				ck.mu.Lock()
				var svName string
				if svIdx, ok2 := ck.groupLeader[gid]; ok2 {
					svName = servers[svIdx]
				} else {
					ck.groupLeader[gid] = 0
					svName = servers[0]
				}
				ck.mu.Unlock()
				reply := PutAppendReply{}
				ok := ck.make_end(svName).Call("ShardKV.PutAppend", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return
				} else {
					ck.logger.WithField("err", reply.Err).Debug("sck: putappend: got error")
					if reply.Err == ErrWrongLeader {
						ck.mu.Lock()
						ck.groupLeader[gid] = (ck.groupLeader[gid] + 1) % len(servers)
						ck.mu.Unlock()
						continue
					}
					if reply.Err == ErrWrongGroup {
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.mu.Lock()
		ck.config = ck.sm.Query(-1)
		ck.logger.WithField("config", ck.config).Debug("sck: putappend: got new config")
		ck.mu.Unlock()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
