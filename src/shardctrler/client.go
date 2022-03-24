package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.824/common"
	"6.824/labrpc"
	"6.824/logging"
	"github.com/sirupsen/logrus"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leader    int32
	mu        sync.Mutex
	clientID  int64
	requestID int64
	logger    *logrus.Entry
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
	ck.clientID = nrand()
	ck.requestID = 0
	ck.leader = 0
	ck.logger = logging.GetLogger("sc", common.ShardCtlLogLevel).WithField("id", ck.clientID)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num: num,
		RequestInfo: common.RequestInfo{
			ClientID:  ck.clientID,
			RequestID: atomic.AddInt64(&ck.requestID, 1),
		},
	}
	for {
		leader := atomic.LoadInt32(&ck.leader)
		reply := &QueryReply{}
		ok := ck.servers[leader].Call("ShardCtrler.Query", args, &reply)
		if ok && !reply.WrongLeader {
			return reply.Config
		} else {
			ck.logger.WithField("id", leader).Warn("ck: query: unreachable server")
			atomic.StoreInt32(&ck.leader, (leader+1)%int32(len(ck.servers)))
			if leader == 0 {
				ck.logger.Warn("no leaders, sleeping 100ms")
				time.Sleep(100 * time.Millisecond)
			}
			continue
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers: servers,
		RequestInfo: common.RequestInfo{
			ClientID:  ck.clientID,
			RequestID: atomic.AddInt64(&ck.requestID, 1),
		},
	}
	for {
		leader := atomic.LoadInt32(&ck.leader)
		reply := &JoinReply{}
		ok := ck.servers[leader].Call("ShardCtrler.Join", args, &reply)
		if ok && !reply.WrongLeader {
			return
		} else {
			ck.logger.WithField("id", leader).Warn("ck: join: unreachable server")
			atomic.StoreInt32(&ck.leader, (leader+1)%int32(len(ck.servers)))
			if leader == 0 {
				ck.logger.Warn("no leaders, sleeping 100ms")
				time.Sleep(100 * time.Millisecond)
			}
			continue
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs: gids,
		RequestInfo: common.RequestInfo{
			ClientID:  ck.clientID,
			RequestID: atomic.AddInt64(&ck.requestID, 1),
		},
	}
	for {
		leader := atomic.LoadInt32(&ck.leader)
		reply := &LeaveReply{}
		ok := ck.servers[leader].Call("ShardCtrler.Leave", args, &reply)
		if ok && !reply.WrongLeader {
			return
		} else {
			ck.logger.WithField("id", leader).Warn("ck: leave: unreachable server")
			atomic.StoreInt32(&ck.leader, (leader+1)%int32(len(ck.servers)))
			if leader == 0 {
				ck.logger.Warn("no leaders, sleeping 100ms")
				time.Sleep(100 * time.Millisecond)
			}
			continue
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard: shard,
		GID:   gid,
		RequestInfo: common.RequestInfo{
			ClientID:  ck.clientID,
			RequestID: atomic.AddInt64(&ck.requestID, 1),
		},
	}
	for {
		leader := atomic.LoadInt32(&ck.leader)
		reply := &MoveReply{}
		ok := ck.servers[leader].Call("ShardCtrler.Move", args, &reply)
		if ok && !reply.WrongLeader {
			return
		} else {
			ck.logger.WithField("id", leader).Warn("ck: move: unreachable server")
			atomic.StoreInt32(&ck.leader, (leader+1)%int32(len(ck.servers)))
			if leader == 0 {
				ck.logger.Warn("no leaders, sleeping 100ms")
				time.Sleep(100 * time.Millisecond)
			}
			continue
		}
	}
}
