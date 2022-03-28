package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/common"
	"6.824/labgob"
	"6.824/shardctrler"
	"github.com/sirupsen/logrus"
)

// sendMigrateRPC thread safe, will block
func (kv *ShardKV) sendMigrateRPC(groupServers []string, args *MigrateArgs, reply *MigrateReply) {
	// 没做groupLeader缓存
	for {
		for _, server := range groupServers {
		Retry:
			ok := kv.make_end(server).Call("ShardKV.Migrate", args, reply)
			if ok && reply.Err == OK {
				return
				// TODO: HOW DOES THIS WORK?
			} else if reply.Err == ErrKeyNoLock {
				kv.logger.Debug("skv: sendMigrate: error no lock, sleeping 75ms")
				time.Sleep(pollCFGInterval)
				goto Retry
			} else {
				kv.logger.WithField("err", reply.Err).Debug("skv: sendMigrateRPC: reply=false")
			}
		}
		kv.logger.WithField("reply", fmt.Sprintf("%+v", reply)).Debug("skv: sendMigrateRPC: round failed, restarting after 100ms")
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) handleTransfer(pullTarget map[int]int, newCfg shardctrler.Config) {
	// 如果对方config没那么新， 会无限重试，不要紧
	var wg sync.WaitGroup
	newData := make(map[string]string)
	for shardKey, targetGID := range pullTarget {
		args := MigrateArgs{
			Shard:     shardKey,
			ConfigNum: newCfg.Num,
		}
		wg.Add(1)
		go func(args *MigrateArgs, gid int) {
			servers := newCfg.Groups[gid]
			reply := &MigrateReply{Data: make(map[string]string)}
			kv.sendMigrateRPC(servers, args, reply)
			for key := range reply.Data {
				newData[key] = reply.Data[key]
			}
			wg.Done()
		}(&args, targetGID)
	}
	wg.Wait()

	kv.logger.WithFields(logrus.Fields{
		"pullTargets": pullTarget,
		"len":         len(newData),
	}).Debug("svCFG: migrate: pull ok")

	// ...pull ok

	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err := encoder.Encode(&newData)
	if err != nil {
		panic(err)
	}

	op := Op{
		OP_TYPE:  OP_TRANSFER,
		OP_KEY:   newCfg.Dump(),
		OP_VALUE: buf.String(),
		RequestInfo: common.RequestInfo{
			ClientID:  kv.clientID,
			RequestID: atomic.AddInt64(&kv.requestID, 1),
		},
	}
	reply := internalReply{}
	kv.proposeAndApply(op, &reply)
	kv.logger.WithFields(logrus.Fields{
		"reply": reply,
	}).Debug("svCFG: handleTransfer: proposed")
}

//
// transferCFGIndex决定version[shard]可能的最大值
// kv.config.num决定transferCFGIndex可能的最大值
// server impl：如果shard的cfgversion小于当前transferCFGIndex：返回ErrReconfigure；
// 如果等于，用key2shard查自己到底现在serve不serve这个key

func (kv *ShardKV) evalTransferOP(op *Op) opResult {
	cfg := shardctrler.LoadCFG(op.OP_KEY)

}
