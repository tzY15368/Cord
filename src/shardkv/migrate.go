package shardkv

import (
	"fmt"
	"time"

	"6.824/shardctrler"
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

func (kv *ShardKV) doMigrate(delta *shardctrler.DiffCfg, newCfg *shardctrler.Config) {
	outGoingShards := delta.FromMe(kv.gid)
	for shardKey, targetGID := range outGoingShards {
		servers := newCfg.Groups[targetGID]
		// 一整个组，需要找到leader（带缓存）
		for _, server := range servers {
			go func(svName string) {
				args := MigrateArgs{
					Data:  make(map[string]string),
					Shard: shardKey,
				}
				kv.mu.Lock()
				for key := range kv.data {
					if key2shard(key) == shardKey {
						args.Data[key] = kv.data[key]
					}
				}
				kv.mu.Unlock()
				reply := MigrateReply{}
				kv.logger.WithField("len", len(args.Data)).
					WithField("shard", shardKey).Debug("svCFG: migrate: moving length on key")
				ok := kv.make_end(server).Call("ShardKV.Migrate", &args, &reply)
			}(server)
		}
	}

	expectedIncomingShards := delta.ToMe(kv.gid)
	for len(expectedIncomingShards) != 0 {
		select {
		case <-time.After(1 * time.Second):
			kv.logger.Panic("op_migrate timeout")
			// block infinitely, no timeouts
		}
	}
}
