package shardkv

import (
	"fmt"
	"sync"
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
	kv.logger.WithField("outboundMap", outGoingShards).Debug("svCFG: migrate: sending migrates")
	var wg sync.WaitGroup
	for shardKey, targetGID := range outGoingShards {
		servers := newCfg.Groups[targetGID]
		args := MigrateArgs{
			Data:      make(map[string]string),
			Shard:     shardKey,
			ConfigNum: newCfg.Num,
		}
		kv.mu.Lock()
		for key := range kv.data {
			if key2shard(key) == shardKey {
				args.Data[key] = kv.data[key]
			}
		}
		kv.mu.Unlock()
		kv.logger.WithField("len", len(args.Data)).
			WithField("shard", shardKey).Debug("svCFG: migrate: moved key")
		wg.Add(1)
		go func(servers []string, args *MigrateArgs) {
			kv.sendMigrateRPC(servers, args, &MigrateReply{})
			kv.logger.WithField("shard", args.Shard).Debug("svCFG: migrate: move ok")
			wg.Done()
		}(servers, &args)
	}
	wg.Add(1)
	go func() {
		expectedIncomingShards := delta.ToMe(kv.gid)
		kv.logger.WithField("incomingMap", expectedIncomingShards).Debug("svCFG: migrate: expecting migrates")
		for len(expectedIncomingShards) != 0 {
			doneShardOffset := <-kv.migrateNotify[newCfg.Num]
			delete(expectedIncomingShards, doneShardOffset)
		}
		kv.logger.Debug("svCFG: migrate: incoming migrates done")
		wg.Done()
	}()
	wg.Wait()
	kv.logger.Debug("svCFG: migrate: done doMigrate")
	// val := atomic.AddInt32(&kv.nextMigrateIndex, 1)
	// kv.logger.WithField("new-nextMigrateIndex", val).Debug("svCFG: migrate: nexMigrateIndex at")
}
