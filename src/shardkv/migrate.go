package shardkv

import (
	"time"

	"6.824/shardctrler"
)

func (kv *ShardKV) doMigrate(delta *shardctrler.DiffCfg, newCfg *shardctrler.Config) {
	outGoingShards := delta.FromMe(kv.gid)
	for shardKey, targetGID := range outGoingShards {
		servers := newCfg.Groups[targetGID]
		for _, server := range servers {
			go func(svName string) {
				args := MigrateArgs{
					Data: make(map[string]string),
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
				ok := kv.make_end(server).Call("", &args, &reply)
				_ = ok
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
