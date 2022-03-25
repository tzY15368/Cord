package shardkv

import "time"

// isKeyServed not thread safe
func (kv *ShardKV) isKeyServed(key string) bool {
	shard := key2shard(key)
	return kv.config.Shards[shard] == kv.gid
}

func (kv *ShardKV) pollCFG() {
	for {
		cfg := kv.ctlClerk.Query(-1)
		kv.mu.Lock()
		if cfg.Num != kv.config.Num {
			kv.logger.WithField("old", kv.config.Num).
				WithField("new", cfg.Num).Debug("skv: poll: found new cfg")
			kv.config = cfg
		}

		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
