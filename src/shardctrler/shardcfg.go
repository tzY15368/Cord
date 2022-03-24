package shardctrler

import (
	"container/list"
	"fmt"

	"github.com/sirupsen/logrus"
)

// The number of shards.
const NShards = 10

type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

// rebalance not thread safe
// rebalances shards to groups at minimal cost
// 返回diff：rebalance之前某个shardidx被分配到某个gid
func (cfg *Config) rebalance() /*map[int]int*/ {
	lowerAvg := NShards / len(cfg.Groups)
	gidToShardMap := make(map[int]int)
	spareShards := list.New()
	for i, gid := range cfg.Shards {
		if gidToShardMap[gid] > lowerAvg {
			spareShards.PushBack(i)
			// spareShards = append(spareShards, i)
		} else {
			gidToShardMap[gid]++
		}
	}
	for gid := range gidToShardMap {
		for spareShards.Len() != 0 && gidToShardMap[gid] < lowerAvg {
			shardID := spareShards.Remove(spareShards.Front()).(int)
			cfg.Shards[shardID] = gid
			gidToShardMap[gid]++
		}
	}
	for gid := range cfg.Groups {
		if spareShards.Len() == 0 {
			break
		}
		if gidToShardMap[gid] > lowerAvg {
			continue
		}
		shardID := spareShards.Remove(spareShards.Front()).(int)
		cfg.Shards[shardID] = gid
	}
}

// dump not thread safe
func (cfg *Config) dump(logger *logrus.Entry) *logrus.Entry {
	return logger.WithField("cfg", fmt.Sprintf("%+v", cfg))
}

// clone not thread safe
func (cfg *Config) clone() Config {
	c := Config{
		Num:    cfg.Num + 1,
		Shards: cfg.Shards,
		Groups: copyMap(cfg.Groups),
	}

	return c
}

func copyMap(m1 map[int][]string) map[int][]string {
	m2 := make(map[int][]string)
	for k, v := range m1 {
		m2[k] = v
	}
	return m2
}
