package shardctrler

import (
	"container/list"
	"fmt"
	"sort"

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
// 把shard分到group头上，需要注意group里可能有新进来的
// 之前shard里没有的内容，也可能会少内容
func (cfg *Config) rebalance() /*map[int]int*/ {
	if len(cfg.Groups) == 0 {
		for i := range cfg.Shards {
			cfg.Shards[i] = 0
		}
		return
	}
	lowerAvg := NShards / len(cfg.Groups)
	gidShardCount := make(map[int]int)
	spareShards := list.New()
	// shard里的gid是之前的，需要注意可能发生leave从而实际比group里的gid多
	// 以group为准
	for gid := range cfg.Groups {
		gidShardCount[gid] = 0
	}
	for i, gid := range cfg.Shards {
		if gid == 0 {
			spareShards.PushBack(i)
			continue
		}
		count, ok := gidShardCount[gid]
		if !ok {
			spareShards.PushBack(i)
			continue
		}
		if count >= lowerAvg {
			spareShards.PushBack(i)
			continue
		}
		gidShardCount[gid]++
	}
	// stable traverse see https://go.dev/blog/maps#TOC_7.
	var keys []int
	for k := range gidShardCount {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, gid := range keys {
		for spareShards.Len() != 0 && gidShardCount[gid] < lowerAvg {
			shardID := spareShards.Remove(spareShards.Front()).(int)
			cfg.Shards[shardID] = gid
			gidShardCount[gid]++
		}
	}

	var keys2 []int
	for k := range cfg.Groups {
		keys2 = append(keys2, k)
	}
	sort.Ints(keys2)
	// stable traverse on groups
	for _, gid := range keys2 {
		if spareShards.Len() == 0 {
			break
		}
		if gidShardCount[gid] > lowerAvg {
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
