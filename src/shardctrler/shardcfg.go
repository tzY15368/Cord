package shardctrler

import (
	"bytes"
	"container/list"
	"fmt"
	"sort"

	"6.824/labgob"
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

func LoadCFG(data string) Config {
	cfg := Config{}
	decoder := labgob.NewDecoder(bytes.NewBuffer([]byte(data)))
	err := decoder.Decode(&cfg)
	if err != nil {
		panic(err)
	}
	return cfg
}

func (cfg *Config) Dump() string {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err := encoder.Encode(cfg)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

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

// Bump not thread safe
func (cfg *Config) Clone() Config {
	c := Config{
		Num:    cfg.Num,
		Shards: cfg.Shards,
		Groups: copyMap(cfg.Groups),
	}

	return c
}

// bump not thread safe
func (cfg *Config) bump() Config {
	c := cfg.Clone()
	c.Num += 1
	return c
}

const (
	CFG_LOCK   = 1000
	CFG_UNLOCK = 1001
)

type DiffPair struct {
	FromGID int
	ToGID   int
}

type DiffCfg struct {
	Data map[int]DiffPair
}

// serialize to string
func (dc *DiffCfg) Dump() string {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err := encoder.Encode(dc.Data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

func NewDiffCfg(s string) *DiffCfg {
	buf := bytes.NewBuffer([]byte(s))
	decoder := labgob.NewDecoder(buf)
	cfg := &DiffCfg{}
	err := decoder.Decode(&cfg.Data)
	if err != nil {
		panic(err)
	}
	return cfg
}

func (dc *DiffCfg) IrrelevantShards(gid int) []int {
	var res []int
	for i := 0; i < NShards; i++ {
		if _, ok := dc.Data[i]; !ok {
			res = append(res, i)
		}
	}
	return res
}

func (dc *DiffCfg) RelevantShards(gid int) []int {
	var res []int
	for shardKey, v := range dc.Data {
		if v.FromGID == gid || v.ToGID == gid {
			res = append(res, shardKey)
		}
	}
	return res
}

// to me returns shards that are transferred to the given gid
// returns map of shards to from-gids
func (dc *DiffCfg) ToMe(gid int) map[int]int {
	res := make(map[int]int)
	for shardKey := range dc.Data {
		if dc.Data[shardKey].FromGID == 0 {
			continue
		}
		if dc.Data[shardKey].ToGID == gid {
			res[shardKey] = dc.Data[shardKey].FromGID
		}
	}
	return res
}

// from me returns shards that are transferred from the given gid.
// returns map of shards to to-gids
func (dc *DiffCfg) FromMe(gid int) map[int]int {
	res := make(map[int]int)
	for shardkey := range dc.Data {
		if dc.Data[shardkey].FromGID == gid {
			res[shardkey] = dc.Data[shardkey].ToGID
		}
	}
	return res
}

func (new *Config) DiffOld(old *Config) *DiffCfg {
	res := DiffCfg{
		Data: make(map[int]DiffPair),
	}
	for i := range old.Shards {
		if new.Shards[i] != old.Shards[i] {
			res.Data[i] = DiffPair{
				FromGID: old.Shards[i],
				ToGID:   new.Shards[i],
			}
		}
	}
	return &res
}

func copyMap(m1 map[int][]string) map[int][]string {
	m2 := make(map[int][]string)
	for k, v := range m1 {
		m2[k] = v
	}
	return m2
}
