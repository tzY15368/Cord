package shardctrler

import (
	"bytes"
	"fmt"

	"6.824/common"
	"6.824/labgob"
)

func (sc *ShardCtrler) loadSnapshot(data []byte) {
	panic("not impl")
}

// isDuplicate not thread safe
func (sc *ShardCtrler) isDuplicate(req common.RequestInfo) bool {
	id, ok := sc.ack[req.ClientID]
	if ok {
		return id >= req.RequestID
	}
	return false
}
func (sc *ShardCtrler) evalOp(op Op) opResult {
	decoder := labgob.NewDecoder(bytes.NewBuffer(op.OP_DATA))
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.isDuplicate(op.RequestInfo) && op.OP_TYPE != OP_QUERY {
		sc.logger.WithField("op", op).Debug("sc: eval: is duplicate, skipping")
		return opResult{
			err:         OK,
			requestInfo: op.RequestInfo,
		}
	}
	sc.ack[op.RequestInfo.ClientID] = op.RequestInfo.RequestID
	switch op.OP_TYPE {
	case OP_JOIN:
		var servers map[int][]string
		err := decoder.Decode(&servers)
		if err != nil {
			sc.logger.Panic("decode fail", op.OP_TYPE, err)
		}
		prevCfg := sc.configs[len(sc.configs)-1]
		prevCfg.dump(sc.logger).Debug("sc: join: before join")
		newConfig := prevCfg.clone()
		for k, v := range servers {
			newConfig.Groups[k] = v
		}
		newConfig.rebalance()
		sc.configs = append(sc.configs, newConfig)
		newConfig.dump(sc.logger).Debug("sc: join: after join")
		return opResult{
			err:         OK,
			requestInfo: op.RequestInfo,
		}
	case OP_LEAVE:
		// 一个shard只出现在一个group，
		// 一个group管多个shard
		// 一个group会包含多个server做复制
		var gids []int
		err := decoder.Decode(&gids)
		if err != nil {
			sc.logger.Panic("decode fail", op.OP_TYPE, err)
		}
		newCfg := sc.configs[len(sc.configs)-1].clone()
		newCfg.dump(sc.logger).Debug("sc: leave: before leave")
		sc.logger.WithField("cfg", fmt.Sprintf("%+v", newCfg)).Debug("sc: leave: before leave")
		for _, gid := range gids {
			delete(newCfg.Groups, gid)
		}
		newCfg.rebalance()
		sc.configs = append(sc.configs, newCfg)
		newCfg.dump(sc.logger).Debug("sc: leave: after leave")
		return opResult{
			err:         OK,
			requestInfo: op.RequestInfo,
		}
	case OP_MOVE:
		var gid int
		var shard int
		err := decoder.Decode(&gid)
		if err != nil {
			sc.logger.Panic("decode fail", op.OP_TYPE, err)
		}
		err = decoder.Decode(&shard)
		if err != nil {
			sc.logger.Panic("decode fail", op.OP_TYPE, err)
		}
		sc.logger.WithField("gid", gid).WithField("shard", shard).
			Debug("sc: move: params")
		cfg := sc.appendConfig()
		cfg.dump(sc.logger).Debug("sc: move: before")
		cfg.Shards[shard] = gid
		cfg.dump(sc.logger).Debug("sc: move: after")
		return opResult{
			err:         OK,
			requestInfo: op.RequestInfo,
		}
	case OP_QUERY:
		var num int
		err := decoder.Decode(&num)
		if err != nil {
			sc.logger.Panic("decode fail", op.OP_TYPE, err)
		}
		if num == -1 || num > len(sc.configs)-1 {
			num = len(sc.configs) - 1
		}
		sc.logger.WithField("num", num).Debug("sc: query: params")
		return opResult{
			err:         OK,
			requestInfo: op.RequestInfo,
			cfg:         &sc.configs[num],
		}
	}
	sc.logger.Panic("invalid op type", op.OP_TYPE)
	return opResult{}
}
