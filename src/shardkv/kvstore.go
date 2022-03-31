package shardkv

import (
	"fmt"

	"6.824/common"
	"github.com/sirupsen/logrus"
)

// isDuplicateKVCMD not thread safe
func (kv *ShardKV) isDuplicateKVCMD(req common.RequestInfo) bool {
	latestReqID, ok := kv.ack[req.ClientID]
	if ok {
		return latestReqID >= req.RequestID
	}
	return false
}

// evalOP not thread safe
func (kv *ShardKV) evalOp(op *Op) opResult {
	res := opResult{
		err:         OK,
		RequestInfo: op.RequestInfo,
	}
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	shard := key2shard(op.OP_KEY)
	if kv.config.Shards[shard] != kv.gid {
		res.err = ErrWrongGroup
		kv.logger.WithFields(logrus.Fields{
			"shard": shard,
		}).Warn("kvstore: eval abort due to shard change")
		return res
	}
	if kv.isDuplicateKVCMD(op.RequestInfo) {
		return res
	} else {
		kv.ack[op.RequestInfo.ClientID] = op.RequestInfo.RequestID
	}
	switch op.OP_TYPE {
	case OP_GET:
		data, ok := kv.data[op.OP_KEY]
		if ok {
			res.data = data
		} else {
			res.err = ErrNoKey
		}
	case OP_PUT:
		kv.data[op.OP_KEY] = op.OP_VALUE
	case OP_APPEND:
		kv.data[op.OP_KEY] += op.OP_VALUE
	}
	// snapshot
	// if kv.shouldIssueSnapshot() {
	// 	go func(index int, data *[]byte) {
	// 		kv.rf.Snapshot(index, *data)
	// 		ok := atomic.CompareAndSwapInt32(&kv.inSnapshot, 1, 0)
	// 		if !ok {
	// 			panic("no snapshot unlock")
	// 		}
	// 	}(idx, kv.dumpData())
	// }
	kv.logger.WithField("op", fmt.Sprintf("%+v", op)).
		WithField("res", fmt.Sprintf("%+v", res)).Debug("kvstore: opresult")
	return res
}
