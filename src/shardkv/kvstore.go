package shardkv

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"6.824/common"
	"6.824/labgob"
	"github.com/sirupsen/logrus"
)

// shouldissuesnapshot not thread safe
func (kv *ShardKV) shouldIssueSnapshot() bool {
	if kv.maxraftstate == -1 || atomic.LoadInt32(&kv.inSnapshot) == 1 {
		return false
	}
	rfSize := kv.rf.GetStateSize()
	if kv.maxraftstate-rfSize < common.StateSizeDiff {

		kv.logger.WithFields(logrus.Fields{
			"raftStateSize": rfSize,
			"maxraftState":  kv.maxraftstate,
		}).Debug("skv: kvstore: should issue snapshot")
		ok := atomic.CompareAndSwapInt32(&kv.inSnapshot, 0, 1)
		if ok {
			return true
		}
	}
	return false
}

// isDuplicateKVCMD not thread safe
func (kv *ShardKV) isDuplicateKVCMD(req common.RequestInfo) bool {
	latestReqID, ok := kv.ack[req.ClientID]
	if ok {
		return latestReqID >= req.RequestID
	}
	return false
}

func (kv *ShardKV) evalOp(idx int, op *Op) opResult {
	res := opResult{
		err:         OK,
		RequestInfo: op.RequestInfo,
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
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
	if kv.shouldIssueSnapshot() {
		go func(index int, data *[]byte) {
			kv.rf.Snapshot(index, *data)
			ok := atomic.CompareAndSwapInt32(&kv.inSnapshot, 1, 0)
			if !ok {
				panic("no snapshot unlock")
			}
		}(idx, kv.dumpData())
	}
	kv.logger.WithField("op", fmt.Sprintf("%+v", op)).
		WithField("res", fmt.Sprintf("%+v", res)).Debug("kvstore: opresult")
	return res
}

// dumpData not thread safe
func (kv *ShardKV) dumpData() *[]byte {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err := encoder.Encode(&kv.data)
	if err != nil {
		panic(err)
	}
	err = encoder.Encode(&kv.ack)
	if err != nil {
		panic(err)
	}
	err = encoder.Encode(&kv.shardCFGVersion)
	if err != nil {
		panic(err)
	}
	err = encoder.Encode(&kv.maxCFGVersion)
	if err != nil {
		panic(err)
	}
	err = encoder.Encode(&kv.maxTransferVersion)
	if err != nil {
		panic(err)
	}
	err = encoder.Encode(&kv.config)
	if err != nil {
		panic(err)
	}
	by := buf.Bytes()
	return &by
}

// loadSnapshot thread safe
func (kv *ShardKV) loadSnapshot(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	decoder := labgob.NewDecoder(bytes.NewBuffer(data))
	err := decoder.Decode(&kv.data)
	if err != nil {
		panic(err)
	}
	err = decoder.Decode(&kv.ack)
	if err != nil {
		panic(err)
	}
	err = decoder.Decode(&kv.shardCFGVersion)
	if err != nil {
		panic(err)
	}
	err = decoder.Decode(&kv.maxCFGVersion)
	if err != nil {
		panic(err)
	}
	err = decoder.Decode(&kv.maxTransferVersion)
	if err != nil {
		panic(err)
	}
	err = decoder.Decode(&kv.config)
	if err != nil {
		panic(err)
	}
}
