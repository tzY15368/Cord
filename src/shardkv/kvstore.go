package shardkv

import (
	"bytes"
	"fmt"
	"strconv"
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
		}).Debug("kv: should issue snapshot")
		return true
	}
	return false
}

// isDuplicate not thread safe
func (kv *ShardKV) isDuplicate(req common.RequestInfo) bool {
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
	if kv.isDuplicate(op.RequestInfo) {
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
	case OP_MIGRATE:
		//再次检查上锁
		shardKey, err := strconv.Atoi(op.OP_KEY)
		if err != nil {
			panic(err)
		}
		if atomic.LoadInt32(&kv.shardLocks[shardKey]) != 1 {
			panic("errnolock")
		}
		buf := bytes.NewBuffer([]byte(op.OP_VALUE))
		decoder := labgob.NewDecoder(buf)
		var migrateMap map[string]string
		err = decoder.Decode(&migrateMap)
		if err != nil {
			panic(err)
		}

		for key := range migrateMap {
			kv.data[key] = migrateMap[key]
		}
	}
	// snapshot
	if kv.shouldIssueSnapshot() {
		go func(index int, data *[]byte) {
			atomic.StoreInt32(&kv.inSnapshot, 1)
			kv.rf.Snapshot(index, *data)
			atomic.StoreInt32(&kv.inSnapshot, 0)
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
	by := buf.Bytes()
	return &by
}

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
}
