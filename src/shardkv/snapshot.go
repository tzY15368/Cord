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
	v := atomic.AddInt64(&kv.msgCount, 1)
	if kv.maxraftstate == -1 {
		return false
	}
	shouldSnapshot := v != 0 && v%common.StateSizeDiff == 0
	return shouldSnapshot
	// if kv.maxraftstate == -1 || atomic.LoadInt32(&kv.inSnapshot) == 1 {
	// 	return false
	// }
	// rfSize := kv.rf.GetStateSize()
	// if kv.maxraftstate-rfSize < common.StateSizeDiff {

	// 	kv.logger.WithFields(logrus.Fields{
	// 		"raftStateSize": rfSize,
	// 		"maxraftState":  kv.maxraftstate,
	// 	}).Debug("skv: kvstore: should issue snapshot")
	// 	ok := atomic.CompareAndSwapInt32(&kv.inSnapshot, 0, 1)
	// 	if ok {
	// 		return true
	// 	}
	// }
	// return false
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
	err = encoder.Encode(&kv.outboundData)
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
	err = decoder.Decode(&kv.outboundData)
	if err != nil {
		panic(err)
	}
	kv.logger.WithFields(logrus.Fields{
		"version": kv.dumpShardVersion(),
		"config":  fmt.Sprintf("%+v", kv.config),
	}).Debug("skv: loaded snapshot")
}
