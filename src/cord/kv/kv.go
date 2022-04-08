package kv

import (
	"errors"
	"sync"

	"6.824/proto"
)

// 需要一个FIFO的RWLOCK
type TempKVStore struct {
	mu                 sync.RWMutex
	dataStore          proto.TempKVStore
	ack                proto.AckMap
	dataChangeHandlers []func(string, string)
}

type EvalResult struct {
	Err  error
	Data map[string]string
	Info *proto.RequestInfo
}

var ErrGetOnly = errors.New("Err get only in unserializable reads")
var ErrNotImpl = errors.New("err not impl")

func NewTempKVStore() *TempKVStore {
	tks := &TempKVStore{
		dataStore:          proto.TempKVStore{Data: make(map[string]string)},
		ack:                proto.AckMap{Ack: make(map[int64]int64)},
		dataChangeHandlers: make([]func(string, string), 0),
	}
	return tks
}

func (kvs *TempKVStore) RegisterDataChangeHandler(in func(string, string)) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	kvs.dataChangeHandlers = append(kvs.dataChangeHandlers, in)
}

func (kvs *TempKVStore) EvalGETUnserializable(args *proto.ServiceArgs) (reply *EvalResult) {
	reply.Data = make(map[string]string)
	kvs.mu.RLock()
	defer kvs.mu.RUnlock()
	for _, cmd := range args.Cmds {
		if *cmd.OpType != proto.CmdArgs_GET {
			reply.Err = ErrGetOnly
			return
		}
		reply.Data[*cmd.OpKey] = kvs.dataStore.Data[*cmd.OpKey]
	}
	return
}

func (kvs *TempKVStore) EvalCMD(args *proto.ServiceArgs, shouldSnapshot bool) (reply *EvalResult, dump *[]byte) {
	reply.Data = make(map[string]string)
	var lockWrite = shouldSnapshot
	if !shouldSnapshot {
		for _, cmd := range args.Cmds {
			if *cmd.OpType != proto.CmdArgs_GET {
				lockWrite = true
				break
			}
		}
	}
	if lockWrite {
		kvs.mu.Lock()
		defer kvs.mu.Unlock()
	} else {
		kvs.mu.RLock()
		defer kvs.mu.RUnlock()
	}
	for _, cmd := range args.Cmds {
		switch *cmd.OpType {
		case proto.CmdArgs_GET:
			reply.Data[*cmd.OpKey] = kvs.dataStore.Data[*cmd.OpKey]
		case proto.CmdArgs_APPEND:
			kvs.dataStore.Data[*cmd.OpKey] += *cmd.OpVal
			for _, handler := range kvs.dataChangeHandlers {
				handler(*cmd.OpKey, kvs.dataStore.Data[*cmd.OpKey])
			}
		case proto.CmdArgs_PUT:
			kvs.dataStore.Data[*cmd.OpKey] = *cmd.OpVal
			for _, handler := range kvs.dataChangeHandlers {
				handler(*cmd.OpKey, *cmd.OpVal)
			}
		default:
			reply.Err = ErrNotImpl
			return
		}
	}
	var data []byte
	if shouldSnapshot {
		data, reply.Err = kvs.dataStore.Marshal()
		dump = &data
	}
	return
}
