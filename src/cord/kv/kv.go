package kv

import (
	"errors"
	"fmt"
	"sync"

	"6.824/cord/cdc"
	"6.824/logging"
	"6.824/proto"
	"github.com/sirupsen/logrus"
)

// 需要一个FIFO的RWLOCK
type TempKVStore struct {
	mu                 sync.RWMutex
	dataStore          proto.TempKVStore
	ack                proto.AckMap
	dataChangeHandlers DataChangeHandler
	logger             *logrus.Logger
}

type DataChangeHandler interface {
	CaptureDataChange(string, string)
	Watch(string) *cdc.WatchResult
}

type EvalResult struct {
	Err     error
	Data    map[string]string
	Info    *proto.RequestInfo
	Watches []*cdc.WatchResult
}

var ErrGetOnly = errors.New("err get only in unserializable reads")
var ErrNoWatch = errors.New("err watches not enabled")
var ErrNotImpl = errors.New("err not impl")

func NewTempKVStore() *TempKVStore {
	tks := &TempKVStore{
		dataStore: proto.TempKVStore{Data: make(map[string]string)},
		ack:       proto.AckMap{Ack: make(map[int64]int64)},
		logger:    logging.GetLogger("kvs", logrus.InfoLevel),
	}
	return tks
}

func (kvs *TempKVStore) SetCDC(dch DataChangeHandler) {
	kvs.dataChangeHandlers = dch
}

func (kvs *TempKVStore) EvalCMDUnlinearizable(args *proto.ServiceArgs) *EvalResult {
	reply := &EvalResult{
		Data: make(map[string]string),
	}
	kvs.mu.RLock()
	for _, cmd := range args.Cmds {
		if cmd.OpType != proto.CmdArgs_GET {
			reply.Err = ErrGetOnly
			break
		}
		reply.Data[cmd.OpKey] = kvs.dataStore.Data[cmd.OpKey]
	}
	defer kvs.mu.RUnlock()
	return reply
}

func (kvs *TempKVStore) isDuplicate(req *proto.RequestInfo) bool {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	latestID, ok := kvs.ack.Ack[req.ClientID]
	var ans = false
	if ok {
		ans = latestID >= req.RequestID
	}
	if !ans {
		kvs.ack.Ack[req.ClientID] = req.RequestID
	}
	return ans
}

func (kvs *TempKVStore) EvalCMD(args *proto.ServiceArgs, shouldSnapshot bool) (reply *EvalResult, dump *[]byte) {
	reply = &EvalResult{
		Data:    make(map[string]string),
		Info:    args.Info,
		Watches: make([]*cdc.WatchResult, 0),
	}
	if kvs.isDuplicate(args.Info) {
		kvs.logger.WithField("Args", fmt.Sprintf("%+v", args)).Warn("duplicate request")
		return
	}
	var lockWrite = shouldSnapshot
	if !shouldSnapshot {
		for _, cmd := range args.Cmds {
			if cmd.OpType != proto.CmdArgs_GET && cmd.OpType != proto.CmdArgs_WATCH {
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
		switch cmd.OpType {
		case proto.CmdArgs_GET:
			reply.Data[cmd.OpKey] = kvs.dataStore.Data[cmd.OpKey]
		case proto.CmdArgs_APPEND:
			kvs.dataStore.Data[cmd.OpKey] += cmd.OpVal
			if kvs.dataChangeHandlers != nil {
				kvs.dataChangeHandlers.CaptureDataChange(cmd.OpKey, kvs.dataStore.Data[cmd.OpKey])
			}
		case proto.CmdArgs_PUT:
			kvs.dataStore.Data[cmd.OpKey] = cmd.OpVal
			if kvs.dataChangeHandlers != nil {
				kvs.dataChangeHandlers.CaptureDataChange(cmd.OpKey, cmd.OpVal)
			}
		case proto.CmdArgs_WATCH:
			if kvs.dataChangeHandlers == nil {
				reply.Err = ErrNoWatch
				return
			}
			watchResult := kvs.dataChangeHandlers.Watch(cmd.OpKey)
			reply.Watches = append(reply.Watches, watchResult)
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
