package kv

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"6.824/cord/cdc"
	"6.824/cord/intf"
	"6.824/logging"
	"6.824/proto"
	"github.com/sirupsen/logrus"
)

// 需要一个FIFO的RWLOCK
type TempKVStore struct {
	mu                 sync.RWMutex
	dataStore          proto.TempKVStore
	dataChangeHandlers DataChangeHandler
	logger             *logrus.Logger
}

type DataChangeHandler interface {
	CaptureDataChange(string, string)
	Watch(string) (*cdc.WatchResult, error)
}

type EvalResult struct {
	err     error
	data    map[string]string
	info    *proto.RequestInfo
	watches []*cdc.WatchResult
}

func (er *EvalResult) GetError() error {
	return er.err
}

func (er *EvalResult) GetData() map[string]string {
	return er.data
}

func (er *EvalResult) GetClientID() int64 {
	if er.info == nil {
		return -1
	}
	return er.info.ClientID
}
func (er *EvalResult) GetRequestID() int64 {
	if er.info == nil {
		return -1
	}
	return er.info.RequestID
}

func (er *EvalResult) AwaitWatches() map[string]string {
	d := make(map[string]string)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, watch := range er.watches {
		wg.Add(1)
		go func(c *cdc.WatchResult) {
			data := <-c.Notify
			fmt.Println("got output", data)
			mu.Lock()
			d[c.Key] = data
			mu.Unlock()
			c.Callback()
			wg.Done()
		}(watch)
	}
	wg.Wait()
	return d

}

var ErrGetOnly = errors.New("err get only in unserializable reads")
var ErrNoWatch = errors.New("err watches not enabled")
var ErrNotImpl = errors.New("err not impl")

func NewTempKVStore(watchEnabled bool) *TempKVStore {
	tks := &TempKVStore{
		dataStore: proto.TempKVStore{Data: make(map[string]*proto.KVEntry)},
		logger:    logging.GetLogger("kvs", logrus.InfoLevel),
	}
	if watchEnabled {
		tks.dataChangeHandlers = cdc.NewDCC()
	}
	return tks
}

func dataExpired(ttl int64) bool {
	if ttl == 0 {
		return false
	}
	now := time.Now().UnixNano() / 1e6
	return ttl < now
}

func (kvs *TempKVStore) EvalCMD(args *proto.ServiceArgs, shouldSnapshot bool, getOnly bool) (intf.IEvalResult, []byte) {
	reply := &EvalResult{
		data:    make(map[string]string),
		info:    args.Info,
		watches: make([]*cdc.WatchResult, 0),
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
		if !args.Linearizable && cmd.OpType != proto.CmdArgs_GET {
			reply.err = ErrGetOnly
			return reply, nil
		}
	}
	for _, cmd := range args.Cmds {
		switch cmd.OpType {
		case proto.CmdArgs_GET:
			if len(cmd.OpKey) > 1 && cmd.OpKey[len(cmd.OpKey)-1] == '*' {
				for key, entry := range kvs.dataStore.Data {
					if len(key) >= len(cmd.OpKey)-1 && key[:len(cmd.OpKey)-1] == cmd.OpKey[:len(cmd.OpKey)-1] {
						if dataExpired(entry.Ttl) {
							fmt.Println("ttl reached")
							delete(kvs.dataStore.Data, key)
						} else {
							reply.data[key] = entry.Data

						}
					}
				}
				return reply, nil
			} else if cmd.OpKey == "*" {
				for key, entry := range kvs.dataStore.Data {
					if dataExpired(entry.Ttl) {
						fmt.Println("ttl reached")
						delete(kvs.dataStore.Data, key)
					} else {
						reply.data[key] = entry.Data

					}
				}
				return reply, nil
			}
			entry := kvs.dataStore.Data[cmd.OpKey]
			reply.data[cmd.OpKey] = ""
			if entry != nil {
				if dataExpired(entry.Ttl) {
					fmt.Println("ttl reached")
					delete(kvs.dataStore.Data, cmd.OpKey)
				} else {
					reply.data[cmd.OpKey] = entry.Data
				}
			}
		case proto.CmdArgs_APPEND:
			old := kvs.dataStore.Data[cmd.OpKey]
			didChange := cmd.OpVal != ""
			if old != nil {
				kvs.dataStore.Data[cmd.OpKey].Data += cmd.OpVal
				kvs.dataStore.Data[cmd.OpKey].Ttl = cmd.Ttl
			} else {
				kvs.dataStore.Data[cmd.OpKey] = &proto.KVEntry{Data: cmd.OpVal, Ttl: cmd.Ttl}
			}
			if kvs.dataChangeHandlers != nil && didChange {
				kvs.dataChangeHandlers.CaptureDataChange(cmd.OpKey, kvs.dataStore.Data[cmd.OpKey].Data)
			}
		case proto.CmdArgs_PUT:
			didChange := false
			entry := kvs.dataStore.Data[cmd.OpKey]
			if entry != nil && entry.Data != cmd.OpVal {
				didChange = true
			}
			kvs.dataStore.Data[cmd.OpKey] = &proto.KVEntry{Data: cmd.OpVal, Ttl: cmd.Ttl}
			if kvs.dataChangeHandlers != nil && didChange {
				kvs.dataChangeHandlers.CaptureDataChange(cmd.OpKey, cmd.OpVal)
			}
		case proto.CmdArgs_WATCH:
			if kvs.dataChangeHandlers == nil {
				reply.err = ErrNoWatch
				return reply, nil
			}
			watchResult, err := kvs.dataChangeHandlers.Watch(cmd.OpKey)
			if err != nil {
				reply.err = err
			} else {
				reply.watches = append(reply.watches, watchResult)
			}
		case proto.CmdArgs_DELETE:
			// 删除不存在的key不应该唤醒watch
			entry := kvs.dataStore.Data[cmd.OpKey]
			if entry != nil && kvs.dataChangeHandlers != nil {
				kvs.dataChangeHandlers.CaptureDataChange(cmd.OpKey, "")
			}

		default:
			reply.err = ErrNotImpl
			return reply, nil
		}
	}
	var dump []byte
	if shouldSnapshot {
		dump, reply.err = kvs.dataStore.Marshal()
		fmt.Println("snapshot: got dump bytes", len(dump))
	}
	return reply, dump
}

func (kvs *TempKVStore) LoadSnapshot(data []byte) {
	err := kvs.dataStore.Unmarshal(data)
	if err != nil {
		panic(err)
	}
}
