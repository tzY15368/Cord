package cord

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"6.824/common"
	"6.824/config"
	"6.824/cord/cdc"
	"6.824/cord/kv"
	"6.824/diskpersister"
	"6.824/logging"
	"6.824/proto"
	"6.824/raft"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type IKVStore interface {
	EvalCMDUnlinearizable(*proto.ServiceArgs) *kv.EvalResult
	EvalCMD(*proto.ServiceArgs, bool) (*kv.EvalResult, []byte)
	SetCDC(kv.DataChangeHandler)
	LoadSnapshot([]byte)
}

type CordServer struct {
	mu               sync.Mutex
	kvStore          IKVStore
	rf               *raft.Raft
	bootConfig       *config.CordConfig
	applyChan        chan raft.ApplyMsg
	localRequestInfo *proto.RequestInfo
	notify           map[int64]chan *kv.EvalResult
	logger           *logrus.Entry
	maxRaftState     int64
	watchEnabled     bool
	cdc              *cdc.DataChangeCapturer
	inSnapshot       int32
	Persister        raft.IPersistable
}

func NewCordServer(cfg *config.CordConfig) *CordServer {
	applyCh := make(chan raft.ApplyMsg)
	var clientID int64 = common.Nrand()
	var RequestID int64 = 0
	persister := diskpersister.NewMMapPersister(
		fmt.Sprintf("raft-state-out-%d", cfg.Me),
		fmt.Sprintf("snapshot-out-%d", cfg.Me),
		5000,
	)
	cs := &CordServer{
		kvStore:    kv.NewTempKVStore(),
		bootConfig: cfg,
		applyChan:  applyCh,
		//rf:               raft.Make(cfg.MakeGRPCClients(), cfg.Me, raft.MakePersister(), applyCh),
		rf:               raft.Make(cfg.MakeGRPCClients(), cfg.Me, persister, applyCh),
		watchEnabled:     cfg.WatchEnabled,
		logger:           logging.GetLogger("server", logrus.DebugLevel).WithField("id", cfg.Me),
		maxRaftState:     int64(cfg.SnapshotThres),
		notify:           make(map[int64]chan *kv.EvalResult),
		localRequestInfo: &proto.RequestInfo{ClientID: clientID, RequestID: RequestID},
		cdc:              cdc.NewDCC(),
		Persister:        persister,
	}
	cs.kvStore.SetCDC(cs.cdc)
	go func() {
		server := grpc.NewServer()
		proto.RegisterGenericServiceServer(server, cs.rf)
		listener, err := net.Listen("tcp", cfg.Addrs[cfg.Me])
		if err != nil {
			panic(err)
		}
		fmt.Println("serving raft internals on addr:", cfg.Addrs[cfg.Me])
		server.Serve(listener)

	}()
	go cs.handleApply()
	return cs
}

func (cs *CordServer) CreateRequestInfo() proto.RequestInfo {
	v := atomic.AddInt64(&cs.localRequestInfo.RequestID, 1)
	return proto.RequestInfo{
		ClientID:  cs.localRequestInfo.ClientID,
		RequestID: v,
	}
}

// HandleRequest thread safe,
// 如果是watch之类的长连接则会block，但不应该长时间挂着锁
func (cs *CordServer) HandleRequest(ctx context.Context, in *proto.ServiceArgs) (*proto.ServiceReply, error) {
	reply := &proto.ServiceReply{}
	evalResult := cs.propose(*in)
	reply.Result = evalResult.Data
	// aggregate watch results if necessary
	if len(evalResult.Watches) > 0 {
		fmt.Printf("result:--%+v", *evalResult.Watches[0])
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, watch := range evalResult.Watches {
		wg.Add(1)
		go func(c *cdc.WatchResult) {
			data := <-c.Notify
			fmt.Println("got output", data)
			mu.Lock()
			reply.Result[c.Key] = data
			mu.Unlock()
			c.Callback()
			wg.Done()
		}(watch)
	}
	wg.Wait()

	return reply, evalResult.Err
}
