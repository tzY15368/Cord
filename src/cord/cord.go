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
	"6.824/logging"
	"6.824/proto"
	"6.824/raft"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type IKVStore interface {
	EvalCMD(*proto.ServiceArgs, bool) (*kv.EvalResult, *[]byte)
	EvalGETUnserializable(*proto.ServiceArgs) *kv.EvalResult
	RegisterDataChangeHandler(func(string, string))
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
}

func NewCordServer(cfg *config.CordConfig) *CordServer {
	applyCh := make(chan raft.ApplyMsg)
	var clientID int64 = common.Nrand()
	var RequestID int64 = 0
	cs := &CordServer{
		kvStore:          kv.NewTempKVStore(),
		bootConfig:       cfg,
		applyChan:        applyCh,
		rf:               raft.Make(cfg.MakeGRPCClients(), cfg.Me, raft.MakePersister(), applyCh),
		watchEnabled:     false,
		logger:           logging.GetLogger("server", logrus.DebugLevel).WithField("id", cfg.Me),
		maxRaftState:     int64(cfg.SnapshotThres),
		notify:           make(map[int64]chan *kv.EvalResult),
		localRequestInfo: &proto.RequestInfo{ClientID: clientID, RequestID: RequestID},
	}
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
	return reply, evalResult.Err
}
