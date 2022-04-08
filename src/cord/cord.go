package cord

import (
	"context"
	"sync"

	"6.824/config"
	"6.824/cord/cdc"
	"6.824/cord/kv"
	"6.824/logging"
	"6.824/proto"
	"6.824/raft"
	"github.com/sirupsen/logrus"
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
	cs := &CordServer{
		kvStore:      kv.NewTempKVStore(),
		bootConfig:   cfg,
		applyChan:    applyCh,
		rf:           raft.Make(cfg.MakeGRPCClients(), cfg.Me, raft.MakePersister(), applyCh),
		watchEnabled: false,
		logger:       logging.GetLogger("server", logrus.DebugLevel).WithField("id", cfg.Me),
		maxRaftState: int64(cfg.SnapshotThres),
		notify:       make(map[int64]chan *kv.EvalResult),
	}
	go cs.handleApply()
	return cs
}

// HandleRequest thread safe,
// 如果是watch之类的长连接则会block，但不应该长时间挂着锁
func (cs *CordServer) HandleRequest(ctx context.Context, in *proto.ServiceArgs) (*proto.ServiceReply, error) {
	reply := &proto.ServiceReply{}
	evalResult := cs.propose(*in)
	reply.Result = evalResult.Data
	return reply, evalResult.Err
}
