package clerk

import (
	"context"
	"sync/atomic"
	"time"

	"6.824/common"
	"6.824/logging"
	"6.824/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Clerk struct {
	localRequestInfo *proto.RequestInfo
	conns            []*grpc.ClientConn
	addrOffset       int32
	logger           *logrus.Logger
}

func NewClerk(addrs []string) *Clerk {
	c := &Clerk{
		localRequestInfo: &proto.RequestInfo{
			ClientID:  common.Nrand(),
			RequestID: 0,
		},
		conns:      make([]*grpc.ClientConn, len(addrs)),
		addrOffset: 0,
		logger:     logging.GetLogger("clerk", logrus.DebugLevel),
	}
	var err error
	for i := range addrs {
		c.conns[i], err = grpc.Dial(addrs[i], grpc.WithInsecure())
		if err != nil {
			panic(err)
		}
	}
	return c
}

func (c *Clerk) HandleRequest(ctx context.Context, args *proto.ServiceArgs) (*proto.ServiceReply, error) {
retry:
	currentOffset := atomic.LoadInt32(&c.addrOffset) % int32(len(c.conns))
	cli := proto.NewExternalServiceClient(c.conns[currentOffset])
	reply, err := cli.HandleRequest(ctx, args)
	if err != nil {
		logrus.WithError(err).WithField("currentOffset", currentOffset).Warn("request failed, sleeping 500ms")
		atomic.AddInt32(&c.addrOffset, 1)
		time.Sleep(500 * time.Millisecond)
		goto retry
	}
	return reply, nil
}

func (cs *Clerk) CreateRequestInfo() proto.RequestInfo {
	v := atomic.AddInt64(&cs.localRequestInfo.RequestID, 1)
	return proto.RequestInfo{
		ClientID:  cs.localRequestInfo.ClientID,
		RequestID: v,
	}
}
