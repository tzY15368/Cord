package clerk

import (
	"context"
	"sync/atomic"

	"6.824/common"
	"6.824/proto"
	"google.golang.org/grpc"
)

type Clerk struct {
	localRequestInfo *proto.RequestInfo
	conns            []*grpc.ClientConn
}

func NewClerk(addrs []string) *Clerk {
	c := &Clerk{
		localRequestInfo: &proto.RequestInfo{
			ClientID:  common.Nrand(),
			RequestID: 0,
		},
		conns: make([]*grpc.ClientConn, len(addrs)),
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
	cli := proto.NewExternalServiceClient(c.conns[0])
	return cli.HandleRequest(ctx, args)
}

func (cs *Clerk) CreateRequestInfo() proto.RequestInfo {
	v := atomic.AddInt64(&cs.localRequestInfo.RequestID, 1)
	return proto.RequestInfo{
		ClientID:  cs.localRequestInfo.ClientID,
		RequestID: v,
	}
}
