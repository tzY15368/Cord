package internals

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"6.824/labgob"
	"6.824/raft"
	"6.824/raft/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type GRPCClient struct {
	Conn *grpc.ClientConn
}

type RaftInternalRPCService struct {
	Rf *raft.Raft
}

func (gs *RaftInternalRPCService) HandleCall(ctx context.Context, in *raftpb.GenericArgs) (*raftpb.GenericReply, error) {
	decoder := labgob.NewDecoder(bytes.NewBuffer(in.Data))
	outBuf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(outBuf)
	switch *in.Method {
	case raftpb.GenericArgs_AppendEntries:
		args := raft.AppendEntriesArgs{}
		err := decoder.Decode(&args)
		if err != nil {
			panic(err)
		}
		reply := raft.AppendEntriesReply{}
		gs.Rf.AppendEntries(&args, &reply)
		err = encoder.Encode(reply)
		if err != nil {
			panic(err)
		}
	case raftpb.GenericArgs_RequestVote:
		args := raft.RequestVoteArgs{}
		err := decoder.Decode(&args)
		if err != nil {
			panic(err)
		}
		reply := raft.RequestVoteReply{}
		gs.Rf.RequestVote(&args, &reply)
		err = encoder.Encode(reply)
		if err != nil {
			panic(err)
		}
	case raftpb.GenericArgs_InstallSnapshot:
		args := raft.InstallSnapshotArgs{}
		err := decoder.Decode(&args)
		if err != nil {
			panic(err)
		}
		reply := raft.InstallSnapshotReply{}
		gs.Rf.InstallSnapshot(&args, &reply)
		err = encoder.Encode(reply)
		if err != nil {
			panic(err)
		}
	default:
		panic(in.Method)
	}
	return &raftpb.GenericReply{Data: outBuf.Bytes()}, nil
}

func (c *GRPCClient) Call(method string, args interface{}, reply interface{}) bool {
	c2 := raftpb.NewGenericServiceClient(c.Conn)
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err := encoder.Encode(args)
	if err != nil {
		panic(err)
	}
	var _method *raftpb.GenericArgs_Method
	switch method {
	case "Raft.AppendEntries":
		_method = raftpb.GenericArgs_AppendEntries.Enum()
	case "Raft.InstallSnapshot":
		_method = raftpb.GenericArgs_InstallSnapshot.Enum()
	case "Raft.RequestVote":
		_method = raftpb.GenericArgs_RequestVote.Enum()
	}
	gArgs := raftpb.GenericArgs{Method: _method, Data: buf.Bytes()}
	ctx := context.TODO()
	clientDeadline := time.Now().Add(time.Duration(3 * time.Second))
	ctx, cancel := context.WithDeadline(ctx, clientDeadline)
	defer cancel()
	r, err := c2.HandleCall(ctx, &gArgs)
	if err != nil {
		stat, ok := status.FromError(err)
		fmt.Println(stat, ok)
		return false
	}

	decoder := labgob.NewDecoder(bytes.NewBuffer(r.Data))
	err = decoder.Decode(reply)
	if err != nil {
		panic(err)
	}
	return true
}
