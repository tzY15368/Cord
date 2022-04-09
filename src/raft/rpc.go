package raft

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"6.824/labgob"
	"6.824/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type GRPCClient struct {
	Conn *grpc.ClientConn
}

func init() {
	labgob.Register(proto.ServiceArgs{})
}

func (rf *Raft) HandleCall(ctx context.Context, in *proto.GenericArgs) (*proto.GenericReply, error) {
	decoder := labgob.NewDecoder(bytes.NewBuffer(in.Data))
	outBuf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(outBuf)
	switch in.Method {
	case proto.GenericArgs_AppendEntries:
		args := AppendEntriesArgs{}
		err := decoder.Decode(&args)
		if err != nil {
			panic(err)
		}
		reply := AppendEntriesReply{}
		rf.AppendEntries(&args, &reply)
		err = encoder.Encode(reply)
		if err != nil {
			panic(err)
		}
	case proto.GenericArgs_RequestVote:
		args := RequestVoteArgs{}
		err := decoder.Decode(&args)
		if err != nil {
			panic(err)
		}
		reply := RequestVoteReply{}
		rf.RequestVote(&args, &reply)
		err = encoder.Encode(reply)
		if err != nil {
			panic(err)
		}
	case proto.GenericArgs_InstallSnapshot:
		args := InstallSnapshotArgs{}
		err := decoder.Decode(&args)
		if err != nil {
			panic(err)
		}
		reply := InstallSnapshotReply{}
		rf.InstallSnapshot(&args, &reply)
		err = encoder.Encode(reply)
		if err != nil {
			panic(err)
		}
	case proto.GenericArgs_Start:
		//a :=
		var a = proto.ServiceArgs{}
		err := decoder.Decode(&a)
		if err != nil {
			panic(err)
		}
		index, term, isLeader := rf.Start(a)
		reply := StartReply{
			Index:    index,
			Term:     term,
			IsLeader: isLeader,
		}
		err = encoder.Encode(&reply)
		if err != nil {
			panic(err)
		}
	default:
		panic(in.Method)
	}
	return &proto.GenericReply{Data: outBuf.Bytes()}, nil
}

func (c *GRPCClient) Call(method string, args interface{}, reply interface{}) bool {
	c2 := proto.NewGenericServiceClient(c.Conn)
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err := encoder.Encode(args)
	if err != nil {
		panic(err)
	}
	var _method proto.GenericArgs_Method
	switch method {
	case "Raft.AppendEntries":
		_method = proto.GenericArgs_AppendEntries
	case "Raft.InstallSnapshot":
		_method = proto.GenericArgs_InstallSnapshot
	case "Raft.RequestVote":
		_method = proto.GenericArgs_RequestVote
	case "Raft.Start":
		_method = proto.GenericArgs_Start
	}
	gArgs := proto.GenericArgs{Method: _method, Data: buf.Bytes()}
	ctx := context.TODO()
	clientDeadline := time.Now().Add(time.Duration(1 * time.Second))
	ctx, cancel := context.WithDeadline(ctx, clientDeadline)
	_ = cancel
	r, err := c2.HandleCall(ctx, &gArgs)
	if err != nil {
		stat, _ := status.FromError(err)
		fmt.Println(method, fmt.Sprintf("%+v", args), stat)
		return false
	}

	decoder := labgob.NewDecoder(bytes.NewBuffer(r.Data))
	err = decoder.Decode(reply)
	if err != nil {
		panic(err)
	}
	return true
}
