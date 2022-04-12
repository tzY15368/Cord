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
		args := proto.AppendEntriesArgs{}
		err := args.Unmarshal(in.Data)
		if err != nil {
			panic(err)
		}
		reply := proto.AppendEntriesReply{}
		rf.AppendEntries(&args, &reply)
		data, err := reply.Marshal()
		if err != nil {
			panic(err)
		}
		return &proto.GenericReply{Data: data}, nil
	case proto.GenericArgs_RequestVote:
		args := proto.RequestVoteArgs{}
		err := args.Unmarshal(in.Data)
		if err != nil {
			panic(err)
		}
		reply := proto.RequestVoteReply{}
		rf.RequestVote(&args, &reply)
		data, err := reply.Marshal()
		if err != nil {
			panic(err)
		}
		return &proto.GenericReply{Data: data}, nil
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
	gArgs := proto.GenericArgs{Method: _method}
	switch _method {
	case proto.GenericArgs_AppendEntries:
		aea, ok := args.(*proto.AppendEntriesArgs)
		if !ok {
			panic("bad conv ae")
		}
		data, err := aea.Marshal()
		if err != nil {
			panic(err)
		}
		gArgs.Data = data
	case proto.GenericArgs_RequestVote:
		rva, ok := args.(*proto.RequestVoteArgs)
		if !ok {
			panic("bad conv rv")
		}
		data, err := rva.Marshal()
		if err != nil {
			panic(err)
		}
		gArgs.Data = data
	default:
		buf := new(bytes.Buffer)
		encoder := labgob.NewEncoder(buf)
		err := encoder.Encode(args)
		if err != nil {
			panic(err)
		}
		gArgs.Data = buf.Bytes()
	}

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

	switch _method {
	case proto.GenericArgs_AppendEntries:
		err := reply.(*proto.AppendEntriesReply).Unmarshal(r.Data)
		if err != nil {
			panic(err)
		}
	case proto.GenericArgs_RequestVote:
		err := reply.(*proto.RequestVoteReply).Unmarshal(r.Data)
		if err != nil {
			panic(err)
		}
	default:
		decoder := labgob.NewDecoder(bytes.NewBuffer(r.Data))
		err = decoder.Decode(reply)
		if err != nil {
			panic(err)
		}
	}

	return true
}
