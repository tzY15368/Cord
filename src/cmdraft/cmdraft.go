package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net"
	"time"

	"6.824/labgob"
	"6.824/raft"
	"6.824/raft/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type GRPCClient struct {
	conn *grpc.ClientConn
}

type GenericService struct {
	rf *raft.Raft
}

func (gs *GenericService) HandleCall(ctx context.Context, in *raftpb.GenericArgs) (*raftpb.GenericReply, error) {
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
		gs.rf.AppendEntries(&args, &reply)
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
		gs.rf.RequestVote(&args, &reply)
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
		gs.rf.InstallSnapshot(&args, &reply)
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
	c2 := raftpb.NewGenericServiceClient(c.conn)
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

func main() {
	addrs := []string{
		"127.0.0.1:6000",
		"127.0.0.1:6001",
		"127.0.0.1:6002",
	}

	var clis []*GRPCClient
	me := flag.Int("me", -1, "rf.me")
	flag.Parse()
	for i, addr := range addrs {
		if i == *me {
			clis = append(clis, nil)
		}
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			panic(err)
		}
		cli := &GRPCClient{
			conn: conn,
		}
		clis = append(clis, cli)
	}
	server := grpc.NewServer()
	listener, err := net.Listen("tcp", addrs[*me])
	if err != nil {
		panic(err)
	}
	applyCh := make(chan raft.ApplyMsg)
	go func() {
		for msg := range applyCh {
			fmt.Println("incoming message:", msg.CommandIndex)
		}
	}()
	iclis := make([]raft.Callable, len(clis))
	for i := range clis {
		iclis[i] = clis[i]
	}
	rf := raft.Make(iclis, *me, raft.MakePersister(), applyCh)
	go func() {
		time.Sleep(3 * time.Second)
		for {
			_, k := rf.GetState()
			if k {
				rf.Start("helo")
			}
			time.Sleep(1 * time.Second)
		}
	}()
	gs := GenericService{
		rf: rf,
	}
	fmt.Println("serving on addr", addrs[*me])

	raftpb.RegisterGenericServiceServer(server, &gs)
	server.Serve(listener)
}
