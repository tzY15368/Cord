package main

import (
	"flag"
	"fmt"
	"net"
	"time"

	"6.824/proto"
	"6.824/raft"
	"google.golang.org/grpc"
)

func main() {
	addrs := []string{
		"127.0.0.1:6000",
		"127.0.0.1:6001",
		"127.0.0.1:6002",
	}
	var clis []*raft.GRPCClient
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
		cli := &raft.GRPCClient{
			Conn: conn,
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
	rf := raft.Make(clis, *me, raft.MakePersister(), applyCh)
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
	fmt.Println("serving on addr", addrs[*me])

	proto.RegisterGenericServiceServer(server, rf)
	server.Serve(listener)
}
