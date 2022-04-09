package main

import (
	"flag"
	"fmt"
	"net"
	"strconv"
	"time"

	"6.824/proto"
	"6.824/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
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
	var kacp = keepalive.ClientParameters{
		Time:                1 * time.Second, // send pings every 10 seconds if there is no activity
		Timeout:             time.Second / 2, // wait 1 second for ping back
		PermitWithoutStream: true,            // send pings even without active streams
	}
	for i, addr := range addrs {
		if i == *me {
			clis = append(clis, nil)
			continue
		}
		conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithKeepaliveParams(kacp))
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
			fmt.Println("incoming message:", msg.CommandIndex, msg.Command)
		}
	}()
	rf := raft.Make(clis, *me, raft.MakePersister(), applyCh)
	go func() {
		time.Sleep(3 * time.Second)
		for i := 0; ; i++ {
			//_, k := rf.GetState()
			//if k {
			if i%3 == *me {
				v := "helo" + strconv.Itoa(i)
				rf.Start(proto.ServiceArgs{
					Cmds: []*proto.CmdArgs{{
						OpType: proto.CmdArgs_GET,
						OpKey:  v,
					}},
				})
			}
			//}
			time.Sleep(1 * time.Second)
		}
	}()
	fmt.Println("serving on addr", addrs[*me])

	proto.RegisterGenericServiceServer(server, rf)
	server.Serve(listener)
}
