package main

import (
	"fmt"
	"net"

	"6.824/config"
	"6.824/cord"
	"6.824/logging"
	"6.824/proto"
	"google.golang.org/grpc"
)

const (
	outBoundPortBase = 7500
)

func main() {
	config := config.LoadConfig("config.json")
	logging.PrepareLogger("raft", config.LogLevel.Raft)
	logging.PrepareLogger("server", config.LogLevel.Server)
	logging.PrepareLogger("dataStore", config.LogLevel.Datastore)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", outBoundPortBase+config.Me))
	if err != nil {
		panic(err)
	}
	server := grpc.NewServer()
	cordServer := cord.NewCordServer(config)
	proto.RegisterExternalServiceServer(server, cordServer)
	fmt.Printf("server: starting with config:\n %+v\n", config)
	fmt.Printf("server: outbound: listening on %s", listener.Addr())
	server.Serve(listener)
}
