package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"6.824/config"
	"6.824/cord"
	"6.824/logging"
	"6.824/proto"
	"6.824/repl"
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

	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", outBoundPortBase+config.Me))
	if err != nil {
		panic(err)
	}
	server := grpc.NewServer()
	cordServer := cord.NewCordServer(config)
	proto.RegisterExternalServiceServer(server, cordServer)
	fmt.Printf("server: starting with config:\n %+v\n", config)
	fmt.Printf("server: outbound: listening on %s\n", listener.Addr())
	if config.StartWithCLI {
		go func() {
			repl.RunREPL(cordServer)
			//repl(nil)
		}()
	}
	go func() {
		c := make(chan os.Signal, 2)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		fmt.Println("ctrl-c")
		os.Exit(2)
	}()
	server.Serve(listener)
}
