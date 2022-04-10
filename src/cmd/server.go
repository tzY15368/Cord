package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"6.824/config"
	"6.824/cord"
	"6.824/logging"
	"6.824/proto"
	"google.golang.org/grpc"
)

const (
	outBoundPortBase = 7500
)

func repl(cs *cord.CordServer) {
	reader := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf(">_ ")
		if !reader.Scan() {
			break
		}
		s := reader.Text()

		if strings.EqualFold(s, "exit") {
			fmt.Printf("bye\n")
			os.Exit(0)
		}
		params := strings.Split(s, " ")
		if len(params) < 2 || len(params) > 3 {
			fmt.Printf("invalid params\n")
			continue
		}
		cmd := proto.CmdArgs{}
		switch params[0] {
		case "get":
			cmd.OpType = proto.CmdArgs_GET
		case "append":
			cmd.OpType = proto.CmdArgs_APPEND
		case "put":
			cmd.OpType = proto.CmdArgs_PUT
		case "watch":
			cmd.OpType = proto.CmdArgs_WATCH
		default:
			fmt.Printf("invalid command\n")
			continue
		}
		cmd.OpKey = params[1]
		if cmd.OpType != proto.CmdArgs_WATCH && cmd.OpType != proto.CmdArgs_GET {
			cmd.OpVal = params[2]
		}
		args := &proto.ServiceArgs{
			Cmds: []*proto.CmdArgs{&cmd},
		}
		linearizable := true
		if params[0] == "get" && len(params) == 3 {
			if params[2] == "false" || params[2] == "0" {
				linearizable = false
			}
		}
		args.Linearizable = linearizable
		if cs != nil {

			info := cs.CreateRequestInfo()
			args.Info = &info
			ctx, _ := context.WithTimeout(context.TODO(), 2*time.Second)
			reply, err := cs.HandleRequest(ctx, args)
			if err != nil {
				fmt.Printf("got error: %s\n", err.Error())
			} else {
				fmt.Printf("%+v\n", reply.Result)
			}
		}
	}
}

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
			repl(cordServer)
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
