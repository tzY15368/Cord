package repl

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"6.824/proto"
)

type IRepl interface {
	HandleRequest(context.Context, *proto.ServiceArgs) (*proto.ServiceReply, error)
	CreateRequestInfo() proto.RequestInfo
}

func RunREPL(repler IRepl) {
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
		if repler != nil {

			info := repler.CreateRequestInfo()
			args.Info = &info
			ctx, _ := context.WithTimeout(context.TODO(), 2*time.Second)
			reply, err := repler.HandleRequest(ctx, args)
			if err != nil {
				fmt.Printf("got error: %s\n", err.Error())
			} else {
				fmt.Printf("%+v\n", reply.Result)
			}
		}
	}
}
