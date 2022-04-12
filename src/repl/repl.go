package repl

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"6.824/cord"
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

		if len(s) > 1 && s[0] == '%' {

			var cordsv *cord.CordServer
			var ok bool
			if cordsv, ok = repler.(*cord.CordServer); !ok {
				fmt.Println("error: not sv cli, ignoring metrics cmd")
				continue
			}

			if s[1:] == "rfstatesize" {
				fmt.Println(cordsv.Persister.RaftStateSize())
				continue
			}
			if s[1:] == "snapshotsize" {
				fmt.Println(cordsv.Persister.SnapshotSize())
				continue
			}
			fmt.Println("error: unknown metrics")
			continue
		}

		params := strings.Split(s, " ")
		if len(params) < 2 || len(params) > 4 {
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
		case "del":
			cmd.OpType = proto.CmdArgs_DELETE
		default:
			fmt.Printf("invalid command\n")
			continue
		}
		cmd.OpKey = params[1]
		if cmd.OpType != proto.CmdArgs_WATCH && cmd.OpType != proto.CmdArgs_GET {
			if len(params) < 3 {
				fmt.Println("invalid command")
				continue
			}
			cmd.OpVal = params[2]
		}
		args := &proto.ServiceArgs{
			Cmds: []*proto.CmdArgs{&cmd},
		}
		linearizable := true
		if params[0] == "get" && len(params) >= 3 {
			if params[2] == "false" || params[2] == "0" {
				linearizable = false
			}
		}
		args.Linearizable = linearizable
		//ttl: time(ms)
		if len(params) >= 4 {
			var ttl int64
			n, err := fmt.Sscanf(params[3], "%d", &ttl)
			if err != nil || n != 1 {
				fmt.Println("warning: parse ttl failed, ignoring ttl")
			} else {
				cmd.Ttl = time.Now().UnixNano()/1e6 + ttl
			}
		}

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
