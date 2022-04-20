package benches

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"6.824/clerk"
	"6.824/common"
	"6.824/proto"
)

func TestSpeed(t *testing.T) {
	total := 5000
	NClerks := 32
	Nops := total / NClerks
	servers := []string{
		"10.9.116.177:7500",
		"10.9.116.177:7501",
		"10.9.116.177:7502",
	}
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < NClerks; i++ {
		wg.Add(1)
		cs := clerk.NewClerk(servers)
		go func(ii int) {
			defer wg.Done()
			for j := 0; j < Nops; j++ {
				info := cs.CreateRequestInfo()
				baseKey := fmt.Sprintf("x-test-%d-%d", ii, j)
				cmd := []*proto.CmdArgs{
					{
						OpKey: baseKey,
					},
				}
				cmdarg := cmd[0]
				switch j % 10 {
				case 0:
					cmdarg.OpType = proto.CmdArgs_PUT
					cmdarg.OpVal = string(*common.GenRandomBytes(9, 10))
				case 9:
					cmdarg.OpType = proto.CmdArgs_GET
				default:
					// if j < 5 {

					// }
					cmdarg.OpType = proto.CmdArgs_APPEND
					cmdarg.OpVal = string(*common.GenRandomBytes(9, 10))
				}
				_, err := cs.HandleRequest(context.TODO(), &proto.ServiceArgs{
					Cmds:         cmd,
					Info:         &info,
					Linearizable: true,
				})
				if err != nil {
					panic(err)
				}
			}
		}(i)
	}
	wg.Wait()
	fmt.Println(time.Since(start))
	fmt.Println("nclient:", NClerks)
	fmt.Println("total ops:", Nops*NClerks)
}
