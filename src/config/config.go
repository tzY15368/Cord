package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"6.824/raft"
	"google.golang.org/grpc"
)

type ComponentLogLevel struct {
	Raft      string
	Server    string
	Datastore string
}

type CordConfig struct {
	Addrs           []string
	LogLevel        ComponentLogLevel
	SnapshotThres   int
	Me              int
	ApplyTimeoutMil int
}

func (cc *CordConfig) MakeGRPCClients() []*raft.GRPCClient {
	var clis []*raft.GRPCClient

	for i, addr := range cc.Addrs {
		if i == cc.Me {
			clis = append(clis, nil)
			continue
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
	return clis
}

func LoadConfig(cfgPath string) *CordConfig {
	file, err := os.Open(cfgPath)
	if err != nil {
		panic(err)
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}
	c := &CordConfig{}
	err = json.Unmarshal(data, c)
	if err != nil {
		panic(err)
	}
	if c.Me == -1 {
		me := flag.Int("me", -1, "rf.me")
		flag.Parse()
		if *me == -1 || *me > len(c.Addrs)-1 {
			fmt.Println("me should be in range [0,n)")
			os.Exit(1)
		}
		c.Me = *me
	}
	_len := len(c.Addrs)
	if _len%2 != 1 || _len < 3 {
		fmt.Println("expecting odd number(>=3) of nodes, got", len(c.Addrs))
		os.Exit(1)
	}
	return c
}
