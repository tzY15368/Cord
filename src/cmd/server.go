package main

import (
	"flag"
	"fmt"
	"os"

	"6.824/config"
	"6.824/logging"
)

func main() {
	config := config.LoadConfig("config.json")
	logging.PrepareLogger("raft", config.LogLevel.Raft)
	logging.PrepareLogger("server", config.LogLevel.Server)
	logging.PrepareLogger("dataStore", config.LogLevel.Datastore)

	_len := len(config.Addrs)
	if _len%2 != 1 || _len < 3 {
		fmt.Println("expecting odd number(>=3) of nodes, got", len(config.Addrs))
		os.Exit(1)
	}

	if config.Me == -1 {
		me := flag.Int("me", -1, "rf.me")
		flag.Parse()
		if *me == -1 || *me > _len-1 {
			fmt.Println("me should be in range [0,n)")
			os.Exit(1)
		}
		config.Me = *me
	}

	fmt.Printf("server: starting with config:\n %+v\n", config)

}
