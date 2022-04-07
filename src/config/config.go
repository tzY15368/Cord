package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type ComponentLogLevel struct {
	Raft      string
	Server    string
	Datastore string
}

type CordConfig struct {
	Addrs         []string
	LogLevel      ComponentLogLevel
	SnapshotThres int
	Me            int
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
	return c
}
