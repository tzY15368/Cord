package common

import (
	"time"

	"github.com/sirupsen/logrus"
)

const (
	RaftLogLevel     = logrus.InfoLevel
	KVServerLogLevel = logrus.DebugLevel
	KVStoreLogLevel  = logrus.InfoLevel
)
const (
	ApplyCHTimeout = 15 * time.Second
)

func Min(i int, j int) int {
	if i > j {
		return j
	}
	return i
}

func Max(i int, j int) int {
	if i > j {
		return i
	}
	return j
}
