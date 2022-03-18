package common

import (
	"time"

	"github.com/sirupsen/logrus"
)

const (
	RaftLogLevel     = logrus.WarnLevel
	KVServerLogLevel = logrus.WarnLevel
	KVStoreLogLevel  = logrus.WarnLevel
)
const (
	ApplyCHTimeout = 15 * time.Second
)

func Min(i int, j int) int {
	if i > j {
		return i
	}
	return j
}
