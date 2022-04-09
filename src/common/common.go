package common

import (
	"crypto/rand"
	"math/big"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	RaftLogLevel     = logrus.InfoLevel
	KVServerLogLevel = logrus.DebugLevel
	KVStoreLogLevel  = logrus.InfoLevel
	ShardCtlLogLevel = logrus.InfoLevel
	ShardKVLogLevel  = logrus.DebugLevel
)
const (
	ApplyCHTimeout = 800 * time.Millisecond
	StateSizeDiff  = 50
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

type RequestInfo struct {
	ClientID  int64
	RequestID int64
}

func (ri1 *RequestInfo) Equals(ri2 *RequestInfo) bool {
	return ri1.ClientID == ri2.ClientID && ri1.RequestID == ri2.RequestID
}

func Nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
