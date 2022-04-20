package common

import (
	"bytes"
	crand "crypto/rand"
	"encoding/gob"
	"math/big"
	"math/rand"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	RaftLogLevel     = logrus.InfoLevel
	KVServerLogLevel = logrus.WarnLevel
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
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func EncodeCommand(data interface{}) []byte {
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(&data)
	// d, err := json.Marshal(&data)
	if err != nil {
		panic(err)
	}
	//fmt.Printf("%T encoding %+v\n", data, data)
	return buf.Bytes()
	//return d
}

func DecodeCommand(data []byte) interface{} {
	var r interface{}
	decoder := gob.NewDecoder(bytes.NewBuffer(data))
	err := decoder.Decode(&r)
	//err := json.Unmarshal(data, &r)

	if err != nil {
		panic(err)
	}
	//fmt.Printf("%T decoding %+v\n", r, r)
	return r
}

// will generate len(rangeEnd) slice anyways
func GenRandomBytes(rangeStart int, rangeEnd int) *[]byte {
	diff := rangeEnd - rangeStart
	r := rand.Intn(diff)
	tokens := make([]byte, rangeEnd)
	_, err := rand.Read(tokens)
	tokenSlice := tokens[:rangeStart+r]
	if err != nil {
		panic(err)
	}
	return &tokenSlice
}
