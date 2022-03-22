package kvraft

import (
	"bytes"
	"sync"

	"6.824/labgob"
	"github.com/sirupsen/logrus"
)

// put 和 append的int参数用于指定期待的index，该index来自kv.rf.start
type KVInterface interface {
	// EvalOp return value for GET, and "" for PUT/APPEND
	EvalOp(Op, bool) (OPResult, []byte)
	Load([]byte) error
}

type SimpleKVStore struct {
	data   map[string]string
	mu     sync.Mutex
	logger *logrus.Entry
	ack    map[int64]int64 // client's latest request id (for deduplication)
}

func NewKVStore(logger *logrus.Entry) KVInterface {
	sks := &SimpleKVStore{
		data:   make(map[string]string),
		logger: logger,
		ack:    make(map[int64]int64),
	}
	logger.Debug("kvstore: started kvstore")
	return sks
}

func (sk *SimpleKVStore) isDuplicate(request RequestInfo) bool {
	latestRequestId, ok := sk.ack[request.ClientID]
	if ok {
		return latestRequestId >= request.RequestID
	}
	return false
}

func (sk *SimpleKVStore) EvalOp(op Op, dump bool) (OPResult, []byte) {
	sk.mu.Lock()
	defer sk.mu.Unlock()
	opRes := OPResult{
		err:         nil,
		requestInfo: op.RequestInfo,
	}
	if sk.isDuplicate(op.RequestInfo) {
		if op.OpType == OP_GET {
			opRes.data = sk.data[op.OpKey]
		}
		goto Return
	}

	// register in ack
	sk.ack[op.RequestInfo.ClientID] = op.RequestInfo.RequestID

	switch op.OpType {
	case OP_GET:
		data, ok := sk.data[op.OpKey]
		if ok {
			opRes.data = data
		} else {
			opRes.err = ErrKeyNotFound
		}
	case OP_PUT:
		sk.data[op.OpKey] = op.OPValue
	case OP_APPEND:
		sk.data[op.OpKey] += op.OPValue
	}

Return:
	var dumpData []byte
	var dumpErr error
	if dump {
		dumpData, dumpErr = sk.unsafeDump()
		if dumpErr != nil {
			opRes.err = ErrDumpFail
		}
	}
	return opRes, dumpData
}

// unsafeDump not threadsafe, dumps db to []byte
func (sk *SimpleKVStore) unsafeDump() ([]byte, error) {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err := encoder.Encode(&sk.data)
	if err != nil {
		return nil, err
	}
	err = encoder.Encode(&sk.ack)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (sk *SimpleKVStore) Load(data []byte) error {
	sk.mu.Lock()
	defer sk.mu.Unlock()
	decoder := labgob.NewDecoder(bytes.NewBuffer(data))
	err := decoder.Decode(&sk.data)
	if err != nil {
		return err
	}
	err = decoder.Decode(&sk.ack)
	if err != nil {
		return err
	}
	return nil

}
