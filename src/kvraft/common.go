package kvraft

import (
	"errors"

	"6.824/common"
)

var (
	ErrOK          = errors.New("OK")
	ErrKeyNotFound = errors.New("Errkeynotfound")
	ErrWrongLeader = errors.New("ErrWrongLeader")
	ErrUnexpected  = errors.New("ErrUnexpected")
	ErrTimeout     = errors.New("ErrTimeout")
	ErrDumpFail    = errors.New("ErrDumpFail")
)

const (
	OP_PUT    = "PUT"
	OP_GET    = "GET"
	OP_APPEND = "APPEND"
)

type OPResult struct {
	data        string
	err         error
	requestInfo common.RequestInfo
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "PUT" or "APPEND"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	common.RequestInfo
}

type Err string

type PutAppendReply struct {
	Err Err
	RV  int
}

func (par *PutAppendReply) SetReplyErr(err error) {
	if err != nil {
		par.Err = Err(err.Error())
	} else {
		par.Err = Err(ErrOK.Error())
	}
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	common.RequestInfo
}

type GetReply struct {
	Err   Err
	Value string
}

func (gr *GetReply) SetReplyErr(err error) {
	if err != nil {
		gr.Err = Err(err.Error())
	} else {
		gr.Err = Err(ErrOK.Error())
	}
}

type ReplyInterface interface {
	SetReplyErr(err error)
}
