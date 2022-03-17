package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrUnexpected  = "ErrUnexpected"
	ErrTimeout     = "ErrTimeout"
)

const (
	OP_PUT    = "PUT"
	OP_GET    = "GET"
	OP_APPEND = "APPEND"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "PUT" or "APPEND"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

func (par *PutAppendReply) SetReplyErr(err Err) {
	par.Err = err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

func (gr *GetReply) SetReplyErr(err Err) {
	gr.Err = err
}

type ReplyInterface interface {
	SetReplyErr(err Err)
}
