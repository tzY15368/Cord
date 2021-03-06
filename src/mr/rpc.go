package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	MAP = iota
	REDUCE
	END
)

const (
	WAIT = -1
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const baseIntFilename = "map-out-"

type WorkerTask struct {
	TaskType    int
	Filename    string
	IntFilename string
	WorkerID    string
}
type NewTaskArgs struct {
	WorkerID string
}

type TaskCompleteReply struct{}

type NReduceReply struct {
	NReduce int
}
type NReduceArgs struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
