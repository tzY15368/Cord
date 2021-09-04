package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const TimeoutSeconds = 5

const (
	Pending = iota
	MapRunning
	MapFinished
	ReduceRunning
	ReduceFinished
)

type Task struct {
	Filename    string
	IntFilename string
	State       int
	startTime   int64
}

type Coordinator struct {
	// Your definitions here.
	currentState string
	tasks        map[string]*Task
	mu           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) CompleteTask(args *WorkerTask, reply *TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case MAP:
		taskPtr := c.tasks[args.Filename]
		if taskPtr != nil {
			taskPtr.State = MapFinished
			taskPtr.IntFilename = args.IntFilename

		}
		log.Printf("mapped %v %v", args.TaskType, args.Filename)
	case REDUCE:
		log.Printf("reduced")
	case END:
		log.Printf("ended")
	case WAIT:
		log.Printf("waited")
	}
	return nil
}
func (c *Coordinator) IssueNewTask(args *NewTaskArgs, reply *WorkerTask) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.currentState {
	case MAP:
		// 这里有问题
		reply.TaskType = MAP
		for _, task := range c.tasks {
			if task.State == Pending {
				task.State = MapRunning
				reply.Filename = task.Filename
				log.Printf("issue map %v", task.Filename)
				break
			} else {
				continue
			}
		}
		if reply.Filename == "" {
			reply.TaskType = WAIT
		}

	case REDUCE:
		log.Printf("reduce issue")
		reply.TaskType = END
	case END:
		reply.TaskType = END
	}
	mapFinishedCount := 0
	reduceFinishedCount := 0
	for _, task := range c.tasks {
		if task.State == MapFinished {
			mapFinishedCount++
		} else if task.State == ReduceFinished {
			reduceFinishedCount++
		}
	}
	if c.currentState == MAP && mapFinishedCount == len(c.tasks) {
		log.Printf("mapping done\n")
		c.currentState = REDUCE
	}
	if c.currentState == REDUCE && reduceFinishedCount == len(c.tasks) {
		log.Printf("reduce done")
		c.currentState = END
	}
	return nil
}
func (c *Coordinator) checkTimeout() {
	for {
		time.Sleep(1 * time.Second)
		log.Printf("[*] Timeout check")
		now := time.Now().Unix()
		c.mu.Lock()
		for _, task := range c.tasks {
			if task.startTime != 0 && now-task.startTime < TimeoutSeconds {
				continue
			}
			// this means a worker crash happend
			// we should rollback the state
			if task.State == ReduceRunning {
				log.Printf("found timeout in [%v] %v", task.State, task.Filename)
				task.State = MapFinished
			} else if task.State == MapRunning {
				log.Printf("found timeout in [%v] %v", task.State, task.Filename)
				task.State = Pending
			}
		}
		c.mu.Unlock()
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range c.tasks {
		if task.State != ReduceFinished {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.tasks = make(map[string]*Task)
	c.currentState = MAP
	// Your code here.
	for _, fname := range os.Args[1:] {
		c.tasks[fname] = &Task{
			Filename: fname,
			State:    Pending,
		}
	}
	log.Printf("loaded %d files", len(c.tasks))
	c.server()
	go c.checkTimeout()
	return &c
}
