package mr

import (
	"fmt"
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
	Running
	Finished
)

type Task struct {
	Filename    string
	IntFilename string
	State       int
	startTime   int64
	taskType    int
}

type Coordinator struct {
	// Your definitions here.
	currentState int
	nReduce      int
	tasks        map[string]*Task
	mu           sync.Mutex
}

// 需要缓冲区，只有成功完成的任务才能提交到实际输出文件
// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) CompleteTask(args *WorkerTask, reply *TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case MAP:
		taskPtr := c.tasks[args.Filename]
		if taskPtr != nil {
			taskPtr.State = Finished
			taskPtr.IntFilename = args.IntFilename

		}
		log.Printf("mapped %v", args.Filename)
	case REDUCE:
		taskPtr := c.tasks[args.Filename]
		if taskPtr != nil {
			taskPtr.State = Finished
		}
		log.Printf("reduced %v", args.Filename)
	case END:
		log.Printf("ended")
	case WAIT:
		log.Printf("waited")
	}
	return nil
}
func (c *Coordinator) getCurrentTasks() []*Task {
	var tasks []*Task
	for _, task := range c.tasks {
		if task.taskType == c.currentState {
			tasks = append(tasks, task)
		}
	}
	return tasks
}
func (c *Coordinator) getCurrentFinishedCount() int {
	i := 0
	for _, task := range c.getCurrentTasks() {
		if task.State == Finished {
			i++
		}
	}
	return i
}
func (c *Coordinator) GetNReduce(args *NReduceArgs, reply *NReduceReply) error {
	reply.NReduce = c.nReduce
	return nil
}
func (c *Coordinator) IssueNewTask(args *NewTaskArgs, reply *WorkerTask) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentTasks := c.getCurrentTasks()
	// 首先检查当前状态的任务完成了没

	finishedCount := c.getCurrentFinishedCount()
	if finishedCount == len(currentTasks) {
		if c.currentState != END {
			c.currentState++
		}
		log.Printf("current stage finished, moving on to %v", c.currentState)
		currentTasks = c.getCurrentTasks()
	}

	reply.TaskType = c.currentState
	if reply.TaskType == END {
		return nil
	}

	for _, task := range currentTasks {
		if task.State == Pending {
			task.State = Running
			task.startTime = time.Now().Unix()
			reply.Filename = task.Filename
			log.Printf("issue %v %v", task.taskType, task.Filename)
			break
		}
	}
	if reply.Filename == "" {
		log.Printf("waiting for other process to finsh")

		reply.TaskType = WAIT
	}
	return nil
}
func (c *Coordinator) checkTimeout() {
	for {
		time.Sleep(1 * time.Second)
		log.Printf("[*] Timeout check")
		now := time.Now().Unix()
		if c.currentState == END {
			break
		}
		c.mu.Lock()
		ct := c.getCurrentTasks()
		for _, task := range ct {
			if task.State != Finished && task.startTime != 0 && now-task.startTime < TimeoutSeconds {
				continue
			}
			// this means a worker crash happend
			// we should rollback the state
			if task.State == Running {
				log.Printf("found timeout in [%v] %v", task.taskType, task.Filename)
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
	return c.currentState == END
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
	c.nReduce = nReduce
	// Your code here.
	for _, fname := range os.Args[1:] {
		c.tasks[fname] = &Task{
			Filename: fname,
			State:    Pending,
			taskType: MAP,
		}
	}
	for i := 0; i < c.nReduce; i++ {

		c.tasks[fmt.Sprintf(baseIntFilename+"%d", i)] = &Task{
			Filename: fmt.Sprintf(baseIntFilename+"%d", i),
			taskType: REDUCE,
			State:    Pending,
		}
	}
	log.Printf("loaded %d files", len(c.tasks))
	c.server()
	go c.checkTimeout()

	return &c
}
func init() {
	fname := "log.txt"
	logFile, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile) // 将文件设置为log输出的文件
	log.SetPrefix("[-]")
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.LUTC)
}
