package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

var nReduce int
var intermediateFiles []*os.File
var workerID string

const baseIntFilename = "map-out-"

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
func handleErr(e error) {
	if e != nil {
		log.Fatalf("worker:%v", e)
	}
}

//
// main/mrworker.go calls this function.
//
func doReduce(reducef func(string, []string) string, taskPtr *WorkerTask) error {
	fname := taskPtr.Filename
	intFile, err := os.Open(fname)
	if err != nil {
		return err
	}
	content, err := ioutil.ReadAll(intFile)
	if err != nil {
		return err
	}
	var id int
	_, err = fmt.Sscanf(fname, "map-out-%d", &id)
	if err != nil {
		return err
	}
	oname := fmt.Sprintf("mr-out-%d", id)
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}

	var intermediate []KeyValue

	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		kv := strings.Split(line, " ")
		if len(kv) == 2 {

			intermediate = append(intermediate, KeyValue{Key: kv[0], Value: kv[1]})
		}
	}

	//json.Unmarshal(content, &intermediate)
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	intFile.Close()
	return nil
}
func doMap(mapf func(string, string) []KeyValue, taskPtr *WorkerTask) error {
	fname := taskPtr.Filename
	file, err := os.Open(fname)
	if err != nil {
		return err
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	kva := mapf(fname, string(content))
	sort.Sort(ByKey(kva))
	for _, kv := range kva {
		_, err := fmt.Fprintf(intermediateFiles[ihash(kv.Key)%nReduce], "%v %v\n", kv.Key, kv.Value)
		if err != nil {
			return err
		}
	}
	return nil
}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		taskPtr := GetNewTask()
		time.Sleep(3 * time.Second)
		fmt.Printf("sleeping before task ")
		var e error
		if taskPtr.TaskType == MAP {
			e = doMap(mapf, taskPtr)
		} else if taskPtr.TaskType == REDUCE {
			e = doReduce(reducef, taskPtr)
		} else if taskPtr.TaskType == END {
			log.Printf("no more tasks, exit")
			os.Exit(0)
		} else if taskPtr.TaskType == WAIT {
			log.Printf("need to wait 3s")
			time.Sleep(1 * time.Second)
		}
		if e != nil {
			log.Fatalf("worker: %v", e)
		}
		TaskComplete(taskPtr)
	}
}

func GetNewTask() *WorkerTask {
	args := NewTaskArgs{}
	reply := WorkerTask{}
	err := call("Coordinator.IssueNewTask", &args, &reply)
	handleErr(err)
	log.Printf("got new [%v] task, fname=%v", reply.TaskType, reply.Filename)
	return &reply
}
func GetNReduce() error {
	reply := &NReduceReply{}

	err := call("Coordinator.GetNReduce", &NReduceArgs{}, reply)
	if err != nil {
		return err
	}
	nReduce = reply.NReduce
	return nil
}
func TaskComplete(argsPtr *WorkerTask) error {
	argsPtr.WorkerID = workerID
	reply := TaskCompleteReply{}
	err := call("Coordinator.CompleteTask", argsPtr, &reply)
	if err != nil {
		return err
	}
	return nil
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}

func init() {
	fname := "log.txt"
	logFile, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile) // 将文件设置为log输出的文件
	log.SetPrefix("[+]")
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.LUTC)
	workerID = md5V(uuid.New().String())[0:3]

	err = GetNReduce()
	if err != nil {
		panic(err)
	}
	for i := 0; i < nReduce; i++ {
		fp, err := os.OpenFile(fmt.Sprintf("%v-"+baseIntFilename+"%d", workerID, i), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)
		if err != nil {
			panic(err)
		}
		intermediateFiles = append(intermediateFiles, fp)
	}
}
