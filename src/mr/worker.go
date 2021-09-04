package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
func doMap(mapf func(string, string) []KeyValue, task WorkerTask) error {

	// fname := GetNewMapTask()

	// if fname == "" {
	// 	log.Printf("no more tasks, waiting for reduce to start")
	// 	return true
	// }
	fname := task.Filename
	file, err := os.Open(fname)
	if err != nil {
		return err
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}

	intFilename := fmt.Sprintf("map-out-%d", ihash(string(content))%5)
	task.IntFilename = intFilename

	kva := mapf(fname, string(content))
	var intFP *os.File
	var outBytes []byte
	if _, err := os.Stat(intFilename); os.IsExist(err) {
		intFP, err = os.Open(intFilename)
		if err != nil {
			return err
		}
		outBytes, err = ioutil.ReadAll(intFP)
		if err != nil {
			return err
		}
		var tmpKVA []KeyValue
		err = json.Unmarshal(outBytes, &tmpKVA)
		if err != nil {
			return err
		}
		kva = append(kva, tmpKVA...)
	} else {
		intFP, err = os.Create(intFilename)
		if err != nil {
			return err
		}
	}

	outBytes, err = json.Marshal(kva)
	if err != nil {
		return err
	}
	intFP.Write(outBytes)
	intFP.Close()
	return nil
}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		task := GetNewTask()
		if task.TaskType == MAP {
			doMap(mapf, task)
		} else if task.TaskType == REDUCE {

		} else if task.TaskType == END {
			log.Fatalf("no more tasks, exit")
		} else if task.TaskType == WAIT {
			log.Printf("need to wait 3s")
			time.Sleep(3 * time.Second)
		}
		TaskComplete(task)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func GetNewTask() WorkerTask {
	args := NewTaskArgs{}
	reply := WorkerTask{}
	err := call("Coordinator.IssueNewTask", &args, &reply)
	handleErr(err)
	log.Printf("got new [%v] task, fname=%v", reply.TaskType, reply.Filename)
	return reply
}

func TaskComplete(args WorkerTask) error {
	reply := TaskCompleteReply{}
	err := call("Coordinator.CompleteTask", &args, &reply)
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
