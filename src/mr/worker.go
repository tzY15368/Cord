package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

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
func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
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

	json.Unmarshal(content, &intermediate)
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

	intFilename := fmt.Sprintf("map-out-%d", ihash(string(content))%5)
	taskPtr.IntFilename = intFilename

	kva := mapf(fname, string(content))
	//var intFP *os.File
	var outBytes []byte
	log.Printf("int file: %v", intFilename)
	existence, _ := pathExists(intFilename)
	if existence {
		log.Printf("out %v exists, appending", intFilename)
		intFP, err := os.OpenFile(intFilename, os.O_RDWR, 0755)
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
		log.Printf("old kva len: %d, new kva len: %d, file is %s, tmpfileis %s", len(tmpKVA), len(kva), fname, intFilename)
		resultBytes, err := json.Marshal(kva)
		if err != nil {
			return err
		}
		intFP.Seek(0, io.SeekStart)
		n, err := intFP.Write(resultBytes)
		if err != nil {
			return err
		}
		log.Printf("n=%d", n)
		intFP.Close()
	} else {
		log.Printf("out %v doesnt exist", intFilename)
		intFP, err := os.Create(intFilename)
		if err != nil {
			return err
		}
		resultBytes, err := json.Marshal(kva)
		if err != nil {
			return err
		}
		intFP.Write(resultBytes)
		intFP.Close()
	}
	return nil
}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		taskPtr := GetNewTask()
		var e error
		if taskPtr.TaskType == MAP {
			e = doMap(mapf, taskPtr)
		} else if taskPtr.TaskType == REDUCE {
			e = doReduce(reducef, taskPtr)
		} else if taskPtr.TaskType == END {
			log.Fatalf("no more tasks, exit")
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

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func GetNewTask() *WorkerTask {
	args := NewTaskArgs{}
	reply := WorkerTask{}
	err := call("Coordinator.IssueNewTask", &args, &reply)
	handleErr(err)
	log.Printf("got new [%v] task, fname=%v", reply.TaskType, reply.Filename)
	return &reply
}

func TaskComplete(argsPtr *WorkerTask) error {
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
