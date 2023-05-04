package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const ScheduleInterVal = 10

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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// poll for task
	for {
		reply, ok := RequestTask()

		if !ok {
			log.Fatalf("Failed to contact master, worker exiting.")
			return
		}

		var exit, disconnect bool

		switch reply.TaskType {
		case MapTask :
			doMap(mapf, reply)
			exit, disconnect = Report(MapTask, reply.TaskId)
		case ReduceTask:
			doReduce(reducef, reply)
			exit, disconnect = Report(ReduceTask, reply.TaskId) 
		case FinishedTask:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", reply.TaskType))
		}

		if exit || disconnect {
			log.Fatalf("All Task Done or master disconnected, Worker exits.")
			return
		}

		time.Sleep(ScheduleInterVal * time.Millisecond)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func CallExample() {

	// declare an argument structure.
//	args := ExampleArgs{}

	// fill in the argument(s).
//	args.X = 99

	// declare a reply structure.
//	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
		// reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}

func RequestTask() (*RequestTaskReply, bool) {
	args := RequestTaskArgs{os.Getpid()}
	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)

	return &reply, ok
}

func Report(TaskType TaskType, TaskId int) (bool, bool) {
	args := ReportArgs{TaskType, TaskId, os.Getpid()}
	reply := ReportReply{}

	ok := call("Coordinator.Report", &args, &reply)

	return reply.Exit, ok
}

func doMap(mapf func(string, string) []KeyValue, reply *RequestTaskReply) {
	filepath := reply.TaskFile
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("doMap can not open %v", filepath)
	}
	defer file.Close()
	content, error := ioutil.ReadAll(file)
	if error != nil {
		log.Fatalf("doMap can not read %v", filepath)
	}
	
	kvs := mapf(filepath, string(content))

	MapOutput(kvs, reply.TaskId, reply.nReduce)
}


func MapOutput(kvs []KeyValue, mapId int, nReduce int) {
	prefix := fmt.Sprintf("%v/mr-%v", TempDir, mapId)

	// build int-kvArray hash for next intermediate file write, bad when mermory limit
	idxKvs := make(map[int][]KeyValue)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % nReduce
		idxKvs[idx] = append(idxKvs[idx], kv)
	}

	for reduceId, kvs := range idxKvs {
		// temporary file dedicated to the worker will be renamed after it is written
		// The goal is to keep the writing atomicity
		filePath := fmt.Sprintf("%v-%v-%v", prefix, reduceId, os.Getpid())
		file, err := os.Create(filePath)
		if err != nil {
			log.Fatalf("MapOutput can not create file %v", filePath)
		}
		defer file.Close()
		buf := bufio.NewWriter(file)
		encoder := json.NewEncoder(buf)

		for _, kv := range kvs {
			error := encoder.Encode(&kv)
			if error != nil {
				log.Fatalf("MapOutput can not encode kv :%v", kv)
			}
		}

		// flush to local disk
		buf.Flush()

		newPath := fmt.Sprintf("%v-%v", prefix, reduceId)
		renameErr := os.Rename(filePath, newPath)
		if renameErr != nil {
			log.Fatalf("MapOutput can not rename file :%v, newName is : %v", filePath, newPath)
		}
	}
}

func doReduce(reducef func(string, []string) string, reply *RequestTaskReply) {
	reduceId := reply.TaskId
	// 
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TempDir, "*", reduceId))
	if err != nil {
		log.Fatalf("doReuce search files for reduce %v failed", reduceId)
	}

	intermediate := []KeyValue{}
	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("doRuce can not Open file %v", filePath)
		}
		Decoder := json.NewDecoder(file)

		var kv KeyValue

		for Decoder.More() {
			decErr := Decoder.Decode(&kv)
			if decErr != nil {
				log.Fatalf("Exception occurs when encode file %v", filePath) 
			}

			intermediate = append(intermediate, kv) 
		}
	}

	ReduceOutput(reducef, reply, intermediate)

}

func ReduceOutput(reducef func(string, []string) string, 
reply *RequestTaskReply, intermediate []KeyValue) {
	// must order firstly
	sort.Sort(ByKey(intermediate))
	// file
	reduceId := reply.TaskId
	filePath := fmt.Sprintf("%v/mr-out-%v-%v", TempDir, reduceId, os.Getpid())
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("ReduceOutput can not create new file %v", filePath)
	}

	defer file.Close()

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
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	newPath := fmt.Sprintf("mr-out-%v", reduceId)
	renameErr := os.Rename(filePath, newPath)
	if renameErr != nil {
		log.Fatalf("reduceOutput can not rename file %v", filePath)
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returnsRequestTaskReply true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
