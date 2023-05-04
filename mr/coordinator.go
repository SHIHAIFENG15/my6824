package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	"path/filepath"
)

const TempDir = "tmp"
const TaskTimeoutInteval = 10

type TaskStatus int
type TaskType int

const (
	Idle TaskStatus = iota
	Execusing
	Completed
)

const (
	MapTask TaskType = iota
	ReduceTask
	FinishedTask
)

type Task struct {
	Type TaskType
	Status TaskStatus
	Id int
	File string
	workerId int
}

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	mapTasks []Task
	reduceTasks []Task
	// sub 1 when corresponding task completed, all 0 means done
	nMap int
	nReduce int	
}



// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

// support 2 kind of RPC:
// 1. coordninator -> work, request for task
// 2. work -> coordinator, notify master that the task is done


func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	
	var task *Task

	if c.nMap > 0 {
		task = c.allocateTask(c.mapTasks, args.WorkerId)
	} else if c.nReduce > 0 {
		task = c.allocateTask(c.reduceTasks, args.WorkerId)
	} else {
		task = &Task{FinishedTask, Completed, -1, "", -1}
	}

	reply.TaskFile = task.File
	reply.TaskId = task.Id
	reply.TaskType = task.Type
	reply.nReduce = len(c.reduceTasks)

	c.mu.Unlock()
	
	// new goroutine for time-wait task
	go c.waitForTask(task)
	
	return nil
}

func (c *Coordinator) allocateTask (Candidates []Task, WorkerId int) *Task{
	for i := 0; i < len(Candidates); i++ {
		if Candidates[i].Status == Idle {
			task := Candidates[i]
			task.Status = Execusing
			task.workerId = WorkerId

			return &task
		}
	}

	return &Task{FinishedTask, Completed, -1, "", -1}
}

func (c *Coordinator) waitForTask(task *Task) {
	if task.Type != MapTask && task.Type != ReduceTask {
		return
	}

	<-time.After(time.Second * TaskTimeoutInteval)
	
	c.mu.Lock()
	defer c.mu.Unlock()

	// solution for "straggle"
	if task.Status == Execusing {
		task.Status = Completed
		task.workerId = -1
	}
}

func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	switch args.TaskType {
	case MapTask:
		task = &c.mapTasks[args.TaskId]
	case ReduceTask:
		task = &c.reduceTasks[args.TaskId]
	default:
		return nil
	}

	if task.workerId == args.WorkerId && task.Status == Execusing {
		task.Status = Completed

		if task.Type == MapTask && c.nMap > 0 {
			c.nMap--
		} else if task.Type == ReduceTask && c.nReduce > 0 {
			c.nReduce--
		}
	}

	reply.Exit = c.nMap == 0 && c.nReduce == 0

	return nil
}

//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}


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
		log.Fatal("listen error::", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	ret = (c.nMap == 0 && c.nReduce == 0)

	return ret
}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	nMap := len(files)
	c.nMap = nMap
	c.nReduce = nReduce
	c.mapTasks = make([]Task, 0, c.nMap)
	c.reduceTasks = make([]Task, 0, nReduce)

	for i := 0; i < nMap; i++ {
		curMap := Task{MapTask, Idle, i, files[i], -1}
		c.mapTasks = append(c.mapTasks, curMap)
	}

	for i:= 0; i < nReduce; i++ {
		curReuce := Task{ReduceTask, Idle, i, "", -1}
		c.reduceTasks = append(c.reduceTasks, curReuce)
	}

	c.server()

	// clean up output files and create temp directory
	outFiles, _ := filepath.Glob("mr-out*")
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("Cannot remove file %v\n", f)
		}
	}
	err := os.RemoveAll(TempDir)
	if err != nil {
		log.Fatalf("Cannot remove temp directory %v\n", TempDir)
	}
	err = os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", TempDir)
	}

	return &c
}
