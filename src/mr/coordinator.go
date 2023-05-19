package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// STEP 1 : Define custom structs for map task , reduce task and for the coordinator
type MapTask struct {
	id       int
	stage    int
	fileName string
}

type ReduceTask struct {
	id    int
	stage int
}

type Coordinator struct {
	// Your definitions here.
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	nReduce     int
	// We need mutex to avoid race conditions on different workers
	// to prevent discrepancies in results of counts.
	mu sync.Mutex
	// Each hash val would have unknown keyval pairs mapped to it.
	// Usually the count of hash values would be the nReduce count.
	tempMapFiles [][]string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) SendTaskToWorker(args *WorkerTaskRqst, reply *MastersReply) error {

	// Lock the master
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if all map tasks are done . If not send map task to worker
	if !c.checkAllMapTasksDone() {
		for idx, mapTa := range c.mapTasks {
			if mapTa.stage == 0 {
				reply.AllDone = false
				reply.FileName = mapTa.fileName
				reply.NReduce = c.nReduce
				reply.TaskID = idx
				reply.TaskType = 1 // 1 for map 2 for reduce

				mapTa.stage = 1
				c.mapTasks[idx] = mapTa

				// Wait for 10 secs for worker to finish the map task
				go TenSecondsMap(mapTa.id, c)

				return nil
			}
		}
	} else if !c.checkAllReduceTasksDone() {
		for idx, rTa := range c.reduceTasks {
			if rTa.stage == 0 {
				reply.AllDone = false
				reply.NReduce = c.nReduce
				reply.TaskID = rTa.id
				reply.TaskType = 2
				tempFileNames := make([]string, 0)
				// While running map tasks, each worker returns the temp file locations
				// for the map tasks performed by it for each of the file with ids from [0...len(files))
				// So we now branch over each of those temp files
				// We appedned temp files in reportMapcompletion function
				for _, i := range c.tempMapFiles {
					tempFileNames = append(tempFileNames, i[rTa.id])
				}
				reply.TempMapLocations = tempFileNames
				rTa.stage = 1
				c.reduceTasks[idx] = rTa

				// Wait for worker to complete reduce task

				go TenSecondsReduce(rTa.id, c)
				return nil
			}

		}
	} else {
		reply.AllDone = true
		return nil
	}

	return nil
}

func (c *Coordinator) ReportMapCompletion(rqst *ReportonMap, reply *ReportonMapReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.tempMapFiles[rqst.TaskID] = rqst.TempMapLocations

	if rqst.Stage == 2 {
		c.mapTasks[rqst.TaskID].stage = 2
	} else {
		c.mapTasks[rqst.TaskID].stage = 0
	}
	return nil
}

func (c *Coordinator) ReportReduceCompletion(rqst *ReportonMap, reply *ReportonMapReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if rqst.Stage == 2 {
		c.reduceTasks[rqst.TaskID].stage = 2
	} else {
		c.reduceTasks[rqst.TaskID].stage = 0
	}
	return nil
}

func TenSecondsMap(taskID int, c *Coordinator) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()

	// Checking if num of hashbuckets = nReduce
	if len(c.tempMapFiles[taskID]) == c.nReduce {
		// if yes then map stage is complete
		c.mapTasks[taskID].stage = 2
	} else {
		c.mapTasks[taskID].stage = 0
	}
	return
}

func TenSecondsReduce(taskID int, c *Coordinator) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()

	intermediateFileName := "mr-out" + strconv.Itoa(taskID)
	if _, err := os.Stat(intermediateFileName); err == nil {
		c.reduceTasks[taskID].stage = 2
	} else {
		c.reduceTasks[taskID].stage = 0
	}

	return
}

func (c *Coordinator) checkAllMapTasksDone() bool {
	for _, mt := range c.mapTasks {
		if mt.stage != 2 {
			return false
		}
	}
	return true
}

func (c *Coordinator) checkAllReduceTasksDone() bool {
	for _, mt := range c.reduceTasks {
		if mt.stage != 2 {
			return false
		}
	}
	return true
}

// an example RPC handler
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.checkAllMapTasksDone() && c.checkAllReduceTasksDone() {
		return true
	}

	return ret
}

// STEP2 INITIALIZE COORDINATOR METHOD
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	fmt.Println(c.nReduce)
	c.mapTasks = make([]MapTask, len(files))
	for index, file := range files {
		c.mapTasks[index] = MapTask{fileName: file, id: index, stage: 0}
	}

	c.reduceTasks = make([]ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{id: i, stage: 0}
	}

	// This file's nReduce num of intermediate files are in this locn
	c.tempMapFiles = make([][]string, len(files))

	c.server()
	return &c
}
