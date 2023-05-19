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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type WorkerTaskRqst struct {
}

type MastersReply struct {
	TaskID           int
	FileName         string
	TaskType         int // Map or Reduce
	AllDone          bool
	TempMapLocations []string
	NReduce          int
}

type ReportonMap struct {
	TaskID           int
	TempMapLocations []string
	Stage            int
}

type ReportonMapReply struct{}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

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
