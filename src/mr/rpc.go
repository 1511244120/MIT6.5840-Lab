package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Status int

const (
	Idle Status = iota
	Assigned
	Completed
)

type Type int

const (
	Map Type = iota
	Reduce
	Hang
	Done
)

type MapReduceTask struct {
	Inputfiles  []string
	Outputfiles []string

	StartTime    time.Time
	Status       Status
	Type         Type
	TaskPosition int
}

type Protobuf struct {
	TaskNumber int
	Task       MapReduceTask
	NReduce    int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
