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

type Coordinator struct {
	// Your definitions here.
	inputFiles []string
	nReduce    int

	mapTasks    []MapReduceTask
	reduceTasks []MapReduceTask

	mapComplete    bool
	reduceComplete bool

	mapCount    int
	reduceCount int

	lock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) NotifyComplete(arg *Protobuf, reply *Protobuf) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// We will mark the task as complete
	if arg.Task.Type == Map {
		c.mapTasks[arg.TaskNumber] = arg.Task
	} else if arg.Task.Type == Reduce {
		c.reduceTasks[arg.TaskNumber] = arg.Task
	}

	return nil
}

func (c *Coordinator) RequestTask(arg *Protobuf, reply *Protobuf) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	//assign map task
	if c.mapCount < len(c.inputFiles) {

		c.mapTasks[c.mapCount].StartTime = time.Now()
		reply.TaskNumber = c.mapCount
		reply.Task = c.mapTasks[c.mapCount]
		reply.Task.Status = Assigned
		reply.NReduce = c.nReduce

		c.mapCount++
		return nil
	}
	//make sure all map tasks complete before assigning reduce task
	if !c.mapComplete {
		for i, task := range c.mapTasks {
			if task.Status != Completed {
				if time.Since(task.StartTime) > 10*time.Second {
					//reassign map task that worker took too long and still not finished
					c.mapTasks[i].StartTime = time.Now()
					reply.TaskNumber = i
					reply.Task = c.mapTasks[i]
					reply.Task.Status = Assigned
					reply.NReduce = c.nReduce
					return nil
				} else {
					//map task still in process, new freed worker waits and later check if task stuck
					reply.Task.Type = Hang
					return nil
				}
			}
		}
		c.mapComplete = true
	}
	//assign reduce task
	if c.reduceCount < c.nReduce {

		c.reduceTasks[c.reduceCount].StartTime = time.Now()
		reply.TaskNumber = c.reduceCount
		reply.Task = c.reduceTasks[c.reduceCount]
		reply.Task.Status = Assigned
		reply.NReduce = c.nReduce

		c.reduceCount++
		return nil
	}
	//make sure all reduce tasks complete before exit coordinator
	if !c.reduceComplete {
		for i, task := range c.reduceTasks {
			if task.Status != Completed {
				if time.Since(task.StartTime) > 10*time.Second {
					//reassign reduce task that worker took too long and still not finished
					c.reduceTasks[i].StartTime = time.Now()
					reply.TaskNumber = i
					reply.Task = c.reduceTasks[i]
					reply.Task.Status = Assigned
					reply.NReduce = c.nReduce

					return nil
				} else {
					//reduce task still in process, new freed worker waits and later check if task stuck
					reply.Task.Type = Hang
					return nil
				}
			}
		}
		c.reduceComplete = true
	}

	reply.Task.Status = Status(Done)
	return nil

}

// an example RPC handler.
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

	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.mapComplete && c.reduceComplete
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inputFiles:  files,
		nReduce:     nReduce,
		mapTasks:    make([]MapReduceTask, len(files)),
		reduceTasks: make([]MapReduceTask, nReduce),

		mapCount:       0,
		reduceCount:    0,
		mapComplete:    false,
		reduceComplete: false,

		lock: sync.Mutex{},
	}

	// Your code here.

	for i := range c.mapTasks {
		c.mapTasks[i] = MapReduceTask{
			Inputfiles:   []string{files[i]},
			StartTime:    time.Now(),
			Type:         Map,
			Status:       Idle,
			Outputfiles:  nil,
			TaskPosition: i,
		}
	}

	for i := range c.reduceTasks {
		c.reduceTasks[i] = MapReduceTask{
			Inputfiles:   generateInputFileNames(i, len(files)),
			StartTime:    time.Now(),
			Type:         Reduce,
			Status:       Idle,
			Outputfiles:  []string{fmt.Sprintf("mr-out-%d", i)},
			TaskPosition: i,
		}
	}

	c.server()
	return &c
}

// prepare input file names to be used by each reduce task
func generateInputFileNames(i int, file int) []string {
	var inputFileNames []string

	for j := 0; j < file; j++ {
		inputFileNames = append(inputFileNames, fmt.Sprintf("mr-%d-%d", j, i))
	}

	return inputFileNames
}
