package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// worker keep asking for new task from coordinator if completes one
	for {
		arg := Protobuf{}
		reply := Protobuf{}

		ret := call("Coordinator.RequestTask", &arg, &reply)

		if !ret {
			break
		}

		switch reply.Task.Type {
		case Map:
			doMapTask(&reply, mapf)
		case Reduce:
			doReduceTask(&reply, reducef)
		case Hang:
			//no more map task for free worker before other done map tasks, wait for stuck task or upcoming reduce tasks
			time.Sleep(1 * time.Second)
		case Done:
			os.Exit(0)
		default:
			time.Sleep(1 * time.Second)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func doReduceTask(reply *Protobuf, reducef func(string, []string) string) {
	// Load intermediate files
	intermediate := []KeyValue{}
	for m := 0; m < len(reply.Task.Inputfiles); m++ {
		file, err := os.Open(reply.Task.Inputfiles[m])
		if err != nil {
			log.Fatalf("cannot open %v", reply.Task.Inputfiles[m])
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// Sort intermediate key-value pairs by key
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// Create output file
	oname := fmt.Sprintf("mr-out-%d", reply.Task.TaskPosition)
	ofile, _ := ioutil.TempFile("", oname)

	// Apply reduce function
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	// Close output file
	ofile.Close()

	// Rename output file
	os.Rename(ofile.Name(), oname)

	// Update task status
	reply.Task.Status = Completed
	dummyReply := Protobuf{}
	call("Coordinator.NotifyComplete", &reply, &dummyReply)
}

func doMapTask(reply *Protobuf, mapf func(string, string) []KeyValue) {

	file, err := os.Open(reply.Task.Inputfiles[0])
	if err != nil {
		log.Fatalf("cannot open %v", reply.Task.Inputfiles[0])
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Task.Inputfiles[0])
	}
	file.Close()

	kva := mapf(reply.Task.Inputfiles[0], string(content))
	intermediate := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % reply.NReduce
		intermediate[r] = append(intermediate[r], kv)
	}

	for r, kva := range intermediate {
		oname := fmt.Sprintf("mr-%d-%d", reply.Task.TaskPosition, r)
		ofile, _ := ioutil.TempFile("", oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			enc.Encode(&kv)
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}

	// Update server state of the task, by calling the RPC NotifyComplete
	reply.Task.Status = Completed
	dummyReply := Protobuf{}
	call("Coordinator.NotifyComplete", &reply, &dummyReply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
