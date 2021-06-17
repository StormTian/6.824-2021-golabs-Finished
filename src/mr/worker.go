package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

const (
	waitForTasks = 30*time.Millisecond
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		// ask for a task
		ok, reply := callAssignATask()
		if !ok {
			break // worker exit
		}
		if reply.Err == NoTasks {
			time.Sleep(waitForTasks)
			continue
		}

		var t string
		// deal with the task
		switch reply.Phase {
		case Map:
			{
				t = Map
				nReduce := reply.NReduce

				// read contents and do map
				filename := reply.Filename
				intermediate := []KeyValue{}
				file, err := os.Open(filename)
				if err != nil {
					Dprintf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					Dprintf("cannot read %v", filename)
				}
				file.Close()
				intermediate = mapf(filename, string(content))
				sort.Sort(ByKey(intermediate))
				// Dprintf("intermediate: %v", intermediate)

				// partition intermediate into nReduce buckets
				partitionedInter := [][]KeyValue{} // y -> list of kv
				for i:=0;i<nReduce;i++{
					partitionedInter=append(partitionedInter, []KeyValue{})
				}
				i:=0
				for i<len(intermediate) {
					y := ihash(intermediate[i].Key)%nReduce
					j := i+1
					for j<len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					for k:=i;k<j;k++{
						partitionedInter[y] = append(partitionedInter[y], intermediate[k])
					}
					i = j
				}

				// write intermediate files
				x := reply.TaskID
				for y:=0;y<nReduce;y++{
					ifile, _ := ioutil.TempFile("/newFS/workspace/src/6.824-2021/src", "intermediate-")
					enc := json.NewEncoder(ifile)
					toWrite := partitionedInter[y]
					// Dprintf("length of toWrite: %v", len(toWrite))
					for _, kv := range toWrite {
						err := enc.Encode(&kv)
						if err!=nil{
							Dprintf("cannot write.")
						}
					}
					ifile.Close()
					//rename
					iname := "mr-"+strconv.Itoa(x)+"-"+strconv.Itoa(y)
					err := os.Rename(ifile.Name(), iname)
					if err!= nil{
						Dprintf("cannot rename: ", err)
					}
				}
			}
		case Reduce:
			{
				t = Reduce
				mMap := reply.MMap
				y := reply.TaskID

				// read all kv charged by this task
				kva := []KeyValue{}
				for x:=0;x<mMap;x++{
					filename := "mr-"+strconv.Itoa(x)+"-"+strconv.Itoa(y)
					file, err := os.Open(filename)
					if err != nil {
						Dprintf("cannot open %v", filename)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							// Dprintf("cannot read %v", filename)
							break
						}
						kva = append(kva, kv)
					}
					file.Close()
				}
				sort.Sort(ByKey(kva)) // for deterministic output

				// write the output file
				ofile, _ := ioutil.TempFile("/newFS/workspace/src/6.824-2021/src", "out-")
				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					// conbine all values that belong to the key
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
					i = j
				}
				ofile.Close()
				// rename
				oname := "mr-out-"+strconv.Itoa(y)
				err := os.Rename(ofile.Name(), oname)
				if err!= nil{
					Dprintf("cannot rename: ", err)
				}
			}
		default:
			{
				Dprintf("Unvalid task type.")
				continue
			}

		}

		// inform finish
		go callFinishATask(t, reply.TaskID)
	}
}

// Ask coordinator for a task.
func callAssignATask() (bool, AssignATaskReply) {
	args := AssignATaskArgs{}
	reply := AssignATaskReply{}
	ok := call("Coordinator.AssignATask", &args, &reply)
	return ok, reply
}

// Inform coordinator the task has finished.
func callFinishATask(t string, taskID int) {
	args := FinishATaskArgs{
		Phase:  t,
		TaskID: taskID,
	}
	reply := FinishATaskReply{}
	call("Coordinator.FinishATask", &args, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
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
