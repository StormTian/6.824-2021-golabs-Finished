package mr

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files         []string
	nReduce       int
	mMap          int // number of map tasks
	tasks         []int
	finishedTasks map[int]struct{} // taskID -> struct{}
	mu            sync.Mutex
	phase         string
	done          int64 // changed by monitor()
	mapFinish     chan struct{}
	reduceFinish  chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// AssignATask assigns a task to a worker.
// an RPC handler, called by workers.
func (c *Coordinator) AssignATask(args *AssignATaskArgs, reply *AssignATaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.tasks) == 0 {
		reply.Err = NoTasks // no tasks currently.
		return nil
	}
	reply.Phase = c.phase
	reply.TaskID = c.tasks[0]
	c.tasks = c.tasks[1:] // delete the task from the queue.
	if reply.Phase == Map {
		reply.NReduce = c.nReduce
		reply.Filename = c.files[reply.TaskID]
	} else {
		reply.MMap = c.mMap
	}
	go c.wait(reply.TaskID)
	return nil
}

// FinishATask is informed that a task has finished.
// an RPC handler, called by workers.
func (c *Coordinator) FinishATask(args *FinishATaskArgs, reply *FinishATaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Phase != c.phase {
		// outdated request.
		return nil
	}
	c.finishedTasks[args.TaskID] = struct{}{}
	switch args.Phase {
	case Map:
		{
			if len(c.finishedTasks) == c.mMap {
				c.mapFinish <- struct{}{}
			}
		}
	case Reduce:
		{
			if len(c.finishedTasks) == c.nReduce {
				c.reduceFinish <- struct{}{}
			}
		}
	}
	return nil
}

// wait waits for ten seconds, then check if the task has finished.
func (c *Coordinator) wait(taskID int) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	_, ok := c.finishedTasks[taskID]
	if !ok {
		c.tasks = append(c.tasks, taskID) // re-assign this task
	}
	c.mu.Unlock()
}

// monitor monitors the progress of this computation,
// and ends the main process.
func (c *Coordinator) monitor() {
	// wait for all map tasks to finish.
	<-c.mapFinish
	Dprintf("map phase finish.")

	// start the reduce phase.
	c.mu.Lock()
	c.tasks = []int{}
	for i := 0; i < c.nReduce; i++ {
		c.tasks = append(c.tasks, i)
	}
	c.finishedTasks = make(map[int]struct{})
	c.phase = Reduce
	c.mu.Unlock()

	// wait for all reduce tasks to finish.
	<-c.reduceFinish
	Dprintf("reduce phase finish.")

	// finish this computation.
	atomic.StoreInt64(&c.done, 1)
}

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
		log.Fatal("listen error:", e)
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
	if atomic.LoadInt64(&c.done) > 0 {
		ret = true
	}

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
	c.files = files
	c.nReduce = nReduce
	c.mMap = len(files) // n files -> n map tasks

	// start from map phase.
	c.tasks = []int{}
	for i := 0; i < c.mMap; i++ {
		c.tasks = append(c.tasks, i) // [0,1,...,mMap-1]
	}
	c.finishedTasks = make(map[int]struct{})
	c.phase = Map
	c.done = 0
	c.mapFinish = make(chan struct{}, 1)
	c.reduceFinish = make(chan struct{}, 1)
	// start a backup goroutine to monitor the progress of this computation.
	go c.monitor()

	c.server()
	return &c
}
