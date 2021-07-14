package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"log"
	"os"
)
import "strconv"

const (
	Debug   = 1
	Map     = "Map"
	Reduce  = "Reduce"
	NoTasks = "NoTasks"
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
type AssignATaskArgs struct{}

type AssignATaskReply struct {
	Err      string
	Phase    string // Map or Reduce
	NReduce  int
	MMap     int
	TaskID   int
	Filename string // for Map
}

type FinishATaskArgs struct {
	Phase  string
	TaskID int
}

type FinishATaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func Dprintf(format string, v ...interface{}) {
	if Debug > 0 {
		log.Printf(format, v)
	}
}
