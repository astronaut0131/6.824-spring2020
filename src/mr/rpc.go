package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type GetInfoArgs struct {
}

type GetInfoReply struct {
	NReduce int
}

type FinishJobArgs struct {
	FileNames []string
	TaskNum   int
}

type FinishJobReply struct {
}

type GetJobArgs struct {
}

type GetJobReply struct {
	FileNames []string
	// JobType = 0 => map
	// JobType = 1 => reduce
	// JobType = 2 => no available job currently, should sleep for a while
	// JobType = 3 => all jobs have been done, exit goroutine
	JobType int
	TaskNum int
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
