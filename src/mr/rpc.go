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

type RequestJobArgs struct{}

type RequestJobReply struct {
	MapJob    *MapJob
	ReduceJob *ReduceJob
	Done      bool

	ID int
}

type ReportMapJobArgs struct {
	FileName          string
	IntermediateFiles map[int]string

	ID int
}

type ReportMapJobReply struct{}

type ReportReduceJobArgs struct {
	ReduceId int

	ID int
}

type ReportReduceJobReply struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
