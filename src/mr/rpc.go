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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

//ReqType 1 finishedMap 2 finishedReduce 3 get a new task
type RequestArgs struct {
	ReqType  int
	FileName string //文件名
}

//1 for Map and 2 for Reduce
type ReplyArgs struct {
	WorkType int    //1 for Map and 2 for Reduce
	FileName string //文件名
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
