package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

var (
	TaskCount  int
	taskAssign chan ReplyArgs
)

type Coordinator struct {
	// Your definitions here.

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

func (c *(Coordinator)) GetTask(args *RequestArgs, reply *ReplyArgs) error {
	fmt.Println("Being called,args = ", *args)
	fmt.Println("TaskCount=", TaskCount)
	if args.ReqType == 2 {
		TaskCount--
	} else if args.ReqType == 1 {
		//finished map task
		tsk := ReplyArgs{2, args.FileName} // A reduce task
		taskAssign <- tsk
	} else {
		if TaskCount == 0 {
			return nil
		}
		//assigning a task
		tmpReply := <-taskAssign
		(*reply).FileName = tmpReply.FileName
		(*reply).WorkType = tmpReply.WorkType
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	//不是很了解socket，先用http顶一顶
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
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
	if TaskCount == 0 {
		c.Done()
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
	taskAssign = make(chan ReplyArgs, 1000)
	// Your code here.
	TaskCount = 0
	c.server()
	for _, filename := range files {
		TaskCount++
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		//		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		tsk := ReplyArgs{1, filename}
		taskAssign <- tsk
	}
	return &c
}
