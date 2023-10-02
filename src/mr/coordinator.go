package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Stage int32

const (
	MAP    Stage = 0
	REDUCE Stage = 1
	DONE   Stage = 2
)

type Coordinator struct {
	// Your definitions here.
	nMap     int
	nReduce  int
	stage    Stage
	currTask []string
	nextTask []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) DeliverTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	reply.Task = "haha"
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
	ret := false

	// Your code here.
	ret = c.stage == DONE

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:     len(files),
		nReduce:  nReduce,
		stage:    MAP,
		currTask: files,
		nextTask: []string{},
	}

	// Your code here.

	c.server()
	return &c
}
