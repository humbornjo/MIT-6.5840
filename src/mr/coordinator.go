package mr

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MAP    int = 0
	REDUCE int = 1
	DONE   int = 2
	IDLE   int = 3
)

type Coordinator struct {
	// Your definitions here.
	nMap     int
	nReduce  int
	stage    int
	currTask chan Task
	nextTask []string
	runnTask map[Task]chan int
	lock     sync.RWMutex
}

type Task struct {
	Tid   int
	Fname string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) DeliverTask(args *AskForTaskArgs, reply *AskForTaskReply) error {

	if len(c.currTask) == 0 {
		reply.Task = Task{}
		reply.Stage = IDLE
		reply.Nout = c.nReduce
		return nil
	}

	c.lock.Lock()
	task := <-c.currTask
	c.lock.Unlock()

	reply.Task = task

	c.lock.RLock()
	reply.Stage = c.stage
	reply.Nout = c.nReduce
	c.lock.RUnlock()

	go func() {
		ch := make(chan int)

		c.lock.Lock()
		c.runnTask[task] = ch // 开始计时10秒，完成任务，否则删除字典项，任务重新入队
		c.lock.Unlock()

		pCtx := context.Background()
		ctx, cancel := context.WithTimeout(pCtx, 5*time.Second)
		defer cancel()

		select {
		case <-ctx.Done():
			c.lock.Lock()
			c.currTask <- task
			c.lock.Unlock()
		case <-ch:
		}

		c.lock.Lock()
		delete(c.runnTask, task)
		c.lock.Unlock()
	}()

	return nil
}

func (c *Coordinator) GatherRes(args *ReportForTaskArgs, reply *ReportForTaskReply) error {
	//对runnTask中的键值对发送信号
	reply.Foo = true

	c.lock.Lock()
	ch, exists := c.runnTask[args.Task]
	if !exists {
		reply.Foo = false
	} else {
		ch <- 1
		delete(c.runnTask, args.Task)
	}
	c.lock.Unlock()

	go c.nextStage()

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
	ret := true

	// Your code here.
	c.lock.RLock()
	ret = c.stage == DONE
	c.lock.RUnlock()

	return ret
}

func (c *Coordinator) nextStage() {
	// fmt.Printf("currTask: %d\n", len(c.currTask))
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(c.currTask) == 0 && len(c.runnTask) == 0 {
		switch c.stage {
		case MAP:
			// fmt.Println("map done")
			c.stage = REDUCE
			for i := 0; i < c.nReduce; i++ {
				c.currTask <- Task{Tid: i, Fname: fmt.Sprintf("mr-*-%d", i)}
			}
		case REDUCE:
			// fmt.Println("reduce done")
			c.stage = DONE
		case DONE:
			os.Exit(0)
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:     len(files),
		nReduce:  nReduce,
		stage:    MAP,
		currTask: make(chan Task, max(len(files), nReduce)),
		nextTask: []string{},
		runnTask: make(map[Task]chan int),
	}

	c.lock.Lock()
	for i, file := range files {
		c.currTask <- Task{Tid: i, Fname: file}
	}
	c.lock.Unlock()

	c.server()

	go func() {
		for {
			time.Sleep(1 * time.Second)
			c.nextStage()
		}
	}()

	return &c
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
