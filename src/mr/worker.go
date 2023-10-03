package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	wid := os.Getpid()
	for {
		args := AskForTaskArgs{Wid: wid}
		reply := AskForTaskReply{}

		if !AskForTask(&args, &reply) {
			continue
		}

		if reply.Stage == DONE {
			break
		}

		switch reply.Stage {
		case MAP:
			filename := reply.Task.Fname
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			kva := mapf(filename, string(content))
			saveJson(reply.Task.Tid, kva, reply.Nout)

			ReportForTask(&ReportForTaskArgs{Task: reply.Task}, &ReportForTaskReply{})

		case REDUCE:
			cmd := exec.Command("find", os.TempDir(), "-name", reply.Task.Fname)
			out, err := cmd.Output()
			if err != nil {
				log.Fatalf("fail to exec find command")
			}

			filenames := strings.Split(string(out), "\n")

			var kva []KeyValue
			for _, filename := range filenames {
				file, err := os.Open(filename)
				if err != nil {
					continue
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}

			oname := fmt.Sprintf("mr-out-%d", reply.Task.Tid)
			ofile, err := os.Create(oname)
			if err != nil {
				log.Fatalf("cannot create %v", oname)
			}

			i := 0
			sort.Sort(ByKey(kva))

			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			ofile.Close()

			for _, filename := range filenames {
				os.Remove(filename)
			}

			if !ReportForTask(&ReportForTaskArgs{Task: reply.Task}, &ReportForTaskReply{}) {
				os.Remove(oname)
			}

		case IDLE | DONE:
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) bool {
	ok := call("Coordinator.DeliverTask", args, reply)
	return ok
}

func ReportForTask(args *ReportForTaskArgs, reply *ReportForTaskReply) bool {
	ok := call("Coordinator.GatherRes", args, reply)
	return ok
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		os.Exit(0)
		// log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

func saveJson(tid int, kva []KeyValue, nout int) {
	hash_kva := make(map[int][]KeyValue)
	for _, kv := range kva {
		hash := ihash(kv.Key) % nout
		hash_kva[hash] = append(hash_kva[hash], kv)
	}

	for i := 0; i < nout; i++ {
		filename := fmt.Sprintf("mr-%d-%d", tid, i)
		ofile, err := os.CreateTemp("", filename)
		dir := filepath.Dir(ofile.Name())
		filename = filepath.Join(dir, filename)
		os.Rename(ofile.Name(), filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range hash_kva[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot save %v", ofile)
			}
		}
	}
}
