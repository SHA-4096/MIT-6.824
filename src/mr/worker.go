package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	for {
		time.Sleep(5 * time.Second)
		reply, _ := requestTask(3, "")
		if reply.WorkType == 0 {
			continue
		}
		fmt.Println(reply)
		//获得reply之后判断任务要求
		if reply.WorkType == 1 {
			//进行map操作
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}
			file.Close()
			kva := mapf(reply.FileName, string(content))
			//fmt.Println(kva)
			sort.Sort(ByKey(kva))
			oname := fmt.Sprintf("mr-out-%d", ihash(reply.FileName))
			ofile, _ := os.Create(oname)
			enc := json.NewEncoder(ofile)
			for _, kv := range kva {
				err := enc.Encode(&kv)
				if err != nil {
					panic(err)
				}
			}
			//发送完成的信息
			requestTask(1, oname)

		} else {
			//进行reduce操作
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}
			kva := []KeyValue{}
			defer file.Close()
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			//写文件
			oname := fmt.Sprintf("%s-%d", reply.FileName, ihash(reply.FileName))
			ofile, _ := os.Create(oname)
			defer ofile.Close()
			i := 0
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

				// this is the correct format for each line of Reduce output.
				//fmt.Printf("%v %v\n", kva[i].Key, output)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			//发送完成的信息
			requestTask(2, oname)

		}
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func requestTask(requestType int, fileName string) (ReplyArgs, error) {

	// declare an argument structure.
	args := RequestArgs{}

	// fill in the argument(s).
	args.ReqType = requestType
	args.FileName = fileName

	// declare a reply structure.
	reply := ReplyArgs{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		//fmt.Println(reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply, nil
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
