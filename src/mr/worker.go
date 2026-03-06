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
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type IntKeyValue struct {
	Key   int
	Value string
}

// Implement sort.Interface for KeyValue slice
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

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
	args := ExampleArgs{}
	replys := TaskReply{}
	// ask master for map tasks
	for {
		args = ExampleArgs{}
		replys = TaskReply{}
		// RPC call master asking for a task to work on
		call("Master.GetMapTask", &args, &replys)
		filename := replys.Filename
		log.Printf("get map task, filename %v", filename)
		if filename == "" {
			if replys.CompletedMapTasks >= replys.TotalMapTasks {
				log.Printf("NO MAP TASK, BREAK")
				break
			} else {
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}
		nReduce := replys.NReduce
		mapTaskNumber := replys.MapTaskNumber
		// read the file
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		// do the map work, emit the intermediate KeyValue pairs.
		kva := mapf(filename, string(content))
		openedFileMap := make(map[int]*os.File)
		cwd, _ := os.Getwd()
		for _, kv := range kva {
			reduceTaskNumber := ihash(kv.Key) % nReduce
			if openedFileMap[reduceTaskNumber] == nil {
				intermediateFileName := fmt.Sprintf("mr-%v-%v**", mapTaskNumber, reduceTaskNumber)
				ofile, err := os.CreateTemp(cwd, intermediateFileName)
				// ofile, err := os.OpenFile(intermediateFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.Fatalf("cannot open intermediate file %v", intermediateFileName)
				}
				openedFileMap[reduceTaskNumber] = ofile
			}
			enc := json.NewEncoder(openedFileMap[reduceTaskNumber])
			encodeErr := enc.Encode(&kv)
			if encodeErr != nil {
				log.Fatalf("encode failed %v", kv)
			}
		}
		for _, file := range openedFileMap {
			tempFileName := file.Name()
			i := strings.IndexByte(tempFileName, '*')
			os.Rename(file.Name(), tempFileName[:i])
			file.Close()
		}
		log.Printf("mapTaskNumber %v, map tasks finished", mapTaskNumber)
		// tell master that map task is finished
		call("Master.ReportMapTaskFinished", &mapTaskNumber, &replys)
	}

	for {
		// RPC polling call master asking for reduce tasks
		args = ExampleArgs{}
		replys = TaskReply{}
		for {
			call("Master.GetReduceTask", &args, &replys)
			if replys.ShouldStartReduce {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if replys.ReduceTaskNumber == -1 {
			if replys.CompletedReduceTasks >= replys.NReduce {
				log.Printf("NO REDUCE TASK, BREAK")
				break
			} else {
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		log.Printf("start to do reduce tasks, task number %v", replys.ReduceTaskNumber)
		// read intermediate files and get all KeyValue pairs for this reduce task
		kva := []KeyValue{}
		reduceTaskNumber := replys.ReduceTaskNumber
		for i := 0; i < replys.TotalMapTasks; i++ {
			intermediateFileName := fmt.Sprintf("mr-%v-%v", i, reduceTaskNumber)
			file, err := os.Open(intermediateFileName)
			if err != nil {
				log.Printf("cannot open intermediate file %v", intermediateFileName)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			file.Close()
		}
		log.Printf("file reading finished, task number %v", reduceTaskNumber)
		// do the reduce work, emit the output KeyValue pairs.
		sort.Sort(ByKey(kva))

		oname := fmt.Sprintf("mr-out-%v**", reduceTaskNumber)
		cwd, _ := os.Getwd()
		ofile, _ := os.CreateTemp(cwd, oname)

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
		for i := 0; i < len(kva); {
			log.Printf("reduce task number %v, i = %v", reduceTaskNumber, i)
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
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

			i = j
		}
		idx := strings.IndexByte(ofile.Name(), '*')
		os.Rename(ofile.Name(), ofile.Name()[:idx])
		ofile.Close()
		call("Master.ReportReduceTaskFinished", &reduceTaskNumber, &replys)
	}
	os.Exit(0)
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
