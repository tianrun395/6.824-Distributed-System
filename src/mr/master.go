package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	nReduce              int
	mutex                sync.Mutex
	files                []string
	mapTaskPool          []IntKeyValue
	reduceTaskPool       []int
	completedMapTasks    int
	completedReduceTasks int
	mapTasksFinished     map[int]bool
	reduceTasksFinished  map[int]bool
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetMapTask(args *ExampleArgs, reply *TaskReply) error {
	m.mutex.Lock()
	i := len(m.mapTaskPool) - 1
	log.Printf("map task pool size %v", len(m.mapTaskPool))
	if i < 0 {
		reply.Filename = ""
		reply.CompletedMapTasks = m.completedMapTasks
		reply.TotalMapTasks = len(m.files)
		m.mutex.Unlock()
		log.Printf("RETURN FILE NAME, %v, completed map tasks %v", reply.Filename, reply.CompletedMapTasks)
		return nil
	}
	// stack
	topElement := m.mapTaskPool[i]
	m.mapTaskPool = m.mapTaskPool[:i]

	reply.Filename = topElement.Value
	log.Printf("RETURN FILE NAME, %v", reply.Filename)
	reply.MapTaskNumber = topElement.Key
	reply.NReduce = m.nReduce
	m.mutex.Unlock()
	// wait for 10s for worker to finish the map task,
	// if not, we will consider the map task failed and reassign it to another worker
	go func(topElement IntKeyValue) {
		log.Printf("alive, file: %v", topElement.Value)
		time.Sleep(10 * time.Second)
		log.Printf("still alive, file: %v, %v", topElement.Value, m.mapTasksFinished[topElement.Key])
		m.mutex.Lock()
		if m.mapTasksFinished[topElement.Key] != true {
			log.Printf("append map task back to pool, %v", topElement.Value)
			m.mapTaskPool = append(m.mapTaskPool, topElement)
		}
		m.mutex.Unlock()
	}(topElement)
	return nil
}

func (m *Master) ReportMapTaskFinished(mapTaskNumber *int, reply *TaskReply) error {
	m.mutex.Lock()
	m.completedMapTasks++
	m.mapTasksFinished[*mapTaskNumber] = true
	log.Printf("completed map tasks %v", m.completedMapTasks)
	log.Printf("map task %v finished", *mapTaskNumber)
	m.mutex.Unlock()
	return nil
}

func (m *Master) GetReduceTask(args *ExampleArgs, reply *TaskReply) error {
	m.mutex.Lock()
	taskAssigned := false
	topElement := -1
	if m.completedMapTasks >= len(m.files) {
		reply.ShouldStartReduce = true
		reply.TotalMapTasks = len(m.files)
		log.Printf("reduce task pool size %v", len(m.reduceTaskPool))
		j := len(m.reduceTaskPool) - 1
		if j < 0 {
			reply.ReduceTaskNumber = -1
			reply.CompletedReduceTasks = m.completedReduceTasks
			reply.NReduce = m.nReduce
			m.mutex.Unlock()
			log.Printf("NO REDUCE TASK, RETURN, completed reduce tasks %v", reply.CompletedReduceTasks)
			return nil
		}
		topElement = m.reduceTaskPool[j]
		m.reduceTaskPool = m.reduceTaskPool[:j]
		reply.ReduceTaskNumber = topElement
		log.Printf("ASSIGN REDUCE TASK NUMBER %v", reply.ReduceTaskNumber)
		taskAssigned = true
	}
	m.mutex.Unlock()
	if taskAssigned {
		go func(topElement int) {
			// wait for 10s for worker to finish the reduce task,
			// if not, we will consider the reduce task failed and reassign it to another worker
			time.Sleep(10 * time.Second)
			m.mutex.Lock()
			if m.reduceTasksFinished[reply.ReduceTaskNumber] != true {
				log.Printf("append reduce task back to pool, %v", topElement)
				m.reduceTaskPool = append(m.reduceTaskPool, reply.ReduceTaskNumber)
			}
			m.mutex.Unlock()
		}(topElement)
	}
	return nil
}

func (m *Master) ReportReduceTaskFinished(reduceTaskNumber *int, reply *TaskReply) error {
	m.mutex.Lock()
	m.completedReduceTasks++
	m.reduceTasksFinished[*reduceTaskNumber] = true
	log.Printf("completed reduce tasks %v", m.completedReduceTasks)
	log.Printf("reduce task %v finished", *reduceTaskNumber)
	m.mutex.Unlock()
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	ret = m.completedReduceTasks >= m.nReduce

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.files = files
	m.nReduce = nReduce
	m.completedMapTasks = 0
	m.completedReduceTasks = 0
	m.mapTasksFinished = make(map[int]bool)
	m.reduceTasksFinished = make(map[int]bool)
	for i := 0; i < len(files); i++ {
		m.mapTaskPool = append(m.mapTaskPool, IntKeyValue{Key: i, Value: files[i]})
	}
	for i := 0; i < nReduce; i++ {
		m.reduceTaskPool = append(m.reduceTaskPool, i)
	}

	m.server()
	return &m
}
