package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	phase                                   string
	files                                   []string
	lock                                    sync.Mutex
	nMapTask, nReduceTask                   int
	nCompletedMapTask, nCompletedReduceTask int
	assignedMapTask, assignedReduceTask     []bool
	timeoutInterval                         int64
	mapTaskStartTime, reduceTaskStartTime   []int64
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) AssignTask(args *Args, reply *Reply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.checkTimeoutTask()

	if m.phase == "map" {
		idx := m.findEmptyMapTask()
		if idx == -1 {
			reply.Phase = "waitting"
			return nil
		}
		reply.MIdx = idx
		reply.NReduce = m.nReduceTask
		reply.INames = []string{m.files[idx]}
		m.assignedMapTask[idx] = true
		m.mapTaskStartTime[idx] = time.Now().Unix()
	}

	if m.phase == "reduce" {
		idx := m.findEmptyReduceTask()
		if idx == -1 {
			reply.Phase = "waitting"
			return nil
		}
		reply.OIdx = idx
		reply.INames = m.getInterFileNames(idx)
		m.assignedReduceTask[idx] = true
		m.reduceTaskStartTime[idx] = time.Now().Unix()
	}

	reply.Phase = m.phase
	return nil
}

func (m *Master) RecordCompleteInfo(args *Args, reply *Reply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if args.Phase == "map" {
		if !args.Succeed {
			m.assignedMapTask[args.TIdx] = false
			return nil
		}
		m.nCompletedMapTask++
		m.mapTaskStartTime[args.TIdx] = 0
		if m.nCompletedMapTask == m.nMapTask {
			m.phase = "reduce"
		}
	}

	if args.Phase == "reduce" {
		if !args.Succeed {
			m.assignedReduceTask[args.TIdx] = false
			return nil
		}
		m.nCompletedReduceTask++
		m.reduceTaskStartTime[args.TIdx] = 0
		if m.nCompletedReduceTask == m.nReduceTask {
			m.phase = "finish"
		}
	}

	return nil
}

func (m *Master) getInterFileNames(idx int) []string {
	files := make([]string, 0)
	for i := 0; i < m.nMapTask; i++ {
		filename := "./mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(idx) + ".json"
		files = append(files, filename)
	}
	return files
}

func (m *Master) findEmptyMapTask() int {
	r := -1
	for i := 0; i < m.nMapTask; i++ {
		if !m.assignedMapTask[i] {
			r = i
			break
		}
	}
	return r
}

func (m *Master) findEmptyReduceTask() int {
	r := -1
	for i := 0; i < m.nReduceTask; i++ {
		if !m.assignedReduceTask[i] {
			r = i
			break
		}
	}
	return r
}

func (m *Master) checkTimeoutTask() {
	timeStamp := time.Now().Unix()

	for i := 0; i < m.nMapTask; i++ {
		if m.mapTaskStartTime[i] != 0 && m.assignedMapTask[i] &&
			(timeStamp-m.mapTaskStartTime[i]) > m.timeoutInterval {
			m.assignedMapTask[i] = false
		}
	}

	for i := 0; i < m.nReduceTask; i++ {
		if m.reduceTaskStartTime[i] != 0 && m.assignedReduceTask[i] &&
			(timeStamp-m.reduceTaskStartTime[i]) > m.timeoutInterval {
			m.assignedReduceTask[i] = false
		}
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.phase == "finish" {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	nMap := len(files)
	m := Master{
		nMapTask:             nMap,
		nReduceTask:          nReduce,
		phase:                "map",
		files:                files,
		nCompletedMapTask:    0,
		nCompletedReduceTask: 0,
		assignedMapTask:      make([]bool, nMap, nMap),
		assignedReduceTask:   make([]bool, nReduce, nReduce),
		timeoutInterval:      10,
		mapTaskStartTime:     make([]int64, nMap, nMap),
		reduceTaskStartTime:  make([]int64, nReduce, nReduce),
	}

	// Your code here.
	for i := 0; i < nMap; i++ {
		m.assignedMapTask[i] = false
		m.mapTaskStartTime[i] = 0
	}
	for i := 0; i < nReduce; i++ {
		m.assignedReduceTask[i] = false
		m.reduceTaskStartTime[i] = 0
	}

	m.server()
	return &m
}
