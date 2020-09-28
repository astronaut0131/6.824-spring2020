package mr

import (
	"log"
	"strconv"
	"strings"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.

	nReduce int
	mu sync.Mutex
	// phase = 0: map
	// phase = 1: reduce
	phase int
	// input files for map
	inputFileNames []string
	// output files of map
	outputFileNames map[int][]string
	// record whether map job x has finished
	finished map[int]bool
	nextTaskId int
	done bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *GetInfoArgs, reply *GetInfoReply) error {
	reply.NReduce = m.nReduce
	return nil
}

func (m *Master) GetInfo(args *GetInfoArgs, reply *GetInfoReply) error {
	reply.NReduce = m.nReduce
	return nil
}

func (m *Master) GetJob(args *GetJobArgs, reply *GetJobReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done {
		reply.JobType = 3
		return nil
	}
	if m.phase == 0 {
		if m.nextTaskId == len(m.inputFileNames) {
			reply.JobType = 2
			return nil
		}
		var fileNames []string
		fileNames = append(fileNames,m.inputFileNames[m.nextTaskId])
		reply.FileNames = fileNames
		reply.JobType = 0
		reply.TaskNum = m.nextTaskId
		m.nextTaskId += 1
		return nil
	} else {
		if m.nextTaskId == m.nReduce {
			reply.JobType = 2
			return nil
		}
		reply.FileNames = m.outputFileNames[m.nextTaskId]
		reply.JobType = 1
		reply.TaskNum = m.nextTaskId
		m.nextTaskId += 1
		return nil
	}
}

func (m *Master) FinishJob(args *FinishJobArgs, reply *FinishJobReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.phase == 0 {
		m.finished[args.TaskNum] = true
		for _,filename := range args.FileNames {
			s := strings.Split(filename,"-")
			reduceTaskId,err := strconv.Atoi(s[2])
			if err != nil {
				log.Fatal("convert error:", err)
			}
			m.outputFileNames[reduceTaskId] = append(m.outputFileNames[reduceTaskId],filename)
		}
		if len(m.finished) == len(m.inputFileNames) {
			m.phase = 1
			m.nextTaskId = 0
			m.finished = make(map[int]bool)
		}
	} else {
		m.finished[args.TaskNum] = true
		if len(m.finished) == m.nReduce {
			m.done = true
		}
	}
	return nil
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
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	//cmd := exec.Command("rm","-rf mr-*-*")
	//cmd.Output()
	m := Master{}
	// Your code here.
	m.nReduce = nReduce
	m.inputFileNames = files
	m.outputFileNames = make(map[int][]string)
	m.finished = make(map[int]bool)
	m.nextTaskId = 0
	m.done = false
	m.server()
	m.phase = 0
	return &m
}
