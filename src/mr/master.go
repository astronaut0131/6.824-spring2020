package mr

import (
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
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
	jobIdQueue []int
	done bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
const Timeout = 10

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
		println("done ...")
		reply.JobType = 3
		return nil
	}
	if m.phase == 0 {
		if len(m.jobIdQueue) == 0 {
			reply.JobType = 2
			return nil

		}
		var fileNames []string
		front := m.jobIdQueue[0]
		m.jobIdQueue = m.jobIdQueue[1:]
		fileNames = append(fileNames,m.inputFileNames[front])
		reply.FileNames = fileNames
		reply.JobType = 0
		reply.TaskNum = front
		go Monitor(m,front)
		return nil
	} else {
		if len(m.jobIdQueue) == 0 {
			reply.JobType = 2
			return nil
		}
		front := m.jobIdQueue[0]
		m.jobIdQueue = m.jobIdQueue[1:]
		reply.FileNames = m.outputFileNames[front]
		reply.JobType = 1
		reply.TaskNum = front
		go Monitor(m,front)
		return nil
	}
}

func Monitor(m *Master, jobId int) {
	time.Sleep(Timeout * time.Second)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.finished[jobId] == false {
		// worker fail to finish the job
		m.jobIdQueue = append(m.jobIdQueue,jobId)
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
			println("enter reduce phase")
			m.phase = 1
			m.jobIdQueue = []int{}
			for i:= 0; i < m.nReduce; i++ {
				m.jobIdQueue = append(m.jobIdQueue,i)
			}
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
	for i:= 0; i < len(files); i++ {
		m.jobIdQueue = append(m.jobIdQueue,i)
	}
	m.done = false
	m.server()
	m.phase = 0
	return &m
}
