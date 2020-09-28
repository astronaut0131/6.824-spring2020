package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const WorkerNum int = 10

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

func checkFileIsExist(filename string) bool {
	var exist = true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

func SpawnWorker(mapf func(string, string) []KeyValue, reducef func(string, []string) string,nReduce int,/*wg *sync.WaitGroup*/) {
	// defer wg.Done()
	for {
		jobArgs := GetJobArgs{}
		jobReply := GetJobReply{}
		success := call("Master.GetJob",&jobArgs,&jobReply)
		if success == false {
			break
		}
		if jobReply.JobType == 3 {
			break
		}
		if jobReply.JobType == 2 {
			time.Sleep(time.Second)
			continue
		}
		if jobReply.JobType == 0 {
			var finishFiles []string
			// map job
			// pass fileName, fileContent to mapf
			filename := jobReply.FileNames[0]
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			sort.Sort(ByKey(kva))
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				reduceTaskNum := ihash(kva[i].Key) % nReduce
				outputFileName := "mr-" + strconv.Itoa(jobReply.TaskNum) + "-" + strconv.Itoa(reduceTaskNum)
				var f *os.File
				var err error
				if checkFileIsExist(outputFileName) {
					f, err = os.OpenFile(outputFileName, os.O_APPEND|os.O_WRONLY,os.ModeAppend)
					if err != nil {
						log.Fatalf("cannot open %v", outputFileName)
					}
				} else {
					finishFiles = append(finishFiles, outputFileName)
					f, err = os.Create(outputFileName)
					if err != nil {
						log.Fatalf("cannot create %v", outputFileName)
					}
				}
				enc := json.NewEncoder(f)
				for k := i; k < j; k++ {
					err := enc.Encode(&kva[k])
					if err != nil {
						log.Fatal("encode err:", err)
					}
				}
				f.Close()
				i = j
			}
			finishJobArgs := FinishJobArgs{}
			finishJobArgs.TaskNum = jobReply.TaskNum
			finishJobArgs.FileNames = finishFiles
			finishJobReply := FinishJobReply{}
			call("Master.FinishJob",&finishJobArgs,&finishJobReply)
		} else {
			// reduce job
			// pass key, value list to reducef
			println("start reducing...")
			var kva []KeyValue
			var output []KeyValue
			for _,filename := range jobReply.FileNames {
				f,err := os.OpenFile(filename, os.O_RDONLY, 0666)
				if err != nil {
					log.Fatal("open file error",err)
				}
				dec := json.NewDecoder(f)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				f.Close()
			}
			sort.Sort(ByKey(kva))
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
				var kv KeyValue
				kv.Key = kva[i].Key
				kv.Value = reducef(kva[i].Key, values)
				output = append(output,kv)
				i = j
			}
			outputFileName := "mr-out-" + strconv.Itoa(jobReply.TaskNum)
			f,err := os.Create(outputFileName)
			if err != nil {
				log.Fatal(err)
			}
			for _,kv := range output{
				fmt.Fprintf(f,"%s %s\n",kv.Key,kv.Value)
			}
			f.Close()
			finishJobArgs := FinishJobArgs{}
			finishJobArgs.TaskNum = jobReply.TaskNum
			finishJobReply := FinishJobReply{}
			call("Master.FinishJob",&finishJobArgs,&finishJobReply)
			println("reduce finished ...")
		}
	}
}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()


	args := GetInfoArgs{}
	reply := GetInfoReply{}
	//if call("Master.GetInfo",&args,&reply) == false {
	//	return
	//}
	call("Master.GetInfo",&args,&reply)
	nReduce := reply.NReduce
	//var i int
	//var waitgroup sync.WaitGroup
	//for i = 0; i < WorkerNum; i++ {
	//	waitgroup.Add(1)
	//	go SpawnWorker(mapf,reducef,nReduce,&waitgroup)
	//}
	//waitgroup.Wait()
	SpawnWorker(mapf,reducef,nReduce)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
