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

func updateWorkingStatue(workerId int32) {
	args := Args{}
	reply := Reply{}
	args.WorkerId = workerId
	for true {
		call("Master.UpdateWorkingStatue", args, reply)
		time.Sleep(time.Millisecond * 500)
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reply := Reply{}
	args := Args{}
	for !call("Master.RegisterWorker", &args, &reply) {
		time.Sleep(time.Second)
	}
	workerId := reply.WorkerId
	go updateWorkingStatue(workerId) // 周期性连接master
	var task *Task

	for {
		reply := Reply{}
		args := Args{}
		args.WorkerId = workerId
		if call("Master.ApplyMapTask", &args, &reply) {
			if reply.IsFinished {
				reply.Test = true
				break
			}
			task = reply.TaskInfo
			iFilesSet := map[string]int{} //去重
			for _, filename := range reply.TaskInfo.FileNames {
				for _, f := range processMapTask(task.TaskId, filename, mapf) {
					if iFilesSet[f] != 0 {
						iFilesSet[f]++
					}
					iFilesSet[f] = 1
				}
			}
			for k, _ := range iFilesSet {
				args.IFiles = append(args.IFiles, k)
			}
			args.TaskInfo = task
			call("Master.FinishMapTask", &args, &reply)
		}
	}
	log.Println("Map任务全部完成")
	for {
		reply := Reply{}
		args := Args{}
		args.WorkerId = workerId
		if call("Master.ApplyReduceTask", &args, &reply) {
			if reply.IsFinished {
				break
			}
			task := reply.TaskInfo
			processReduceTask(task.TaskId, task.FileNames, reducef)
			args.TaskInfo = task
			call("Master.FinishReduceTask", &args, &reply)
		}
	}
}

func processMapTask(taskId int32, filename string, mapf func(string, string) []KeyValue) []string {
	file, _ := os.Open(filename)
	contents, _ := ioutil.ReadAll(file)
	kva := mapf(filename, string(contents))
	encoders := map[int]*json.Encoder{}
	res := []string{}
	var key int
	var iname string
	for i := 0; i < len(kva); i++ {
		key = ihash(kva[i].Key) % 10
		if encoders[key] == nil {
			iname = "mr-" + strconv.Itoa(int(taskId)) + "-" + strconv.Itoa(key)
			res = append(res, iname)
			var fwriter *os.File
			if os.ErrNotExist == nil {
				fwriter, _ = os.OpenFile(iname, os.O_APPEND, 0644)
			} else {
				fwriter, _ = os.Create(iname)
			}
			defer fwriter.Close()
			encoders[key] = json.NewEncoder(fwriter)
		}
		_ = encoders[key].Encode(&kva[i])
	}
	return res
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func processReduceTask(taskId int32, filenames []string, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for _, filename := range filenames {
		file, _ := os.Open(filename)
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	keyMap := map[string][]string{}
	for _, kv := range kva {
		if keyMap[kv.Key] == nil {
			keyMap[kv.Key] = []string{}
		}
		keyMap[kv.Key] = append(keyMap[kv.Key], kv.Value)
	}
	res := []KeyValue{}
	for k, values := range keyMap {
		res = append(res, KeyValue{
			Key:   k,
			Value: reducef(k, values),
		})
	}
	ofile := "mr-out-" + strconv.Itoa(int(taskId))
	w, _ := os.Create(ofile)
	for _, kv := range res {
		fmt.Fprintf(w, "%v %v\n", kv.Key, kv.Value)
	}
	w.Close()
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
	//sockname := masterSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	c, err := rpc.DialHTTP("tcp", ":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	//fmt.Println(err)
	return false
}
