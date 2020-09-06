package mr

import (
	"errors"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

const (
	IDLE      = 0
	INPROCESS = 1
	COMPLETED = 2
)

type Task struct {
	TaskId    int32
	FileNames []string
	WorkerId  int32
	StartTime int64
	Statue    int8
}

type Master struct {
	mu               sync.Mutex
	mapTasks         []*Task
	reduceTasks      []*Task
	iFiles           map[int32][]string //key:reduceTaskId,value:中间键值对文件名
	workingWorker    map[int32]int64
	workerCnt        int32
	nReduce          int32
	mapTaskFinish    bool
	reduceTaskFinish bool
	cntCompleteTask  int32
}

func (m *Master) ApplyMapTask(args *Args, reply *Reply) error {
	reply.IsFinished = m.mapTaskFinish
	if m.mapTaskFinish {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.getIdleMapTask()
	if task == nil {
		return errors.New("没有map任务可分配")
	}
	task.StartTime = time.Now().UnixNano()
	task.WorkerId = args.WorkerId
	task.Statue = INPROCESS
	reply.TaskInfo = task
	return nil
}

func (m *Master) ApplyReduceTask(args *Args, reply *Reply) error {
	reply.IsFinished = m.reduceTaskFinish
	if m.reduceTaskFinish {
		return nil
	}
	if !m.mapTaskFinish {
		return errors.New("error:map任务未完成")
	}
	if m.reduceTasks == nil {
		return errors.New("error:reduce还未生成")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.getIdleReduceTask()
	if task == nil {
		return errors.New("没有reduce任务可分配")

	}
	task.StartTime = time.Now().UnixNano()
	task.WorkerId = args.WorkerId
	task.Statue = INPROCESS
	reply.TaskInfo = task
	return nil
}

//
//完成map task，用户需要提交workerId
//
func (m *Master) FinishMapTask(args *Args, reply *Reply) error {
	task := m.getTaskByWorkerId(args.WorkerId)
	if task.WorkerId != args.WorkerId {
		log.Printf("提交task{taskId:%v,workerId:%v}超时", task.TaskId, args.WorkerId)
		return nil
	}
	if m.workingWorker[args.WorkerId] == 0 {
		log.Printf("该worker{workerId:%v}不存在,", args.WorkerId)
		return nil
	}
	log.Printf("worker:%v 完成了Map任务:%v\n", args.TaskInfo.WorkerId, args.TaskInfo.TaskId)
	m.mapTasks[args.TaskInfo.TaskId].Statue = COMPLETED
	m.cntCompleteTask++

	for _, s := range args.IFiles {
		strs := strings.Split(s, "-")
		reduceTaskId, _ := strconv.ParseInt(strs[2], 10, 64)
		m.iFiles[int32(reduceTaskId)] = append(m.iFiles[int32(reduceTaskId)], s)
	}
	if int(m.cntCompleteTask) == len(m.mapTasks) {
		log.Println("Map任务全部完成")
		m.generateReduceTasks()
		m.mapTaskFinish = true
		m.cntCompleteTask = 0
	}
	return nil
}

func (m *Master) FinishReduceTask(args *Args, reply *Reply) error {
	task := m.getTaskByWorkerId(args.WorkerId)
	if task.WorkerId != args.WorkerId {
		log.Printf("提交task{taskId:%v,workerId:%v}超时", task.TaskId, args.WorkerId)
		return nil
	}
	if m.workingWorker[args.WorkerId] == 0 {
		log.Printf("该worker{workerId:%v}不存在,", args.WorkerId)
		return nil
	}
	log.Printf("worker:%v 完成了Reduce任务:%v\n", args.TaskInfo.WorkerId, args.TaskInfo.TaskId)
	m.cntCompleteTask++
	if int(m.cntCompleteTask) == len(m.reduceTasks) {
		log.Println("Reduce任务全部完成")
		m.reduceTaskFinish = true
	}
	task.Statue = COMPLETED
	return nil
}

//
// 得到 map Task
//
func (m *Master) getIdleMapTask() *Task {
	for _, task := range m.mapTasks {
		if task.Statue == IDLE {
			return task
		}
	}
	return nil
}

//
// 得到 reduce Task
//
func (m *Master) getIdleReduceTask() *Task {
	for _, task := range m.reduceTasks {
		if task.Statue == IDLE {
			return task
		}
	}
	return nil
}

func (m *Master) getReduceTaskById(taskId int32) *Task {
	for _, task := range m.reduceTasks {
		if task.TaskId == taskId {
			return task
		}
	}
	return nil
}

func (m *Master) getMapTaskById(taskId int32) *Task {
	for _, task := range m.mapTasks {
		if task.TaskId == taskId {
			return task
		}
	}
	return nil
}

func (m *Master) generateReduceTasks() {
	for taskId, filenames := range m.iFiles {
		task := Task{
			TaskId:    taskId,
			FileNames: filenames,
			WorkerId:  0,
			StartTime: 0,
			Statue:    IDLE,
		}
		m.reduceTasks = append(m.reduceTasks, &task)
	}
	log.Printf("生成reduce任务,数量:%v\n", len(m.reduceTasks))
}

//
// 注册worker，返回workerId和 reduce Task数量
//
func (m *Master) RegisterWorker(request *Args, reply *Reply) error {
	log.Printf("register workerId:%v", m.workerCnt)
	reply.WorkerId = m.workerCnt
	reply.NRecude = m.nReduce
	m.workingWorker[m.workerCnt] = time.Now().UnixNano()
	m.workerCnt++
	return nil
}

//
// 更新worker连接时间
//
func (m *Master) UpdateWorkingStatue(args Args, reply *Reply) error {
	if m.workingWorker[args.WorkerId] != 0 {
		log.Printf("connected: %v", args.WorkerId)
		m.workingWorker[args.WorkerId] = time.Now().UnixNano()
	}
	return nil
}

func (m *Master) getTaskByWorkerId(workerId int32) *Task {
	for _, task := range m.mapTasks {
		if task.WorkerId == workerId && task.Statue == INPROCESS {
			return task
		}
	}
	for _, task := range m.reduceTasks {
		if task.WorkerId == workerId && task.Statue == INPROCESS {
			return task
		}
	}
	return nil
}

func (m *Master) checkWorkerAndTask() {
	for true {
		for workerId, t := range m.workingWorker {
			if time.Now().UnixNano()-t > int64(time.Second*2) {
				log.Printf("worker{workerId:%v} 长时间未连接,已经清除", workerId)
				delete(m.workingWorker, workerId)
				task := m.getTaskByWorkerId(workerId)
				if task != nil {
					task.Statue = IDLE
				}
			}
		}
		for _, task := range m.mapTasks {
			if task.Statue == INPROCESS &&
				task.StartTime != 0 &&
				time.Now().UnixNano()-task.StartTime > int64(time.Second*10) {
				task.Statue = IDLE
				task.WorkerId = -1
			}
		}
		for _, task := range m.reduceTasks {
			if task.Statue == INPROCESS &&
				task.StartTime != 0 &&
				time.Now().UnixNano()-task.StartTime > int64(time.Second*10) {
				task.Statue = IDLE
				task.WorkerId = -1
			}
		}
		time.Sleep(time.Second)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//sockname := masterSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
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
	return m.mapTaskFinish && m.reduceTaskFinish
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	if len(files) == 0 {
		return nil
	}
	m := Master{
		mu:            sync.Mutex{},
		mapTasks:      nil,
		reduceTasks:   nil,
		iFiles:        map[int32][]string{},
		mapTaskFinish: false,
		nReduce:       int32(nReduce),
		workingWorker: map[int32]int64{},
	}
	for i, filename := range files {
		m.mapTasks = append(m.mapTasks, &Task{
			TaskId:    int32(i),
			FileNames: []string{},
			WorkerId:  -1,
			StartTime: 0,
			Statue:    IDLE,
		})
		m.mapTasks[len(m.mapTasks)-1].FileNames =
			append(m.mapTasks[len(m.mapTasks)-1].FileNames, filename)
	}
	log.Printf("生成map任务,数量:%v\n", len(m.mapTasks))
	go m.checkWorkerAndTask()
	m.server()
	return &m
}
