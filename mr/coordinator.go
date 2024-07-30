package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

type Coordinator struct {
	// Your definitions here.
	NReduce int
	NMap int
	MapTasks chan *Task
	ReduceTasks chan *Task
	Files []string
	State int // 0: map, 1: reduce, 2: end
	MapDoneNum int
	ReduceDoneNum int
	MapTaskInfo []TaskInfo
	ReduceTaskInfo []TaskInfo
	Mutex *sync.Mutex
}

type TaskInfo struct {
	ID int
	StartTime time.Time
	Done bool
	Assigned bool
}

type Task struct {
	ID int
	TaskType int // 0: map, 1: reduce, 2: wait, 3: exit
	NReduce int
	NMap int
	File string
}
// Your code here -- RPC handlers for the worker to call.

func MakeTask(taskType int, file string, id int, nReduce int, nMap int) *Task {
	task := Task{
		ID: id,
		TaskType: taskType,
		NReduce: nReduce,
		NMap: nMap,
		File: file,
	}
	return &task
}

func (c *Coordinator) StartTask(args *Task, reply *Task) error {
	switch args.TaskType {
		case 0:
			c.Mutex.Lock()
			c.MapTaskInfo[args.ID].StartTime = time.Now()
			c.Mutex.Unlock()
		case 1:
			c.Mutex.Lock()
			c.ReduceTaskInfo[args.ID].StartTime = time.Now()
			c.Mutex.Unlock()
	}

	return nil
}

func (c *Coordinator) EndTask(args *Task, reply *Task) error {
	switch args.TaskType {
		case 0:
			c.Mutex.Lock()
			c.MapTaskInfo[args.ID].Done = true
			c.MapDoneNum++
			if c.MapDoneNum == c.NMap {
				c.State = 1
			}
			c.Mutex.Unlock()
		case 1:
			c.Mutex.Lock()
			c.ReduceTaskInfo[args.ID].Done = true
			c.ReduceDoneNum++
			if c.ReduceDoneNum == c.NReduce {
				c.State = 2
			}
			c.Mutex.Unlock()
	}
	
	return nil
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *Task) error {
	switch c.getState() {
		case 0:
			if len(c.MapTasks) > 0 {
				c.Mutex.Lock()
				*reply = *<- c.MapTasks
				c.MapTaskInfo[reply.ID].Assigned = true
				c.Mutex.Unlock()
			} else {
				*reply = *MakeTask(2, "", -1, -1, -1)
			}
		case 1:
			if len(c.ReduceTasks) > 0 {
				c.Mutex.Lock()
				*reply = *<- c.ReduceTasks
				c.ReduceTaskInfo[reply.ID].Assigned = true
				c.Mutex.Unlock()
			} else {
				*reply = *MakeTask(2, "", -1, -1, -1)
			}
		case 2:
			*reply = *MakeTask(3, "", -1, -1, -1)
	}
	return nil
}
//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) getState() int {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	return c. State
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.getState() == 2 {
		ret = true
	}

	return ret
}

func (c *Coordinator) checkTask() {
	for c.getState() == 0 {
		c.Mutex.Lock()
		for _, task := range c.MapTaskInfo {
			if (task.Assigned == true) && (task.Done == false) && (time.Since(task.StartTime) >= 10 * time.Second) {
				newTask := MakeTask(0, c.Files[task.ID], task.ID, c.NReduce, c.NMap)
				c.MapTasks <- newTask
				c.MapTaskInfo[task.ID] = TaskInfo{ID: task.ID, Done: false, Assigned: false}
			}
		}
		c.Mutex.Unlock()
		time.Sleep(time.Second)
	}

	for c.getState() == 1 {
		c.Mutex.Lock()
		for _, task := range c.ReduceTaskInfo {
			if (task.Assigned == true) && (task.Done == false) && (time.Since(task.StartTime) >= 10 * time.Second) {
				newTask := MakeTask(1, "", task.ID, c.NReduce, c.NMap)
				c.ReduceTasks <- newTask
				c.ReduceTaskInfo[task.ID] = TaskInfo{ID: task.ID, Done: false, Assigned: false}
			}
		}
		c.Mutex.Unlock()
		time.Sleep(time.Second)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce: nReduce,
		NMap: len(files),
		State: 0,
		Files: files,
		MapTasks: make(chan *Task, len(files)),
		ReduceTasks: make(chan *Task, nReduce),
		MapDoneNum: 0,
		ReduceDoneNum: 0,
		MapTaskInfo: make([]TaskInfo, len(files)),
		ReduceTaskInfo: make([]TaskInfo, nReduce),
		Mutex: new(sync.Mutex),
	}

	// Your code here.
	for i := 0; i < len(files); i++ {
		task := MakeTask(0, files[i], i, nReduce, len(files))
		c.MapTaskInfo[i] = TaskInfo{ID: i, Done: false, Assigned: false}
		c.MapTasks <- task
	}
	for i := 0; i < nReduce; i++ {
		task := MakeTask(1, "", i, nReduce, len(files))
		c.ReduceTaskInfo[i] = TaskInfo{ID: i, Done: false, Assigned: false,}
		c.ReduceTasks <- task
	}

	go func() {
		c.checkTask()
	}()

	c.server()
	return &c
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}