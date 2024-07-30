package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "sort"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	end := false
	for end != true {
		task := FetchTask()
		switch task.TaskType {
			case 0:
				executeMap(&task, mapf)
				finishTask(&task)
			case 1:
				executeReduce(&task, reducef)
				finishTask(&task)
			case 2:
				executeWait()
			case 3:
				end = true
		}
	}
}

func finishTask(task *Task) {
	arg := task
	reply := Task{}
	call("Coordinator.EndTask", &arg, &reply)
}

func executeWait() {
	time.Sleep(time.Second)
}

func executeMap(task *Task, mapf func(string, string) []KeyValue) {
	arg := task
	reply := Task{}
	ok := call("Coordinator.StartTask", &arg, &reply)
	if ok == false {
		return
	}
	filename := task.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v\n %v", filename, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	intermediate := mapf(filename, string(content))

	nReduce := task.NReduce
	hashedKV := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		id := ihash(kv.Key) % nReduce
		hashedKV[id] = append(hashedKV[id], kv)
	}
	for i := 0; i < nReduce; i++ {
		oname := "mr-" + strconv.Itoa(task.ID) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range hashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}
}

func executeReduce(task *Task, reducef func(string, []string) string) {
	arg := task
	reply := Task{}
	ok := call("Coordinator.StartTask", &arg, &reply)
	if ok == false {
		return
	}
	id := task.ID
	nMap := task.NMap
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		oname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(id)
		ofile, _ := os.Open(oname)
		dec := json.NewDecoder(ofile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(id)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

func FetchTask() Task {
	arg := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.AssignTask", &arg, &reply)
	if ok == false {
		reply = *MakeTask(3, "", -1, -1, -1)
	}
	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}









//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}