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
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for st, reply := callAssignTask(); st && reply.Phase != "finish"; st, reply = callAssignTask() {
		if reply.Phase == "waitting" {
			time.Sleep(time.Second)
		} else if reply.Phase == "map" {
			callSendComplete(reply.Phase, reply.MIdx, doMap(mapf, reply))
		} else if reply.Phase == "reduce" {
			callSendComplete(reply.Phase, reply.OIdx, doReduce(reducef, reply))
		} else {
			log.Fatalf("phase %v is undefined!\n", reply.Phase)
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func doReduce(reducef func(string, []string) string, r Reply) bool {
	kva, err := readKeyValuePairFromFiles(r)
	if err != nil {
		return false
	}

	sort.Sort(ByKey(kva))

	oname := "mr-out-" + strconv.Itoa(r.OIdx)
	ofile, err := os.Create(oname)
	if err != nil {
		return false
	}

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
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	return true
}

func doMap(mapf func(string, string) []KeyValue, r Reply) bool {
	kva := []KeyValue{}
	for _, filename := range r.INames {
		content := readFileContent(filename)
		kva = append(kva, mapf(filename, content)...)
	}

	encs, files, err := createIntermediateFiles(r.MIdx, r.NReduce)
	if err != nil {
		return false
	}

	for _, kv := range kva {
		idx := ihash(kv.Key) % r.NReduce
		if err := encs[idx].Encode(&kv); err != nil {
			log.Fatalln("encode key/value error!")
		}
	}

	// close all files
	for i := 0; i < r.NReduce; i++ {
		files[i].Close()
	}

	return true
}

func readKeyValuePairFromFiles(r Reply) ([]KeyValue, error) {
	kva := make([]KeyValue, 0)
	for _, filename := range r.INames {
		file, err := os.Open(filename)
		if err != nil {
			return nil, err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	return kva, nil
}

func createIntermediateFiles(mIdx, nReduce int) ([]*json.Encoder, []*os.File, error) {

	files := make([]*os.File, nReduce, nReduce)
	encs := make([]*json.Encoder, nReduce, nReduce)
	for i := 0; i < nReduce; i++ {
		filename := "./mr-" + strconv.Itoa(mIdx) + "-" + strconv.Itoa(i) + ".json"
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("file %v create failed!\n", filename)
			return nil, nil, err
		}
		encs[i] = json.NewEncoder(file)
		files[i] = file
	}

	return encs, files, nil
}

func readFileContent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
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

func callAssignTask() (bool, Reply) {
	args := Args{}
	reply := Reply{}

	state := call("Master.AssignTask", &args, &reply)

	return state, reply
}

func callSendComplete(phase string, tIdx int, succeed bool) {
	args := Args{Phase: phase, TIdx: tIdx, Succeed: succeed}
	call("Master.RecordCompleteInfo", args, nil)
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
