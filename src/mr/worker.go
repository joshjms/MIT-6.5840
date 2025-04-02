package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	exited := false

	for !exited {
		mapJob, reduceJob, done := RequestJob()

		if done {
			exited = true

			continue
		}

		if mapJob != nil {
			HandleMapJob(mapJob, mapf)
		}

		if reduceJob != nil {
			HandleReduceJob(reduceJob, reducef)
		}

		time.Sleep(1 * time.Second)
	}
}

func HandleMapJob(mapJob *MapJob, mapf func(string, string) []KeyValue) {
	b, err := os.ReadFile(mapJob.FileName)
	if err != nil {
		log.Fatalf("cannot read %v", mapJob.FileName)
	}

	contents := string(b)
	kva := mapf(mapJob.FileName, contents)

	partitionedKva := make(map[int][]KeyValue)

	for _, kv := range kva {
		reducer := ihash(kv.Key) % mapJob.Reducers
		partitionedKva[reducer] = append(partitionedKva[reducer], kv)
	}

	intermediateFiles := make(map[int]string)
	for reducer, kva := range partitionedKva {
		intermediateFilename := fmt.Sprintf("mr-%v-%v", mapJob.MapJobNumber, reducer)
		intermediateFile, err := os.Create(intermediateFilename)
		if err != nil {
			log.Fatalf("cannot create %v", intermediateFilename)
		}

		b, err := json.Marshal(kva)
		if err != nil {
			log.Fatalf("cannot marshal %v", kva)
		}

		intermediateFile.Write(b)
		intermediateFile.Close()

		intermediateFiles[reducer] = intermediateFilename
	}

	args := ReportMapJobArgs{
		FileName:          mapJob.FileName,
		IntermediateFiles: intermediateFiles,

		ID: mapJob.ID,
	}

	ReportMapJob(&args)
}

func HandleReduceJob(reduceJob *ReduceJob, reducef func(string, []string) string) {
	kva := []KeyValue{}

	for _, intermediateFile := range reduceJob.IntermediateFiles {
		b, err := os.ReadFile(intermediateFile)
		if err != nil {
			log.Fatalf("cannot read %v", intermediateFile)
		}

		var kvaPart []KeyValue
		err = json.Unmarshal(b, &kvaPart)
		if err != nil {
			log.Fatalf("cannot unmarshal %v", b)
		}

		sort.Slice(kvaPart, func(i, j int) bool {
			return kvaPart[i].Key < kvaPart[j].Key
		})

		kva = append(kva, kvaPart...)
	}

	mapResult := make(map[string][]string)

	for _, kv := range kva {
		mapResult[kv.Key] = append(mapResult[kv.Key], kv.Value)
	}

	output := ""

	for key, values := range mapResult {
		output += fmt.Sprintf("%v %v\n", key, reducef(key, values))
	}

	outputFilename := fmt.Sprintf("mr-out-%v", reduceJob.ReduceId)
	outputFile, err := os.Create(outputFilename)
	if err != nil {
		log.Fatalf("cannot create %v", outputFilename)
	}

	outputFile.Write([]byte(output))
	outputFile.Close()

	args := ReportReduceJobArgs{
		ReduceId: reduceJob.ReduceId,

		ID: reduceJob.ID,
	}

	ReportReduceJob(&args)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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

func RequestJob() (*MapJob, *ReduceJob, bool) {
	args := RequestJobArgs{}
	reply := RequestJobReply{}
	if ok := call("Coordinator.RequestJob", &args, &reply); !ok {
		log.Fatalf("RequestJob failed")
	}
	return reply.MapJob, reply.ReduceJob, reply.Done
}

func ReportMapJob(args *ReportMapJobArgs) {
	reply := ReportMapJobReply{}
	if ok := call("Coordinator.ReportMapJob", args, &reply); !ok {
		log.Fatalf("ReportMapJob failed")
	}
}

func ReportReduceJob(args *ReportReduceJobArgs) {
	reply := ReportReduceJobReply{}
	if ok := call("Coordinator.ReportReduceJob", args, &reply); !ok {
		log.Fatalf("ReportReduceJob failed")
	}
}
