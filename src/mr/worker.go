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
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	mapRqstArgs := WorkerTaskRqst{}
	reply := MastersReply{}
	coordinatorStatus := call("Coordinator.SendTaskToWorker", &mapRqstArgs, &reply)

	// coordinator is active and not all tasks are done
	if coordinatorStatus && !reply.AllDone {
		if reply.TaskType == 1 {
			// MAP
			// Run mapf on reply file
			intermediateLocns, err := MapStageOnFile(mapf, &reply)

			// Report this map stage completion to coordinator
			mapReportRqst := ReportonMap{}
			mapReportReply := ReportonMapReply{}
			if err != nil {
				mapReportRqst.Stage = 0
			} else {
				mapReportRqst.Stage = 2
			}
			mapReportRqst.TaskID = reply.TaskID
			mapReportRqst.TempMapLocations = intermediateLocns

			// Call RPC to report to master
			call("Coordinator.ReportMapCompletion", &mapReportRqst, &mapReportReply)

		} else if reply.TaskType == 2 {
			// REDUCE

			err := ReduceStageOnFile(reducef, &reply)
			//Reporting structs
			reduceReportArgs := ReportonMap{}
			reduceReportReply := ReportonMapReply{}

			if err != nil {
				reduceReportArgs.Stage = 2
			} else {
				reduceReportArgs.Stage = 0
			}
			reduceReportArgs.TaskID = reply.TaskID
			call("Coordinator.ReportReduceCompletion", &reduceReportArgs, &reduceReportReply)

		}
		mapRqstArgs = WorkerTaskRqst{}
		reply = MastersReply{}
		coordinatorStatus = call("Coordinator.SendTaskToWorker", &mapRqstArgs, &reply)
	}

	// Send RPC to coordinator asking for a task
	GetTask(mapf, reducef)
}

// Runs map function on the file passed by the coordinator node and returns the list of intermediate file locations
// in which the key value pairs are stored.
func MapStageOnFile(mapf func(string, string) []KeyValue, reply *MastersReply) ([]string, error) {
	fileName := reply.FileName

	//Open file
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}

	//Read file contents
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	// Apply map function on the contents
	keyValuePairs := mapf(fileName, string(content))
	log.Println(keyValuePairs[0].Key)

	// Hash fxn is applied to each key of keyvalue pair and mod is taken
	// with nReduce so that we have only nReduce num of hash values
	// Many key value pairs may map to single hash value. So we need to store
	// dynamic sized array (slice in Golang) to store each kv pair for that hash value.
	hashMap := make([][]KeyValue, reply.NReduce)

	// Calculate hash values for the keys of all the key val pairs
	for i := 0; i < len(keyValuePairs); i++ {
		hashVal := ihash(keyValuePairs[i].Key) % int(reply.NReduce)
		hashMap[hashVal] = append(hashMap[hashVal], keyValuePairs[i])
	}

	var intermediateLocns []string

	for j := 0; j < len(hashMap); j++ {
		//Create new temp file for each of the nReduce num of files
		tempFile, _ := ioutil.TempFile(".", "")
		// Not sure why encoder is needed. CHECK WHY? For now follow instructions
		// on assignments- States taht to store key val pairs in JSON format, we need
		// to use encoder
		enc := json.NewEncoder(tempFile)
		for _, keyValPair := range hashMap[j] {
			// Encode the keyvalpair
			err := enc.Encode(&keyValPair)
			if err != nil {
				log.Fatalf("Cannot encode JSON %v", keyValPair.Key)
			}
		}
		// Each file's intermediate file to be in format mr-X-Y, where X=fileIdx,Y=hashVal or the reduce num of bucket it falls in
		os.Rename(tempFile.Name(), "mr-"+fmt.Sprint(reply.TaskID)+"-"+fmt.Sprint(j))
		tempFile.Close()
		// Adding current file's intermediate file to intermediate locations of all files handled by master.
		intermediateLocns = append(intermediateLocns, "mr-"+fmt.Sprint(reply.TaskID)+"-"+fmt.Sprint(j))
	}
	return intermediateLocns, nil
}

func ReduceStageOnFile(reducef func(string, []string) string, reply *MastersReply) error {
	intermediate := []KeyValue{}
	for _, fileName := range reply.TempMapLocations {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("Cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	opfile, err := ioutil.TempFile("", "mr-out")
	opName := opfile.Name()

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
		fmt.Fprintf(opfile, "%v %v\n", intermediate[i].Key, output)
		i = j

	}
	opfile.Close()

	finalName := "mr-out-" + fmt.Sprint(reply.TaskID)
	err = os.Rename(opName, finalName)
	if err != nil {
		return err
	}
	return nil

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func GetTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

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
