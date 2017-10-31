package mapreduce

import (
	"hash/fnv"
	"os"
	"log"
	"io/ioutil"
	"bufio"
	"fmt"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string,    // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!

	if DebugFlag {
		fmt.Printf("input file %s\n", inFile)
	}
	f, err := os.Open(inFile)
	if err != nil {
		log.Fatalf("open %s %s", inFile, err)
	}
	content, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("read %s %s", inFile, err)
	}
	if DebugFlag {
		fmt.Printf("input content %s\n", string(content))
	}
	f.Close()

	rfs := make([]*os.File, 0, nReduce)
	wrs := make([]*bufio.Writer, 0, nReduce)
	// create R reduce files
	for i := 0; i < nReduce; i++ {
		rn := reduceName(jobName, mapTaskNumber, i)
		rf, err := os.Create(rn)
		if err != nil {
			log.Fatalf("create %s %s", rn, err)
		}
		if DebugFlag {
			fmt.Printf("create file %s\n", rn)
		}
		rfs = append(rfs, rf)
		wrs = append(wrs, bufio.NewWriter(rf))
	}

	kvs := mapF(inFile, string(content))
	for _, kv := range kvs {
		i := ihash(kv.Key) % uint32(nReduce)
		_, err := wrs[i].WriteString(EncodeKV(kv.Key, kv.Value))
		if err != nil {
			log.Fatalf("write to %s %s", wrs[i], err)
		}
		wrs[i].WriteByte('\n')
	}

	// flush contents, close files
	for i := range rfs {
		wrs[i].Flush()
		rfs[i].Close()
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
