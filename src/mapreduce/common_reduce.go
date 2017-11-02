package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	tn := mergeName(jobName, reduceTaskNumber)
	if DebugFlag {
		fmt.Printf("output file %s\n", tn)
	}
	// TODO, bug, should not create ? but to open ?
	tf, err := os.Open(tn)
	fmt.Printf("try to opent output file %s\n", tn)
	if err != nil {
		fmt.Printf("open failed for %s\n", tn)
		tf, err = os.Create(tn)
		fmt.Printf("try to create output file %s\n", tn)
	}
	if err != nil {
		log.Fatalf("open %s %s", tn, err)
	}
	enc := json.NewEncoder(tf)
	defer tf.Close()

	m := map[string]int{}
	a := EntryArr{}

	for i := 0; i < nMap; i++ {
		rn := reduceName(jobName, i, reduceTaskNumber)
		rf, err := os.Open(rn)
		if err != nil {
			log.Fatalf("open %s %s", rn, err)
		}
		if DebugFlag {
			fmt.Printf("opent temp file %s\n", rn)
		}

		scan := bufio.NewScanner(rf)
		for scan.Scan() {
			kv := scan.Text()
			k, v := DecodeKV(kv)
			if id, ok := m[k]; ok {
				e := &a[id] // copy by addr
				e.Values = append(e.Values, v)
			} else {
				m[k] = len(a)
				e := Entry{k, []string{v}}
				a = append(a, e)
			}
		}
		rf.Close()
	}

	sort.Sort(a)
	//output
	for _, e := range a {
		enc.Encode(KeyValue{e.Key, reduceF(e.Key, e.Values)})
	}
}
