package mapreduce

import (
	"fmt"
	"strings"
)

const KVSEP = '~'

func EncodeKV(k, v string) string {
	return k + string(KVSEP) + v
}

func DecodeKV(kv string) (string, string) {
	str := strings.Split(kv, string(KVSEP))
	if len(str) != 2 {
		panic(fmt.Errorf("wrong fommat for key value %s", kv))
	}
	return str[0], str[1]
}

type EntryArr []Entry

// sort keys
type Entry struct {
	Key    string
	Values []string
}

// Len is the number of elements in the collection.
func (e EntryArr) Len() int {
	return len(e)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (es EntryArr) Less(i, j int) bool {
	return es[i].Key < es[j].Key
}

// Swap swaps the elements with indexes i and j.
func (es EntryArr) Swap(i, j int) {
	es[i], es[j] = es[j], es[i]
}

// self defined debug flag
var DebugFlag = false
