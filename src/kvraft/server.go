package raftkv

import (
	"6.824/src/labrpc"
	"6.824/src/raft"
	"encoding/gob"
	"sync"
	"bytes"
	"fmt"
	"strconv"
	"time"
	"strings"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type SeenType int

const (
	TimeOut     SeenType = iota
	AlreadyDone
)

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	syncCh  chan []string // sync apply disposer and front end

	maxraftstate int               // snapshot if log grows this big
	kvmap        map[string]string // kv database
	notfinish    map[string]bool   // for request notfinish but not finished in time
	done         map[string]bool   // for request done

	// Your definitions here.
}

// helper methods for kv wrapper
func wrapCKV(c, k, v string) []string {
	return []string{c, k, v}
}

func getCKV(wrap interface{}) (c, k, v string) {
	if wrap, ok := wrap.([]string); ok {
		c, k, v = wrap[0], wrap[1], wrap[2]
		return
	}
	panic("wrong type")
}

func commendIndexKey(cmd string, cId, rId int) string {
	return strconv.Itoa(cId) + "~" + strconv.Itoa(rId) + "~" + cmd
}

func decomposeCKey(cKey string) (cId, rId int, cmd string) {
	strs := strings.SplitN(cKey, "~", -1)
	cId, _ = strconv.Atoi(strs[0])
	rId, _ = strconv.Atoi(strs[1])
	cmd = strs[2]
	return
}

func (kv *RaftKV) disposeAplMsg() {
	var lastWantCKey string
	var lastDoneCKey string
	for { //todo
		select {
		case aplMsg := <-kv.applyCh:
			wrap := aplMsg.Command.([]string)
			c, k, v := getCKV(wrap)
			cId, rId, cmd := decomposeCKey(c)
			DAplRecvPrintf("[APLRecv] got serv:%d cId:%d rId:%d cKey:%s lastWant:%s lastDone:%s k:%s v:%s\n",
				kv.me, cId, rId, c, lastWantCKey, lastDoneCKey, k, v)
			if strings.HasPrefix(cmd, "Get") {
				v = kv.kvmap[k]
			} else if strings.HasPrefix(cmd, "Put") {
				kv.kvmap[k] = v
			} else {
				kv.kvmap[k] += v
			}
			lastDoneCKey = c
			if c == lastWantCKey {
				go func(v string, ch chan []string) {
					DAplRecvPrintf("[APLRecv] send me:%d cKey: %s k: %s v: %s\n", kv.me, c, k, v)
					ch <- wrapCKV(c, k, v)
				}(v, kv.syncCh)
			}
		case wrap := <-kv.syncCh: // todo assuming the ckey goes ahead from the aplMsg, apparently
			cKey, k, v := getCKV(wrap)
			cId, rId, cmd := decomposeCKey(cKey)
			DAplRecvPrintf("[APLRecv] serv:%d cId:%d rId:%d cmd:%d want %s lastDone %s lastWant %s\n",
				kv.me, cId, rId, cmd, cKey, lastDoneCKey, lastWantCKey)
			lastWantCKey = cKey
			if lastDoneCKey == cKey { // already got
				c := cKey
				DAplRecvPrintf("[APLRecv] send me:%d cKey: %s \n", kv.me, c)
				_, _, cmd := decomposeCKey(c)
				if strings.HasPrefix(cmd, "Get") {
					v = kv.kvmap[k]
				}
				kv.syncCh <- wrapCKV(cKey, k, v)
			}
		}
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	//todo add logic to remove done req,
	//todo add logic to remove notfinish req
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key

	cKey := commendIndexKey("Get", args.ClId, args.ReqId)
	wrap := wrapCKV(cKey, key, "")
	reply.ServId = kv.me
	DServPrintf("Get [Request] serv %d wrap %v\n", kv.me, wrap)

	if kv.done[cKey] {
		reply.Value = kv.kvmap[key]
		return
	}

	// for not finished ones
	if kv.notfinish[cKey] {
		kv.syncCh <- wrap
		kv.waitGet(cKey, reply)
		return
	}

	ind, term, isL := kv.rf.Start(wrap)
	//todo
	DServPrintf("Get [Reply] serv %d ind %d term %d isL %t\n", kv.me, ind, term, isL)
	if !isL {
		reply.WrongLeader = true
		return
	}

	//send the cKey
	kv.syncCh <- wrapCKV(cKey, key, "")

	//wait the answer or timeout
	kv.waitGet(cKey, reply)
}

func (kv *RaftKV) waitGet(cKey string, reply *GetReply) {
	select {
	case <-time.After(time.Second * 1):
		reply.Err = Error_TimeOut
		kv.notfinish[cKey] = true
	case wrap := <-kv.syncCh:
		c, k, v := getCKV(wrap)
		reply.Value = v
		kv.done[cKey] = true
		DServPrintf("Get [Got] serv %d c: %s k: %s v: %s\n", kv.me, c, k, v)
	}
}

// for get $ put, the operation is idempotent
// for append, we should record the discarded ones and do not let it send again

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clId := args.ClId
	reqId := args.ReqId
	key := args.Key
	val := args.Value
	cKey := commendIndexKey(args.Op, clId, reqId)
	wrap := wrapCKV(cKey, key, val)
	reply.ServId = kv.me

	DServPrintf("PutAppend [Request] serv %d wrap %v\n", kv.me, wrap)

	if kv.done[cKey] {
		return
	}

	// for not finished ones, do not resend
	if kv.notfinish[cKey] {
		kv.syncCh <- wrap
		kv.waitPutAppend(cKey, reply)
		return
	}

	ind, term, isL := kv.rf.Start(wrap)

	DServPrintf("PutAppend [Reply] serv %d ind %d term %d isL %t\n", kv.me, ind, term, isL)
	if !isL {
		reply.WrongLeader = true
		return
	}

	//send the cKey
	kv.syncCh <- wrap

	//wait the answer or timeout
	kv.waitPutAppend(cKey, reply)
}

//wait the answer or timeout
func (kv *RaftKV) waitPutAppend(cKey string, reply *PutAppendReply) {
	select {
	case <-time.After(time.Second * 1): // todo, time out 1s ?
		reply.Err = Error_TimeOut
		kv.notfinish[cKey] = true
	case wrap := <-kv.syncCh:
		c, k, v := getCKV(wrap)
		kv.done[cKey] = true
		DServPrintf("PutAppend [Got] serv %d c: %s k: %s v: %s\n", kv.me, c, k, v)
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	DPrintf("Kill Serv %s", kv)
}

//todo
func (kv *RaftKV) String() string {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("me %d ", kv.me))
	return buf.String()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.kvmap = make(map[string]string, 1024)
	kv.notfinish = make(map[string]bool, 1024)
	kv.done = make(map[string]bool, 1024)
	kv.syncCh = make(chan []string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.disposeAplMsg()
	return kv
}
