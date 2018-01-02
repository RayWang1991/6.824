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

	answerCh chan []string  // for disposer to return answerCh
	syncCh   chan []string  // for front ent to sync apply disposer
	doneCh   chan [] string // for disposer answer the sync request

	maxraftstate int               // snapshot if log grows this big
	kvmap        map[string]string // kv database
	notFinish    map[string]bool   // for request notFinish but not finished in time
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
			cKey, k, v := getCKV(wrap)
			cId, rId, cmd := decomposeCKey(cKey)
			DAplRecvPrintf("[APLRecv-Got] got serv:%d cId:%d rId:%d cKey:%s lastWant:%s lastDone:%s k:%s v:%s\n",
				kv.me, cId, rId, cKey, lastWantCKey, lastDoneCKey, k, v)

			if kv.done[cKey] {
				// already Done before
				DAplRecvPrintf("[APLRecv-AlreadyDone]  serv:%d cKey: %s k: %s v: %s\n", kv.me, cKey, k, v)
				break
			}

			if strings.HasPrefix(cmd, "Get") {
				v = kv.kvmap[k]
			} else if strings.HasPrefix(cmd, "Put") {
				kv.kvmap[k] = v
			} else {
				kv.kvmap[k] += v
			}
			kv.done[cKey] = true
			lastDoneCKey = cKey
			break
			//debug
			if cKey == lastWantCKey {
				go func(v string, ch chan []string) {
					DAplRecvPrintf("[APLRecv-AplMsg-Start] send me:%d cKey: %s k: %s v: %s\n", kv.me, cKey, k, v)
					ch <- wrapCKV(cKey, k, v)
					DAplRecvPrintf("[APLRecv-AplMsg-End] send me:%d cKey: %s k: %s v: %s\n", kv.me, cKey, k, v)
				}(v, kv.answerCh)
			}
		case wrap := <-kv.syncCh: // todo assuming the ckey goes ahead from the aplMsg, apparently
			cKey, k, v := getCKV(wrap)
			cId, rId, cmd := decomposeCKey(cKey)
			lastWantCKey = cKey
			done := kv.done[cKey] // done before
			if done {
				DAplRecvPrintf("[APLRecv-AlreadyDone] serv:%d cId:%d rId:%d cmd:%s want %s lastDone %s lastWant %s\n",
					kv.me, cId, rId, cmd, cKey, lastDoneCKey, lastWantCKey)
				if strings.HasPrefix(cmd, "Get") {
					v = kv.kvmap[k]
				}
				DAplRecvPrintf("[APLRecv-Sync-Start] send me:%d cKey: %s \n", kv.me, cKey)
				kv.doneCh <- wrapCKV(cKey, k, v)
				DAplRecvPrintf("[APLRecv-Sync-End] send me:%d cKey: %s \n", kv.me, cKey)
			} else {
				DAplRecvPrintf("[APLRecv-Sync-NotDone] send me:%d cKey: %s \n", kv.me, cKey)
				kv.doneCh <- nil
			}
		}
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	//todo add logic to remove done req,
	//todo add logic to remove notFinish req
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key

	cKey := commendIndexKey("Get", args.ClId, args.ReqId)
	wrap := wrapCKV(cKey, key, "")
	reply.ServId = kv.me
	DServPrintf("Get [Request] serv %d wrap %v\n", kv.me, wrap)

	// ask disposer first
	kv.syncCh <- wrap
	ans := <-kv.doneCh
	if ans != nil {
		kv.GetRet(ans, reply)
		return
	}

	if kv.notFinish[cKey] {
		reply.Err = Error_TimeOut
		return
	}

	ind, term, isL := kv.rf.Start(wrap)
	//todo
	DServPrintf("Get [Reply] serv %d ind %d term %d isL %t\n", kv.me, ind, term, isL)
	if !isL {
		reply.WrongLeader = true
		return
	}

	//todo
	reply.Err = Error_NotWant
	kv.notFinish[cKey] = true

	/*
	//send the cKey
	kv.syncCh <- wrap
	*/

	//wait the answer or timeout
	//kv.waitGet(cKey, reply)
}

func (kv *RaftKV) GetRet(wrap []string, reply *GetReply) {
	c, k, v := getCKV(wrap)
	reply.Value = v
	DServPrintf("Get [Got] serv %d c: %s k: %s v: %s\n", kv.me, c, k, v)
}

func (kv *RaftKV) waitGet(cKey string, reply *GetReply) {
	select {
	case <-time.After(time.Second * 1):
		reply.Err = Error_TimeOut
		kv.notFinish[cKey] = true
	case wrap := <-kv.answerCh:
		//c, _, _ := getCKV(wrap)
		//if c != cKey {
		//	reply.Err = Error_NotWant
		//}
		kv.GetRet(wrap, reply)
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

	// ask disposer first
	kv.syncCh <- wrap
	ans := <-kv.doneCh
	if ans != nil {
		kv.PARet(ans, reply)
		return
	}

	DServPrintf("PutAppend [SyncDone] serv %d wrap %v\n", kv.me, wrap)

	if kv.notFinish[cKey] {
		reply.Err = Error_TimeOut
		return
	}

	//DServPrintf("PutAppend [Start] serv %d wrap %v\n", kv.me, wrap)
	ind, term, isL := kv.rf.Start(wrap)

	DServPrintf("PutAppend [Reply] serv %d wrap %v ind %d term %d isL %t\n", kv.me, wrap, ind, term, isL)
	if !isL {
		reply.WrongLeader = true
		return
	}

	//todo
	kv.notFinish[cKey] = true
	reply.Err = Error_NotWant

	/*
	//send the cKey
	kv.syncCh <- wrap
	*/

	//wait the answer or timeout
	//kv.waitPutAppend(cKey, reply)
}

func (kv *RaftKV) PARet(wrap []string, reply *PutAppendReply) {
	c, k, v := getCKV(wrap)
	DServPrintf("PutAppend [Got] serv %d c: %s k: %s v: %s\n", kv.me, c, k, v)
}

//wait the answer or timeout
func (kv *RaftKV) waitPutAppend(cKey string, reply *PutAppendReply) {
	select {
	case <-time.After(time.Second * 1): // todo, time out 1s ?
		reply.Err = Error_TimeOut
		kv.notFinish[cKey] = true
	case wrap := <-kv.answerCh:
		c, _, _ := getCKV(wrap)
		if c != cKey {
			fmt.Printf("get %s ckey %s\n", c, cKey)
			panic(Error_NotWant)
			reply.Err = Error_NotWant
		}
		kv.PARet(wrap, reply)
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

	kv.mu = sync.Mutex{}
	kv.kvmap = make(map[string]string, 1024)
	kv.notFinish = make(map[string]bool, 1024)
	kv.done = make(map[string]bool, 1024)
	kv.syncCh = make(chan []string)
	kv.answerCh = make(chan []string)
	kv.doneCh = make(chan []string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.disposeAplMsg()
	return kv
}
