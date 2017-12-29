package raftkv

import "6.824/src/labrpc"
import "crypto/rand"
import "math/big"

var idPool = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id            int // id for client
	reqId         int // request id, auto increase
	lastLeader    int
	orderdServers []*labrpc.ClientEnd
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.orderdServers = make([]*labrpc.ClientEnd, 0, len(servers))
	ck.ReOrderedServers(0)
	// here assume the index of the servers do not change
	ck.id = idPool
	idPool++
	// You'll have to add code here.
	return ck
}

/*
type RPCArg struct {
	Key string
	ID  int
}

type RPCReply struct {
	Key string
	Val string
	Err error
}
*/

// helper method to resort the servers, last leader to be first
func (ck *Clerk) ReOrderedServers(newLeader int) {
	ck.lastLeader = newLeader
	ck.orderdServers = ck.orderdServers[:0]
	ck.orderdServers = append(ck.orderdServers, ck.servers[ck.lastLeader])
	for i, serv := range ck.servers {
		if i == ck.lastLeader {
			continue
		}
		ck.orderdServers = append(ck.orderdServers, serv)
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	clId := ck.id
	reqId := ck.reqId

	gotRes := false
	val := ""

	DClientPrintf("Get [Client] Start me:%d rid:%d For %s\n", ck.id, reqId, key)
	for !gotRes {
		for i, serv := range ck.orderdServers {
			arg := GetArgs{
				ClId:  clId,
				ReqId: reqId,
				Key:   key,
			}

			reply := GetReply{
			}
			ok := serv.Call("RaftKV.Get", &arg, &reply)
			DClientPrintf("Get Client Got ok %t serv %d wrongL %t K %s V %s Err %s\n",
				ok, reply.ServId, reply.WrongLeader, arg.Key, reply.Value, reply.Err)
			if !ok || reply.WrongLeader || reply.Err != "" {
				continue
			}
			gotRes = true
			val = reply.Value
			if i != ck.lastLeader {
				ck.ReOrderedServers(i)
			}
			break
		}
	}
	DClientPrintf("Get [Client] End me:%d rid:%d For %s\n", ck.id, reqId, key)
	ck.reqId ++
	return val
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	clId := ck.id
	reqId := ck.reqId
	DClientPrintf("PutAppend [Client] Start me:%d rId:%d For %s %s %s\n",
		ck.id, reqId, key, value, op)
	gotRes := false
	for !gotRes {
		for i, serv := range ck.orderdServers {
			arg := PutAppendArgs{
				ClId:  clId,
				ReqId: reqId,
				Key:   key,
				Value: value,
				Op:    op,
			}
			reply := PutAppendReply{
			}
			ok := serv.Call("RaftKV.PutAppend", &arg, &reply)
			DClientPrintf("PutAppend Client %d Got ok %t serv %d wrongL %t Err %s Key %s V %s\n",
				ck.id, ok, reply.ServId, reply.WrongLeader, reply.Err, arg.Key, arg.Value)
			if !ok || reply.WrongLeader || reply.Err != "" {
				continue
			}
			gotRes = true
			if i != ck.lastLeader {
				ck.ReOrderedServers(i)
			}
			break
		}
	}
	DClientPrintf("PutAppend [Client] me:%d rId:%d End For %s %s %s\n", ck.id, reqId, key, value, op)
	ck.reqId ++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
