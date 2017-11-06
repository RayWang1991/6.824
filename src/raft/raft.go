package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"6.824/src/labrpc"
	"bytes"
	"encoding/gob"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type RaftPeerRole int

const (
	TermKey                = 999
	ActionKey              = 888
	Follower  RaftPeerRole = iota
	Candidate
	Leader
)

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[], self Id

	// persistent states
	role        RaftPeerRole  // role that raft think itself is
	currentTerm int           // current term
	votedFor    int           // candidate id(index) that vote for, -1 for null
	logs        []map[int]int // logs containing term key and action key

	// volatile states
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// volatile state on leaders
	nextIndex  []int // record NextIndex of log entries for all node (all initialized to leader's last index + 1)
	matchIndex []int // index of highest log entry known to be replicated on server

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg
	timeOut int // time out time in milsec
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	err := d.Decode(&rf.currentTerm)
	if err != nil {
		DPrintf("error in decode \"currentTerm\"")
	}
	err = d.Decode(&rf.votedFor)
	if err != nil {
		DPrintf("error in decode \"votedFor\"")
	}
	err = d.Decode(&rf.logs)
	if err != nil {
		DPrintf("error in decode \"logs\"")
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	term         int
	candidateId  int
	lastLogIndex int // use to compare whose log is newer
	lastLogTerm  int // use to compare whose log is newer
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	term        int
	voteGranted bool
}

// logic for comparing which log is newer(inclusive)
func isNewerLog(aIdx, aTerm int, bIdx, bTerm int) bool {
	if aTerm != bTerm {
		return aTerm >= bTerm
	}
	return aIdx >= bIdx
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	meTerm := rf.currentTerm
	canTerm := args.term
	res := false
	if canTerm >= meTerm {
		if rf.votedFor < 0 || rf.votedFor == args.candidateId {
			if len(rf.logs) == 0 {
				res = true
			} else {
				lastlogIdx := len(rf.logs) - 1
				log := rf.logs[lastlogIdx]
				if isNewerLog(args.lastLogTerm, args.lastLogIndex, log[TermKey], lastlogIdx) {
					res = true
				}
			}
		}
	}

	reply.term = meTerm
	reply.voteGranted = res
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := rf.currentTerm
	if rf.role != Leader {
		return -1, term, false
	}
	log := map[int]int{TermKey: rf.currentTerm, ActionKey: 99} // TODO, may use meaningful int for debug
	rf.logs = append(rf.logs, log)
	rf.sendRequestVote()
	go func(){
		rf.applyCh
	}()

	return index, term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	DPrintf("Kill peer %d\n", rf.me)
	// TODO, rf debug string
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	DPrintf("Make peers:%v me%v \n", peers, me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.logs = make([]map[int]int, 0, 128)
	rf.currentTerm = -1
	rf.role = Follower
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.matchIndex = make([]int, 0, 128)
	rf.nextIndex = make([]int, 0, 128)
	rf.applyCh = applyCh
	rf.persist()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

type AppendEntriesArg struct {
}

type AppendEntriesReply struct {
}

// My Code goes here
func (rf *Raft) AppendEntries(args AppendEntriesArg, reply *AppendEntriesReply) {

}

// wrapper for append entries call
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArg, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
