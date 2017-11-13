package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
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

type RaftState int

const (
	None               RaftState = iota
	InElection
	InRecvHeartBeat
	InSendingHeartBeat
)

const (
	Follower  RaftPeerRole = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Content interface{}
}

type Raft struct {
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[], self Id

	// persistent states
	mu          *sync.Mutex
	role        RaftPeerRole // role that raft think itself is
	currentTerm int          // current Term
	votedFor    int          // candidate id(index) that vote for, -1 for null
	logs        []LogEntry   // Logs containing Term key and action key

	// volatile states
	commitIndex int // index of highest log entry known to be committed, 1 for first...
	lastApplied int // index of highest log entry applied to state machine

	state RaftState // role that raft think itself is

	// volatile state on leaders
	nextIndex  []int // record NextIndex of log entries for all node (all initialized to leader's last index + 1)
	matchIndex []int // index of highest log entry known to be replicated on server

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	abort     chan struct{} // abort chan, reuse, candidate for election, leader for heartbeat sending, follower to receiving
	heartBeat chan struct{} // heartBeat chan, for sending and receiving heart beat

	applyCh chan ApplyMsg
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (ind int, term int, isL bool) {
	term = rf.currentTerm
	ind = rf.GetCommitIndex()
	//ind = rf.commitIndex + 1

	DLogPrintf(rf.CommitStr())
	if !rf.IsLeader() {
		// non leader
		//DPrintf("Command on %d, no leader\n", rf.me)
		DLogPrintf("Return Start Answer:%d Commit:%d Term:%d IsLeader:%t\n", rf.me, ind, term, isL)
		return
	}
	DPrintf("[Command] %v on %d, Leader\n", command, rf.me)
	//DPrintf(rf.DebugStr())
	// leader
	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Content: command})
	ind = len(rf.logs)

	// send append entries to all followers (exclude self)
	// copy the history if needed
	//rf.sendRequestVote()

	ok := rf.syncLogsToOthers()
	if ok {
		DLogPrintf("Sync Logs [Succeed] matches:%v\n", rf.matchIndex)
		rf.syncApplyMsgs()
	} else {
		DLogPrintf("Sync Logs [Fail] matches:%v\n", rf.matchIndex)
	}
	isL = rf.IsLeader()
	term = rf.currentTerm
	DLogPrintf("Return Start Answer:%d Commit:%d Term:%d IsLeader:%t\n", rf.me, ind, term, isL)

	return
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
	DPrintf(rf.DebugStr())
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
	rf.mu = &sync.Mutex{}
	rf.state = None

	// Your initialization code here.
	rf.logs = make([]LogEntry, 0, 128)
	rf.currentTerm = -1
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(peers), 16)
	rf.nextIndex = make([]int, len(peers), 16)
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = -1
		rf.matchIndex[i] = -1
	}

	rf.abort = make(chan struct{})
	rf.heartBeat = make(chan struct{})
	rf.applyCh = applyCh // TODO
	rf.persist()

	rf.SetRole(Follower)
	go rf.startRecvHeartBeats()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// TODO
	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.IsLeader()
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
		DPrintf("error in decode \"Logs\"")
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int // use to compare whose log is newer
	LastLogTerm  int // use to compare whose log is newer
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	ReplyId     int
	Me          int
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	meTerm := rf.currentTerm
	canTerm := args.Term
	res := false
	DVotePrintf("Vote Request term:%d %d, sed,recv:%d %d\n", canTerm, meTerm, args.CandidateId, rf.me)
	DVotePrintf("receiver: %s\n", rf.DebugStr())
	if canTerm >= meTerm {
		if canTerm > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1 // new term, update votedFor
			DVotePrintf("term change %d > %d\n", canTerm, meTerm)
			if rf.GetRole() != Follower {
				if rf.IsBusy() {
					rf.abort <- struct{}{}
				}
				DHBPrintf("RF BF Vote Rcv\n")
				rf.becomeFollower()
			}
		}
		if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
			if len(rf.logs) == 0 {
				res = true
			} else {
				lastlogIdx := len(rf.logs) - 1
				log := rf.logs[lastlogIdx]
				if isNewerLog(args.LastLogTerm, args.LastLogIndex, log.Term, lastlogIdx) {
					res = true
				}
			}
		}
	}
	// TODO cmp log latest
	if res {
		DVotePrintf("Win vote recv %d to %d\n", rf.me, args.CandidateId)
	} else {
		DVotePrintf("Lose vote recv %d to %d vote for %d send log indx %d term %d recv log %v\n",
			rf.me, args.CandidateId, rf.votedFor, args.LastLogIndex, args.LastLogTerm, rf.logs)
	}
	reply.Me = rf.me
	reply.Term = meTerm
	reply.VoteGranted = res
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

type AppendEntriesArg struct {
	Id           int        // arg id
	Term         int        // leader's Term
	LeaderId     int        // Leader Id
	Logs         []LogEntry // Logs
	PrevLogIndex int        // index of prev log
	PrevLogTerm  int        // index of prev Term
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Req     *AppendEntriesArg
	Me      int
	Term    int // for leader to update itself's Term(role)
	Success bool
	Error   bool // connection error or others, here just use boolean
}

// My Code goes here
func (rf *Raft) AppendEntries(args AppendEntriesArg, reply *AppendEntriesReply) {
	reply.Me = rf.me
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		DHBPrintf("CanTerm > meTerm %d %d\n", args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		DHBPrintf("Now Term is %d for %d in Append Entries\n", rf.currentTerm, rf.me)
		DHBPrintf("Role is %s for %d\n", rf.RoleStr(), rf.me)
		if rf.GetRole() != Follower {
			if rf.IsBusy() {
				DHBPrintf("Send Abort %d\n", rf.me)
				rf.abort <- struct{}{}
				DHBPrintf("Abort Done %d\n", rf.me)
			}
			DHBPrintf("RF BF AE\n")
			rf.becomeFollower()
		}
	}

	if len(args.Logs) == 0 {
		// Heart beat
		DHBPrintf("Recv HB Id %d from %d to %d\n", args.Id, args.LeaderId, rf.me)
		if rf.GetUserState() == InRecvHeartBeat {
			rf.heartBeat <- struct{}{}
		}
		return
	}

	DLogPrintf("AE Id %d recv:%d args:%s\n", args.Id, rf.me, args.DebugStr())
	// log append
	if args.PrevLogIndex >= len(rf.logs) {
		DLogPrintf("Exceeds PreLogIndex %d > %d\n", args.PrevLogIndex, len(rf.logs))
		reply.Success = false
		return
	}

	if args.PrevLogIndex >= 0 && args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		DLogPrintf("Term Mismatch %d != %d\n", args.PrevLogTerm, rf.logs[args.PrevLogIndex].Term)
		reply.Success = false
		return
	}

	// match, copy(and may overwrite) logs
	rf.logs = rf.logs[:args.PrevLogIndex+1] // prev log index may be -1
	for _, log := range args.Logs {
		rf.logs = append(rf.logs, log)
	}

	rf.mu.Lock()
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = args.LeaderCommit
	}
	if rf.commitIndex > len(rf.logs) {
		rf.commitIndex = len(rf.logs)
	}
	rf.mu.Unlock()

	reply.Success = true

	DLogPrintf("Recv %d Server Commit:%d self Commit:%d lastApply:%d self log %v\n",
		rf.me, args.LeaderCommit, rf.commitIndex, rf.lastApplied, rf.logs)

	rf.syncApplyMsgs()

	DLogPrintf("ON Reply %d commitIdx is %d\n", args.Id, rf.GetCommitIndex())
}

// wrapper for append entries call
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArg, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
