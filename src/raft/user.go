package raft

import (
	"math/rand"
	"time"
)

// return a time that will in rand [150,300) ms
func randomTimeOut() time.Duration {
	t := time.Duration(rand.Intn(150)+150) * time.Millisecond // rand [150,300) ms to time out
	return t
}

func (rf *Raft) resetTimeOut() {
	if !rf.timer.Stop() {
		<-rf.timer.C
	}
	rf.timer.Reset(randomTimeOut())
	go checkTimeOut(rf, startElection)
}

// timeout func to
func checkTimeOut(rf *Raft, rfunc RfFunc) {
	<-rf.timer.C
	// expired and fire
	rfunc(rf)
}

type RfFunc func(*Raft)

func startElection(rf *Raft) {
	rf.currentTerm ++
	total := 0
	var lastLogTerm, lastLogIndex int
	if len(rf.logs) > 0 {
		log := rf.logs[len(rf.logs)-1]
		lastLogTerm = log[TermKey]
		lastLogIndex = len(rf.logs) - 1
	} else {
		lastLogIndex = -1
		lastLogTerm = -1
	}
	args := RequestVoteArgs{
		term:         rf.currentTerm,
		candidateId:  rf.me,
		lastLogIndex: lastLogIndex,
		lastLogTerm:  lastLogTerm,
	}
	reply := &RequestVoteReply{
	}
	for i := range rf.peers {
		// TODO, parallel
		ok := rf.sendRequestVote(i, args, reply)
		if !ok { // should we always retry?
			DPrintf("send RequestVote to %d faild\n", i)
		}
		if reply.voteGranted {
			total ++
			// check
			if total > len(rf.peers)/2 {
				// become leader
			}
		}
	}
}

// follower to candidate
func (rf *Raft) becomeCandidate() {
	rf.role = Candidate
}

// candidate to leader
func (rf *Raft) becomeLeader() {
	rf.role = Leader
}

// candidate / leader to follower
func (rf *Raft) becomeFollower() {
	rf.role = Follower
}

const HEARTBEAT_PERIOD = 100 * time.Millisecond

func (rf *Raft) startHeartBeats() {
	ticker := time.NewTicker(HEARTBEAT_PERIOD)
	<-ticker.C
	for i := range rf.peers {
		args := AppendEntriesArg{
			term:         rf.currentTerm,
			leaderId:     rf.me,
			prevLogIndex: rf.PrevLogIndex(),
			prevLogTerm:  rf.PrevLogTerm(),
			leaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{
		}
		ok := rf.sendAppendEntries(i, args, reply)
		if !ok {
			DPrintf("send AppendEntries to %d failed\n", i)
		}
		// read reply and dispose
	}
}

func (rf *Raft)
