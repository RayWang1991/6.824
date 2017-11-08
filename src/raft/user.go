package raft

import (
	"time"
	"sync"
)


// for follower set time out for not receiving heart beat
func (rf *Raft) resetHeartBeatTimeOut() {
	//DPrintf("resetHeartBeatFor %d %v\n", rf.me, time.Now())
	if !rf.timer.Stop() {
		select {
		case <-rf.timer.C:
		default:
		}
	}
	if rf.abort != nil {
		// already start a heart beat timeout
		//DPrintf("abort send msg %d\n", rf.me)
		rf.abort <- struct{}{}
	} else {
		// first enter
		//DPrintf("abort is nil %d\n", rf.me)
		rf.abort = make(chan struct{})
	}

	rf.timer.Reset(randomTimeOut())

	// consumer
	go func() {
		select {
		case <-rf.abort:
			//DPrintf("HB recv abort msg %d\n", rf.me)
			return
		case <-rf.timer.C:
			//DPrintf("HeartBeat Time Out msg %d %v\n", rf.me, time.Now())
			rf.becomeCandidate()
			return
		}
	}()
}

func (rf *Raft) startElection() {
	//TODO, time out for election
	total := 0
	lastLogTerm := rf.PrevLogTerm()
	lastLogIndex := rf.PrevLogIndex()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	wg := &sync.WaitGroup{}
	replyCh := make(chan *RequestVoteReply)
	for i := range rf.peers {
		wg.Add(1)
		go rf.sendRequestVoteTo(i, args, replyCh, wg)
	}

	go func() {
		wg.Wait()
		close(replyCh)
	}()

loop:
	for rpl := range replyCh {
		if rpl.VoteGranted {
			total ++
			// check
			if total > len(rf.peers)/2 { // TODO ?
				// become leader
				// drain rpl
				rf.becomeLeader()
				go func() {
					for range replyCh {
					}
					DPrintf("Drain reply Channel for Request Vote\n")
				}()
				break loop
			}
		} else {
			// dispose those
			DPrintf("Disagree on asking Request Vote to %d from %d\n", rf.me, rpl.Me)
			if rpl.Term > rf.currentTerm {
				rf.currentTerm = rpl.Term
				rf.becomeFollower()
			}
		}
	}
}

func (rf *Raft) sendRequestVoteTo(
	server int,
	args *RequestVoteArgs,
	replyCh chan *RequestVoteReply,
	wg *sync.WaitGroup) {
	reply := &RequestVoteReply{Term: -1}
	ok := rf.sendRequestVote(server, *args, reply)
	if !ok {
		//DPrintf("send RequestVoteFor to %d failed\n", server)
	}
	replyCh <- reply
	wg.Done()
}



func (rf *Raft) closeTicker() {
	if rf.ticker == nil {
		return
	}
	DPrintf("Close ticker %d\n", rf.me)
	rf.ticker.Stop()
}

const HEARTBEAT_PERIOD = 50 * time.Millisecond

func (rf *Raft) sendHeartBeatsAll(replyCh chan *AppendEntriesReply, wg *sync.WaitGroup) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &AppendEntriesArg{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.PrevLogIndex(), // TODO , error should be from next index
			PrevLogTerm:  rf.PrevLogTerm(),
			LeaderCommit: rf.commitIndex,
		}
		wg.Add(1)
		go rf.sendHeartBeatsTo(i, args, replyCh, wg)
		//DPrintf("Send HEART BEAT %v\n", time.Now())
	}
}

func (rf *Raft) startHeartBeatsPer() {
	rf.ticker = time.NewTicker(HEARTBEAT_PERIOD)
	replyCh := make(chan *AppendEntriesReply)
	wg := &sync.WaitGroup{}
	ind := 0
	// send heart beat right now to maintain authority
	rf.sendHeartBeatsAll(replyCh, wg)

loop:
	for {
		//DPrintf("loop %d \n", ind)
		select {
		case <-rf.ticker.C:
			//DPrintf("in C %d\n", ind)
			rf.sendHeartBeatsAll(replyCh, wg)
			//DPrintf("out C %d %v\n", ind, time.Now())
		case reply := <-replyCh:
			//DPrintf("in REP %d\n", ind)
			//DPrintf("Recv reply from %d succ %t", reply.Me, reply.Success)
			if !reply.Success && reply.Term > rf.currentTerm {
				// find higher Term and become follower
				DPrintf("Find higher Term #%d, self Term #%d, become follower\n", reply.Term, rf.currentTerm)
				rf.currentTerm = reply.Term
				rf.becomeFollower()
				break loop
			}
			//DPrintf("out REP %d\n", ind)
		}
		ind ++
	}
	// drain and close reply channel
	go func() {
		wg.Wait()
		close(replyCh)
	}()

	go func() {
		for range replyCh {
		}
		DPrintf("Drain replay Channel for HeartBeat\n")
	}()
}

func (rf *Raft) sendHeartBeatsTo(
	server int,
	args *AppendEntriesArg,
	replyCh chan *AppendEntriesReply,
	wg *sync.WaitGroup) {
	reply := &AppendEntriesReply{Term: -1}
	//DPrintf("send heart beat to %d from %d\n", server, rf.me)
	ok := rf.sendAppendEntries(server, *args, reply)
	if !ok {
		DPrintf("send AppendEntries to %d failed\n", server)
	}
	replyCh <- reply
	wg.Done()
}
