package raft

import (
	"sync"
	"time"
)

// start election, rf is candidate
// rf set time out, if expired and rf hasn't be selected as leader then rf, abort the election and start a new one
func (rf *Raft) startElection() {
	rf.SetUserState(InElection)
	electionFlag := true // loop electionFlag, keep election or not
	beLeader := false

	for electionFlag {
		rf.currentTerm++
		rf.votedFor = rf.me
		DPrintf("Start Election for %d on Term %d\n", rf.me, rf.currentTerm)
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
			if i == rf.me {
				continue
			}
			wg.Add(1)
			DPrintf("Send Vote Req to %d from %d on Term %d\n", i, rf.me, rf.currentTerm)
			go rf.sendRequestVoteTo(i, args, replyCh, wg)
		}

		// reply ch closer
		go func(ch chan *RequestVoteReply) {
			wg.Wait()
			close(ch)
		}(replyCh)

		waitFlag := true
		succ := 1
		if succ > len(rf.peers)/2 {
			electionFlag = false
			waitFlag = false
			beLeader = true
		}

		for waitFlag {
			select {
			case rpl := <-replyCh:
				if rpl.VoteGranted {
					DPrintf("Agree on asking Request Vote to %d from %d\n", rf.me, rpl.Me)
					succ++
					if succ > len(rf.peers)/2 {
						electionFlag = false
						waitFlag = false
						beLeader = true
					}
				} else {
					if rpl.Term > rf.currentTerm {
						rf.currentTerm = rpl.Term
						rf.votedFor = -1
						electionFlag = false
						waitFlag = false
						beLeader = false
					}
				}
			case <-rf.abort: // may receive heart beats from leader
				electionFlag = false
				waitFlag = false
				beLeader = false
			case <-time.After(randomTimeOut()):
				// keep election
				waitFlag = false
			}
		}
		go dranRplCh(replyCh, rf.me, rf.currentTerm)
	}
	if beLeader {
		rf.becomeLeader()
	} else {
		rf.becomeFollower()
	}
}

func (rf *Raft) startElection1() {
	rf.SetUserState(InElection)
	rf.currentTerm++
	rf.votedFor = rf.me

	//TODO, time out for election
	DPrintf("Start Election for %d on Term %d\n", rf.me, rf.currentTerm)
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
	done := make(chan struct{})

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		DPrintf("Send Vote Req to %d from %d on Term %d\n", i, rf.me, rf.currentTerm)
		go rf.sendRequestVoteTo(i, args, replyCh, wg)
	}

	// reply ch closer
	go func(ch chan *RequestVoteReply) {
		wg.Wait()
		close(ch)
	}(replyCh)

	// reply msg receiver
	go func(ch chan *RequestVoteReply, done chan struct{}) {
		succ := 1
		if succ > len(rf.peers)/2 {
			go dranRplCh(replyCh, rf.me, rf.currentTerm)
			done <- struct{}{}
			return
		}
		for rpl := range ch {
			if rpl.VoteGranted {
				DPrintf("Agree on asking Request Vote to %d from %d\n", rf.me, rpl.Me)
				succ++
				if succ > len(rf.peers)/2 {
					go dranRplCh(replyCh, rf.me, rf.currentTerm)
					done <- struct{}{}
					return
				}
			} else {
				if rpl.Term > rf.currentTerm {
					rf.currentTerm = rpl.Term
					rf.votedFor = -1
					go dranRplCh(replyCh, rf.me, rf.currentTerm)
					rf.abort <- struct{}{}
					return
				}
			}
		}
		// not got enough support from majority
		DPrintf("Election Failed for %d on %d\n", rf.me, rf.currentTerm)
	}(replyCh, done)

	// waiting for done / time out / abort
	select {
	case <-time.After(randomTimeOut()):
		// new election
		// TODO, may use iteration instead of recursion
		rf.startElection()
	case <-done:
		// win the election
		rf.SetUserState(None)
		rf.becomeLeader()
	case <-rf.abort:
		// found higher Term in reply
		rf.SetUserState(None)
		rf.becomeFollower()
	}
}

func (rf *Raft) sendRequestVoteTo(
	server int,
	args *RequestVoteArgs,
	replyCh chan *RequestVoteReply,
	wg *sync.WaitGroup) {
	reply := &RequestVoteReply{Term: -1, VoteGranted: false}
	ok := rf.sendRequestVote(server, *args, reply)
	if !ok {
		// TODO keep asking or election time out?
		DPrintf("Send RequestVote For to %d from %d failed\n", server, rf.me)
		//ok = rf.sendRequestVote(server, *args, reply)
	}
	replyCh <- reply
	wg.Done()
}
