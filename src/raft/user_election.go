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
		DVotePrintf("Start Election for %d on Term %d\n", rf.me, rf.currentTerm)

		wg := &sync.WaitGroup{}
		replyCh := make(chan *RequestVoteReply)

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			lastLogTerm := rf.PrevLogTermFor(i)
			lastLogIndex := rf.PrevLogIndexFor(i)
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			wg.Add(1)
			DVotePrintf("Send Vote Req to %d from %d on Term %d\n", i, rf.me, rf.currentTerm)
			go rf.sendRequestVoteTo(i, args, replyCh, wg)
		}

		// reply ch closer
		go func(ch chan *RequestVoteReply) {
			wg.Wait()
			close(ch)
		}(replyCh)

		waitFlag := true
		succ := 1
		if rf.MostAgreed(succ) {
			electionFlag = false
			waitFlag = false
			beLeader = true
		}

		for waitFlag {
			select {
			case rpl := <-replyCh:
				if rpl.VoteGranted {
					DVotePrintf("Agree on asking Request Vote to %d from %d\n", rf.me, rpl.Me)
					succ++
					if rf.MostAgreed(succ) {
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
		go dranRplChV(replyCh, rf.me, rf.currentTerm)
	}
	if beLeader {
		rf.becomeLeader()
	} else {
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
		DVotePrintf("Send RequestVote For to %d from %d failed\n", server, rf.me)
		//ok = rf.sendRequestVote(server, *args, reply)
	}
	replyCh <- reply
	wg.Done()
}
