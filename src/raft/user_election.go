package raft

import (
	"time"
	"sync"
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

		replyCh := make(chan *RequestVoteReply)
		wg := &sync.WaitGroup{}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			lastLogT := -1
			lastLogI := len(rf.logs) - 1
			if lastLogI >= 0 {
				lastLogT = rf.logs[lastLogI].Term
			}
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogI,
				LastLogTerm:  lastLogT,
			}
			DVotePrintf("Send Vote Req to %d from %d on Term %d\n", i, rf.me, rf.currentTerm)
			wg.Add(1)
			go rf.sendRequestVoteTo(i, args, replyCh, wg)
		}

		// TODO, no
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
						rf.maxId = 0
						rf.votedFor = -1
						electionFlag = false
						waitFlag = false
						beLeader = false
					}
				}
			case <-rf.abort: // may receive heart beats from leader
				DVotePrintf("Election aborted %d\n", rf.me)
				electionFlag = false
				waitFlag = false
				beLeader = false
				rf.abort <- struct{}{}

			case <-time.After(randomTimeOut()):
				// keep election
				waitFlag = false
			}
		}

		go dranRplChV(replyCh, rf.me, rf.currentTerm)
		go func(ch chan *RequestVoteReply, wg *sync.WaitGroup) {
			wg.Wait()
			close(ch)
		}(replyCh, wg)
	}

	rf.state = None
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
		DVotePrintf("Send RequestVote sender %d to %d [failed, Error]\n", rf.me, server)
		//ok = rf.sendRequestVote(server, *args, reply)
	}
	replyCh <- reply
	wg.Done()
}
