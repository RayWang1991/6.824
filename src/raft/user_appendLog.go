package raft

import "sync"

// TODO
var argId = 0

var mu = &sync.Mutex{}

func getId() int {
	mu.Lock()
	res := argId
	argId++
	mu.Unlock()
	return res
}

func (rf *Raft) appendLogToOthers() {
	log := rf.logs[len(rf.logs)-1]
	wg := &sync.WaitGroup{}
	replyCh := make(chan *AppendEntriesReply)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = len(rf.logs) - 1
		go rf.sendAppendLogsTo(i, replyCh, wg)
	}
	DLogPrintf("Phase 1\n")

	foundHigherT := false
	suc := 1

	done := uint(1 << uint(rf.me)) // bit map for recording succ

	for !rf.MostAgreed(suc) {
		//DLogPrintf("next idxs: %v\n", rf.nextIndex)
		rpl := <-replyCh
		if rpl.Success {
			DLogPrintf("Succ %d \n", rpl.Me)
			done |= 1 << uint(rpl.Me)
			rf.matchIndex[rpl.Me] = len(rf.logs) - 1
			suc++
		} else {
			if rpl.Term > rf.currentTerm {
				DLogPrintf("Found Higher Term in Reply %d > %d\n", rpl.Term, rf.currentTerm)
				foundHigherT = true
				break // break loop
			} else {
				to := rpl.Me
				rf.nextIndex[to]--
				//DLogPrintf("-- %d, now is %d\n", rpl.Me, rf.nextIndex[to])
				wg.Add(1)
				go rf.sendAppendLogsTo(to, replyCh, wg) // right to send / receive msgs through replyCh, for want result
			}
		}
	}

	if foundHigherT {
		// finding a higher term
		// notify all,
		go dranRplChA(replyCh, rf.me, rf.currentTerm)
		rf.becomeFollower()
	} else {
		// agreed for most server
		// TODO, just increase index ?
		msg := &ApplyMsg{Index: len(rf.logs), Command: log.Content}

		rf.applyCh <- *msg
		rf.lastApplied ++
		DLogPrintf("Committed! commit index %d matches nexts:%v %v done:%v\n",
			rf.commitIndex, rf.matchIndex, rf.nextIndex, decodeBitMap(done))
		// TODO ,insert log to see done bitmap and
		// send committed rpcs to all followers
		for i := range rf.peers {
			if (done & (1 << uint(i))) > 0 {
				continue
			}
			//DLogPrintf("Resend commit ae indx %d to %d\n", rf.commitIndex, i)
			go func(rf *Raft, i int) {
				notified := false
				for rf.IsLeader() && !notified {
					args := rf.aeArg(i)
					reply := &AppendEntriesReply{Term: -1}
					ok := rf.sendAppendEntries(i, *args, reply)
					if !ok {
						DPrintf("send AppendEntries to %d failed\n", i)
					}
					if reply.Success {
						DLogPrintf("Id %d Committed for %d\n", reply.Id, reply.Me)
						notified = true
					} else {
						rf.mu.Lock()
						t := rf.currentTerm
						rf.mu.Unlock()
						if t >= reply.Term {
							rf.nextIndex[i]--
							// resend
						} else {
							// should break? or leave it
						}
					}
				}
			}(rf, i)
		}
	}
}

func (rf *Raft) aeArg(server int) *AppendEntriesArg {
	return &AppendEntriesArg{
		Id:           getId(),
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.PrevLogIndexFor(server),
		PrevLogTerm:  rf.PrevLogTermFor(server),
		Logs:         rf.logs[rf.PrevLogIndexFor(server)+1:],
		LeaderCommit: rf.GetCommitIndex(),
	}
}

// wrapper
func (rf *Raft) sendAppendLogsTo(
	server int,
	replyCh chan *AppendEntriesReply,
	wg *sync.WaitGroup) {
	args := rf.aeArg(server)
	reply := &AppendEntriesReply{Term: -1}
	//DPrintf("send heart beat to %d from %d\n", server, rf.me)
	ok := rf.sendAppendEntries(server, *args, reply)
	if !ok {
		//DPrintf("send AppendEntries to %d failed\n", server)
	}
	replyCh <- reply
}
