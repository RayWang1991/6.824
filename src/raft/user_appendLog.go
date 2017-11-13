package raft

import (
	"sync"
)

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

// role: leader
// broadcast AE RPC to all followers, in order to copy logs to them
// once the majority says YES
// leader update its commit index to the last
// return value: true for successfully got majority's agreement

func (rf *Raft) syncLogsToOthers() bool {
	wg := &sync.WaitGroup{}
	replyCh := make(chan *AppendEntriesReply)
	DLogPrintf("Sender %d Phase 1 SyncLogs %v\n", rf.me, rf.logs)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = len(rf.logs) - 1 // ? every time ?
		go rf.sendAppendLogsTo(i, replyCh, wg)
	}

	suc := 1  // know sayed YES
	errN := 0 // know err (saying no)
	//done := uint(1 << uint(rf.me)) // bit map for recording succ
	foundHigherT := false
	fail := false
	for !rf.MostAgreed(suc) {
		rpl := <-replyCh
		if rpl.Success {
			DLogPrintf("Succ %d \n", rpl.Me)
			//done |= 1 << uint(rpl.Me)
			// update matches
			rf.matchIndex[rpl.Me] = len(rf.logs) - 1
			suc++
		} else if rpl.Error {
			errN ++
			if errN > len(rf.peers) / 2 {
				fail = true
				break
			}
		} else if rpl.Term > rpl.Req.Term {
			DLogPrintf("Found Higher Term in Reply %d > %d for ae Send %d Recv %d\n",
				rpl.Term, rf.currentTerm, rf.me, rpl.Me)
			foundHigherT = true
		} else {
			to := rpl.Me
			rf.mu.Lock()
			if rf.nextIndex[to] >= 0 { // may be error in reply (disconnection)
				rf.nextIndex[to]--
			}
			rf.mu.Unlock()
			//DLogPrintf("-- %d, now is %d\n", rpl.Me, rf.nextIndex[to])
			wg.Add(1)
			go rf.sendAppendLogsTo(to, replyCh, wg) // right to send / receive msgs through replyCh, for want result
		}
	}

	if foundHigherT {
		// finding a higher term
		// notify all,
		go dranRplChA(replyCh, rf.me, rf.currentTerm)
		rf.abort <- struct{}{}
		rf.becomeFollower()
		return false
	} else if fail {
		return false
	} else {
		// Success routine::
		// agreed for most server, send AE to rest of them in another goroutine
		DLogPrintf("Most Agreed \n")
		rf.commitIndex = len(rf.logs)
		for i := range rf.peers {
			//if (done & (1 << uint(i))) > 0 {
			if i == rf.me {
				continue
			}
			DLogPrintf("Resend commit ae index %d to %d\n", rf.commitIndex, i)
			go func(rf *Raft, i int) {
				notified := false
				for rf.IsLeader() && !notified {
					args := rf.aeArg(i)
					reply := &AppendEntriesReply{Term: -1, Req: args}
					ok := rf.sendAppendEntries(i, *args, reply)
					if !ok {
						DPrintf("send AppendEntries to %d failed\n", i)
						return
					}
					if reply.Success {
						DLogPrintf("Id %d Committed for %d\n", reply.Req.Id, reply.Me)
						notified = true
					} else if reply.Error {

					} else {
						if reply.Req.Term >= reply.Term {
							rf.mu.Lock()
							if rf.nextIndex[i] >= 0 { // may be error in reply (disconnection)
								rf.nextIndex[i]--
							}
							rf.mu.Unlock()
							// resend
						} else {
							DLogPrintf("Id %d TODO recv %d Asking Term %d < Reply Term %d\n",
								reply.Req.Id, reply.Me, reply.Req.Term, reply.Term)
							// asking Term < replying Term
							// should break? or leave it
						}
					}
				}
			}(rf, i)
		}
		return true
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
	reply := &AppendEntriesReply{Term: -1, Req: args}
	//DPrintf("send heart beat to %d from %d\n", server, rf.me)
	ok := rf.sendAppendEntries(server, *args, reply)
	if !ok {
		DPrintf("send AppendEntries to %d failed\n", server)
		reply.Error = true
	}
	replyCh <- reply
}

// apply msg til rf's apply index reaches the commit index
func (rf *Raft) syncApplyMsgs() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.mu.Lock()
		ind := rf.lastApplied
		rf.mu.Unlock()
		if ind <= 0 {
			continue
		}

		msg := ApplyMsg{
			Index:   ind,
			Command: rf.logs[ind-1].Content,
		}
		rf.applyCh <- msg
		DLogPrintf("Done Apply %v ind %d [APPLYEE] %d\n", msg.Command, msg.Index, rf.me)
	}
}
