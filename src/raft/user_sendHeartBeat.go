package raft

import (
	"time"
	"sync"
)

const HEARTBEAT_PERIOD = 50 * time.Millisecond

// use less ae rpc to append logs to others
func (rf *Raft) lessSendHeartBeats() {
	DHBPrintf("Start Send HB rf %d\n", rf.me)
	rf.SetUserState(InSendingHeartBeat)
	nomarlCh := make(chan *AppendEntriesReply)
	wg := &sync.WaitGroup{}
	timer := time.NewTimer(HEARTBEAT_PERIOD)
	rf.sendHeartBeatsAll__(nomarlCh, wg)

	// normal heart beat disposer
	// should be aware that total num > 1 !!!
	total := len(rf.peers)
	go func(rplch chan *AppendEntriesReply, term, me int) {
		var maxIndex = 0
		var done uint = 1 << uint(me) // bitmap for
		for rpl := range rplch {
			if rpl.Success {
				//DPrintf("[MAX] indx %d\n",maxIndex)
				ci := rpl.Req.PrevLogIndex + len(rpl.Req.Logs) + 1
				if ci < maxIndex {
					//leave it, expired reply
				} else if ci > maxIndex {
					maxIndex = ci // update max index
					done = 1 << uint(me)
				} else {
					done |= 1 << uint(rpl.Me)
					succ := succNum(done, total)
					if rf.MostAgreed(succ) {
						DLogPrintf("[Done] maxIndex %d done %v\n", maxIndex, decodeBitMap(done))
						rf.mu.Lock()
						rf.commitIndex = maxIndex
						//DPrintf("[CI] %d\n",rf.commitIndex)
						rf.matchIndex[rpl.Me] = len(rf.logs) - 1
						rf.mu.Unlock()
						rf.syncApplyMsgs__()
					}
				}
			} else if rpl.Term > term {
				// found higher Term
				DHBPrintf("Higher Term on reply heart beat from %d %d > %d\n", me, rpl.Term, term)
				rf.currentTerm = term
				rf.votedFor = -1
				if rf.GetUserState() == InSendingHeartBeat {
					rf.abort <- struct{}{}
				}
				return
			} else { // index not match
				rf.mu.Lock()
				if rf.nextIndex[rpl.Me] >= 0 { // may be error in reply (disconnection)
					rf.nextIndex[rpl.Me]--
				}
				rf.mu.Unlock()
			}
		}
	}(nomarlCh, rf.currentTerm, rf.me)

loop:
	for {
		select {
		case <-rf.abort:
			DPrintf("HB send abort!!! msg %d\n", rf.me)
			break loop
		case <-timer.C:
			timer.Reset(HEARTBEAT_PERIOD) // timer must be triggered
			rf.sendHeartBeatsAll__(nomarlCh, wg)
		case <-rf.aeResCh:
			if ok := timer.Stop(); !ok { // timer must not be triggered
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(HEARTBEAT_PERIOD)
			// Do sync logs to others, in another goroutine
			rf.sendHeartBeatsAll__(nomarlCh, wg)
		}
	}

	DHBPrintf("End Send HB rf %d\n", rf.me)
	// closer for reply channel
	go func() {
		wg.Wait()
		close(nomarlCh)
	}()
	rf.SetUserState(None)
	rf.becomeFollower()
}

// one goroutine loop for sending AE to followers
// once got cmd, start a sync func to append logs to followers, if succeed (most agreed), update commit index
func (rf *Raft) loopSendHeartBeats() {
	DHBPrintf("Start Send HB rf %d\n", rf.me)
	rf.SetUserState(InSendingHeartBeat)
	nomarlCh := make(chan *AppendEntriesReply)
	wg := &sync.WaitGroup{}
	timer := time.NewTimer(HEARTBEAT_PERIOD)
	rf.sendHeartBeatsAll__(nomarlCh, wg)

	// normal heart beat disposer
	go func(rplch chan *AppendEntriesReply, term, me int) {
		for rpl := range rplch {
			if !rpl.Success && rpl.Term > term {
				// found higher Term
				DHBPrintf("Higher Term on reply heart beat from %d %d > %d\n", me, rpl.Term, term)
				rf.currentTerm = term
				rf.votedFor = -1
				if rf.GetUserState() == InSendingHeartBeat {
					rf.abort <- struct{}{}
				}
			} else if !rpl.Success { // index not match
				rf.mu.Lock()
				if rf.nextIndex[rpl.Me] >= 0 { // may be error in reply (disconnection)
					rf.nextIndex[rpl.Me]--
				}
				rf.mu.Unlock()
			}
		}
	}(nomarlCh, rf.currentTerm, rf.me)

	for {
		select {
		case <-rf.abort:
			DPrintf("HB send abort!!! msg %d\n", rf.me)
			break
		case <-timer.C:
			timer.Reset(HEARTBEAT_PERIOD) // timer must be triggered
			rf.sendHeartBeatsAll__(nomarlCh, wg)
		case resCh := <-rf.aeResCh:
			if ok := timer.Stop(); !ok { // timer must not be triggered
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(HEARTBEAT_PERIOD)
			// Do sync logs to others, in another goroutine
			go rf.syncLogsToOthers__(resCh)
		}
	}

	DHBPrintf("End Send HB rf %d\n", rf.me)
	// closer for reply channel
	go func() {
		wg.Wait()
		close(nomarlCh)
	}()
}

func (rf *Raft) syncLogsToOthers__(res chan bool) {
	wg := &sync.WaitGroup{}
	replyCh := make(chan *AppendEntriesReply)
	DLogPrintf("Sender %d Phase 1 SyncLogs %v\n", rf.me, rf.logs)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = len(rf.logs) - 1 // ? every time ?, TODO
		wg.Add(1)
		go rf.sendAETo(i, replyCh, wg)
		DHBPrintf("[AE]Send HEART BEAT sender %d to %d %v\n", rf.me, i, time.Now())
	}
	suc := 1  // know sayed YES
	errN := 0 // know err (saying no)
	//done := uint(1 << uint(rf.me)) // bit map for recording succ
	foundHigherT := false
	fail := false
	for !rf.MostAgreed(suc) {
		rpl := <-replyCh
		if rpl.Success {
			DLogPrintf("Succ for server %d \n", rpl.Me)
			//done |= 1 << uint(rpl.Me)
			// update matches
			rf.matchIndex[rpl.Me] = len(rf.logs) - 1
			suc++
		} else if rpl.Error {
			errN ++
			if errN > len(rf.peers)/2 {
				fail = true
				break
			}
		} else if rpl.Term > rpl.Req.Term {
			DPrintf("Found Higher Term in Reply %d > %d for ae Send %d Recv %d\n",
				rpl.Term, rf.currentTerm, rf.me, rpl.Me)
			foundHigherT = true
			break
		} else {
			to := rpl.Me
			rf.mu.Lock()
			if rf.nextIndex[to] >= 0 { // may be error in reply (disconnection)
				rf.nextIndex[to]--
			}
			rf.mu.Unlock()
			//DLogPrintf("-- %d, now is %d\n", rpl.Me, rf.nextIndex[to])
			wg.Add(1)
			go rf.sendAETo(to, replyCh, wg) // right to send / receive msgs through replyCh, for want result
			DHBPrintf("[AE]Send HEART BEAT sender %d to %d %v\n", rf.me, to, time.Now())
		}
	}

	// ch drainer
	go dranRplChA(replyCh, rf.me, rf.currentTerm)

	// ch closer
	go func(ch chan *AppendEntriesReply, wg *sync.WaitGroup) {
		wg.Wait()
		close(ch)
	}(replyCh, wg)

	if foundHigherT {
		// finding a higher term
		// notify all,
		if rf.IsBusy() {
			rf.abort <- struct{}{}
		}
		DPrintf("[ABORT HB]\n")
		rf.becomeFollower()
		res <- false
		return
	} else if fail {
		res <- false
		return
	} else {
		// Success routine::
		// agreed for most server, send AE to rest of them in another goroutine
		DLogPrintf("Most Agreed \n")
		rf.SetCommitIndex(len(rf.logs))
		res <- true
		return
	}
}

// developing
func (rf *Raft) aeArg__(server int) *AppendEntriesArg {
	return &AppendEntriesArg{
		Id:           getId(),
		Term:         rf.GetTerm(),
		LeaderId:     rf.me,
		PrevLogIndex: rf.PrevLogIndexFor(server),
		PrevLogTerm:  rf.PrevLogTermFor(server),
		Logs:         rf.logs[rf.PrevLogIndexFor(server)+1:],
		LeaderCommit: rf.GetCommitIndex(),
	}
}

func (rf *Raft) sendHeartBeatsAll__(replyCh chan *AppendEntriesReply, wg *sync.WaitGroup) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go rf.sendAETo(i, replyCh, wg)
		DHBPrintf("[HB]Send HEART BEAT sender %d to %d %v\n", rf.me, i, time.Now())
	}
}

// wrapper
func (rf *Raft) sendAETo(
	server int,
	replyCh chan *AppendEntriesReply,
	wg *sync.WaitGroup) {
	args := rf.aeArg__(server)
	reply := &AppendEntriesReply{Term: -1, Req: args}
	//DPrintf("send heart beat to %d from %d\n", server, rf.me)
	DHBPrintf("[real]Send HEART BEAT sender %d to %d %v\n", rf.me, server, time.Now())
	ok := rf.sendAppendEntries(server, *args, reply)
	if !ok {
		DLogPrintf("send AppendEntries to %d failed sender %d\n", server, rf.me)
		reply.Error = true
	}
	replyCh <- reply
	// wg.Done()
}

// apply msg til rf's apply index reaches the commit index
func (rf *Raft) syncApplyMsgs__() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.mu.Lock()
		ind := rf.lastApplied
		if ind > len(rf.logs) {
			ind = len(rf.logs)
		}
		rf.mu.Unlock()
		if ind <= 0 {
			continue
		}
		msg := ApplyMsg{
			Index:   ind,
			Command: rf.logs[ind-1].Content,
		}
		rf.applyCh <- msg
		DPrintf("Done Apply %v ind %d [APPLYEE] %d\n", msg.Command, msg.Index, rf.me)
	}
}
