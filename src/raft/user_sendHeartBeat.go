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
	cterm := rf.GetTerm()
	nomarlCh := make(chan *AppendEntriesReply)
	wg := &sync.WaitGroup{}
	timer := time.NewTimer(HEARTBEAT_PERIOD)
	rf.sendHeartBeatsAll__(nomarlCh, wg, cterm)

	// normal heart beat disposer
	// should be aware that total num > 1 !!!
	total := len(rf.peers)
	go func(rplch chan *AppendEntriesReply, term, me int) {
		var maxAppendLogIndex = 0
		var doneAL uint = 1 << uint(me) // bitmap for appendLogIndex
		for rpl := range rplch {
			//debug
			//DPrintf("[RE] %sn", rpl.DebugStr())
			if rpl.Error {
				// drop the error rpl
				DLogPrintf("Rpl [Error] sender %d to %d \n", rf.me, rpl.Me)
				continue
			}
			if rpl.Success {
				//DPrintf("[MAX] indx %d\n",maxAppendLogIndex)
				appendIndex := rpl.Req.PrevLogIndex + len(rpl.Req.Logs) + 1
				if appendIndex < maxAppendLogIndex {
					//leave it, expired reply
				} else if appendIndex > maxAppendLogIndex {
					maxAppendLogIndex = appendIndex // update max index
					doneAL = 1 << uint(me)
				} else {
					doneAL |= 1 << uint(rpl.Me)
					succAL := succNum(doneAL, total)
					if rf.MostAgreed(succAL) {
						DLogPrintf("[Done] maxAppendLogIndex %d doneAL %v\n", maxAppendLogIndex, decodeBitMap(doneAL))
						rf.mu.Lock()
						rf.commitIndex = maxAppendLogIndex
						//DPrintf("[CI] %d\n",rf.commitIndex)
						rf.matchIndex[rpl.Me] = len(rf.logs) - 1
						rf.syncApplyMsgs()
						rf.mu.Unlock()
					}
				}
			} else if rpl.Term > term {
				// found higher Term
				DHBPrintf("Higher Term on reply heart beat from %d %d > %d\n", me, rpl.Term, term)
				rf.currentTerm = rpl.Term
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
			DHBPrintf("HB send abort!!! msg %d\n", rf.me)
			break loop
		case <-timer.C:
			timer.Reset(HEARTBEAT_PERIOD) // timer must be triggered
			rf.sendHeartBeatsAll__(nomarlCh, wg, cterm)
		case <-rf.aeResCh:
			if ok := timer.Stop(); !ok { // timer must not be triggered
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(HEARTBEAT_PERIOD)
			// Do sync logs to others, in another goroutine
			rf.sendHeartBeatsAll__(nomarlCh, wg, cterm)
		}
	}

	DHBPrintf("End Send HB rf %d\n", rf.me)
	// closer for reply channel
	go dranRplChA(nomarlCh, rf.me, rf.currentTerm)
	go func() {
		wg.Wait()
		close(nomarlCh)
	}()
	rf.SetUserState(None)
	rf.becomeFollower()
}

// developing
func (rf *Raft) aeArg__(server, rfterm int) *AppendEntriesArg {
	rf.mu.Lock()
	ind := rf.nextIndex[server]
	term := -1
	if ind > len(rf.logs)-1 {
		ind = len(rf.logs) - 1
	}
	if ind >= 0 {
		term = rf.logs[ind].Term
	}
	arg := &AppendEntriesArg{
		Term:         rfterm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: ind,
		PrevLogTerm:  term,
		Logs:         rf.logs[ind+1:],
	}
	rf.mu.Unlock()
	return arg
}

func (rf *Raft) sendHeartBeatsAll__(replyCh chan *AppendEntriesReply, wg *sync.WaitGroup, cterm int) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if wg != nil {
			wg.Add(1)
		}
		go rf.sendAETo(i, cterm, replyCh, wg)
		DLogPrintf("[HB]Send HEART BEAT sender %d to %d %v\n", rf.me, i, time.Now())
	}
}

// wrapper
func (rf *Raft) sendAETo(
	server, cterm int,
	replyCh chan *AppendEntriesReply,
	wg *sync.WaitGroup) {
	args := rf.aeArg__(server, cterm)
	reply := &AppendEntriesReply{Term: -1, Req: args}
	//DPrintf("send heart beat to %d from %d\n", server, rf.me)
	DHBPrintf("[real]Send HEART BEAT sender %d to %d %v\n", rf.me, server, time.Now())
	ok := rf.sendAppendEntries(server, *args, reply)
	if !ok {
		DLogPrintf("send AppendEntries to %d failed sender %d\n", server, rf.me)
		reply.Error = true
	}
	replyCh <- reply
	if wg != nil {
		wg.Done()
	}
}

// apply msg til rf's apply index reaches the commit index
func (rf *Raft) syncApplyMsgs() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		ind := rf.lastApplied
		if ind > len(rf.logs) {
			break
		}
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
