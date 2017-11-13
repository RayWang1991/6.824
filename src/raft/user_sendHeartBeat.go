package raft

import (
	"time"
	"sync"
)

const HEARTBEAT_PERIOD = 50 * time.Millisecond

func (rf *Raft) startSendHeartBeats() {
	DHBPrintf("Start Send HB rf %d\n", rf.me)
	rf.SetUserState(InSendingHeartBeat)
	replyCh := make(chan *AppendEntriesReply)
	wg := &sync.WaitGroup{}
	ticker := time.NewTicker(HEARTBEAT_PERIOD)
	rf.sendHeartBeatsAll(replyCh, wg)

	canceled := false

	// reply disposer
	go func(rplch chan *AppendEntriesReply, term, me int) {
		hasChange := false
		for rpl := range rplch {
			if !rpl.Success && rpl.Term > term && !hasChange {
				// found higher Term
				DHBPrintf("Higher Term on reply heart beat from %d %d > %d\n", me, rpl.Term, term)
				hasChange = true
				rf.currentTerm = term
				rf.votedFor = -1
				rf.abort <- struct{}{}
			}
		}
	}(replyCh, rf.currentTerm, rf.me)

	for !canceled {
		select {
		case <-rf.abort:
			DPrintf("HB send abort!!! msg %d\n", rf.me)
			rf.SetUserState(None)
			rf.becomeFollower()
			canceled = true
		case <-ticker.C:
			rf.sendHeartBeatsAll(replyCh, wg)
		}
	}

	DHBPrintf("End Send HB rf %d\n", rf.me)
	// closer for reply channel
	go func() {
		wg.Wait()
		close(replyCh)
	}()
}

func (rf *Raft) sendHeartBeatsAll(replyCh chan *AppendEntriesReply, wg *sync.WaitGroup) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &AppendEntriesArg{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: -1,
			PrevLogTerm:  -1,
			LeaderCommit: rf.commitIndex,
		}
		wg.Add(1)
		go rf.sendHeartBeatsTo(i, args, replyCh, wg)
		//DPrintf("Send HEART BEAT %v\n", time.Now())
	}
}

func (rf *Raft) sendHeartBeatsTo(
	server int,
	args *AppendEntriesArg,
	replyCh chan *AppendEntriesReply,
	wg *sync.WaitGroup) {
	reply := &AppendEntriesReply{Term: -1}
	DHBPrintf("Send heart beat to %d from %d\n", server, rf.me)
	ok := rf.sendAppendEntries(server, *args, reply)
	if !ok {
		//DPrintf("send AppendEntries to %d failed\n", server)
	}
	replyCh <- reply
	wg.Done()
}
