package raft

import "time"

// for follower to start receiving (want) heart beats
func (rf *Raft) startRecvHeartBeats() {
	rf.SetUserState(InRecvHeartBeat)
	canceled := false

	timer := time.NewTimer(randomTimeOut())

	for !canceled {
		select {
		case <-timer.C:
			//DPrintf("HeartBeat Time Out msg %d %v\n", rf.me, time.Now())
			rf.SetUserState(None)
			rf.becomeCandidate()
			canceled = true
			timer.Stop()
		case <-rf.abort:
			//DPrintf("HB recv abort msg %d\n", rf.me)
			rf.SetUserState(None)
			canceled = true
			timer.Stop()
		case <-rf.heartBeat:
			// reset timer
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(randomTimeOut())
		}
	}
}
