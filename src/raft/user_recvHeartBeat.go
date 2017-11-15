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
			DHBPrintf("HeartBeat Time Out msg %d\n", rf.me)
			timer.Stop()
			rf.SetUserState(None)
			rf.becomeCandidate()
			canceled = true
		case <-rf.abort:
			DHBPrintf("HB recv abort msg %d\n", rf.me)
			rf.SetUserState(None)
			canceled = true
			timer.Stop()
		case <-rf.heartBeat:
			// reset timer
			DHBPrintf("Recv HB %d %v\n", rf.me, time.Now())
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			DHBPrintf("[HB Stop] %d %v\n", rf.me, time.Now())
			timer.Reset(randomTimeOut())
		}
	}
}
