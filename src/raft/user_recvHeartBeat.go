package raft

import "time"

// for follower to start receiving (want) heart beats
func (rf *Raft) startRecvHeartBeats() {
	DHBPrintf("[{~START RECV HB~]} %d\n", rf.me)
	rf.SetUserState(InRecvHeartBeat)
	canceled := false
	toBeCandidate := false

	timer := time.NewTimer(randomTimeOut())

	for !canceled {
		select {
		case <-timer.C:
			timer.Stop()
			DHBPrintf("HeartBeat Time Out msg %d\n", rf.me)
			toBeCandidate = true
			canceled = true
		case <-rf.abort:
			timer.Stop()
			DHBPrintf("HB recv abort msg %d\n", rf.me)
			canceled = true
		case <-rf.heartBeat:
			// reset timer
			DHBPrintf("[Recv HB] %d %v\n", rf.me, time.Now())
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
	DHBPrintf("[{~END RECV HB~]} %d\n", rf.me)
	rf.SetUserState(None)
	if toBeCandidate {
		rf.becomeCandidate()
	}
}
