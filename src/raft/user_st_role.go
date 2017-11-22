package raft

func (rf *Raft) GetRole() RaftPeerRole {
	rf.mu.Lock()
	role := rf.role
	rf.mu.Unlock()
	return role
}

func (rf *Raft) IsLeader() bool {
	role := rf.GetRole()
	return role == Leader
}

func (rf *Raft) SetRole(r RaftPeerRole) {
	rf.mu.Lock()
	rf.role = r
	rf.mu.Unlock()
}

func (rf *Raft) IsBusy() bool {
	state := rf.GetUserState()
	return state == InElection || state == InRecvHeartBeat || state == InSendingHeartBeat
}

func (rf *Raft) GetUserState() RaftState {
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	return state
}

func (rf *Raft) SetUserState(state RaftState) {
	rf.mu.Lock()
	rf.state = state
	rf.mu.Unlock()
}

func (rf *Raft) GetCommitIndex() int {
	rf.mu.Lock()
	ci := rf.commitIndex
	rf.mu.Unlock()
	return ci
}

func (rf *Raft) SetCommitIndex(idx int) {
	rf.mu.Lock()
	rf.commitIndex = idx
	rf.mu.Unlock()
}

func (rf *Raft) GetTerm() int {
	rf.mu.Lock()
	res := rf.currentTerm
	rf.mu.Unlock()
	return res
}

func (rf *Raft) SetTerm(t int) {
	rf.mu.Lock()
	rf.currentTerm = t
	rf.mu.Unlock()
}

// follower to candidate
func (rf *Raft) becomeCandidate() {
	if rf.GetRole() != Candidate {
		DPrintf("Become Candidate %d Term %d\n", rf.me, rf.currentTerm)
		rf.SetRole(Candidate)
		rf.startElection()
	}
}

// candidate to leader
func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	if rf.role != Leader {
		DPrintf("Become Leader %d Term %d\n", rf.me, rf.currentTerm)
		rf.role = Leader
		rf.maxId = 0
		if rf.state != None {
			rf.state = None
			rf.abort <- struct{}{}
			<-rf.abort
		}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.nextIndex[i] = len(rf.logs) - 1
		}
		go rf.lessSendHeartBeats()
	}
	rf.mu.Unlock()
}

// candidate / leader to follower
func (rf *Raft) becomeFollower() {
	rf.mu.Lock()
	rf.becomeFollowerNoLock()
	rf.mu.Unlock()
}

func (rf *Raft) becomeFollowerNoLock() {
	if rf.role != Follower {
		DPrintf("Become Follower %d Term %d\n", rf.me, rf.currentTerm)
		rf.role = Follower
		rf.maxId = 0
		if rf.state != None {
			rf.state = None
			rf.abort <- struct{}{}
			<-rf.abort
		}
		go rf.startRecvHeartBeats()
	}
}
