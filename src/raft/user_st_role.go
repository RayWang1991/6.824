package raft

func (rf *Raft) GetRole() RaftPeerRole {
	rf.muRole.Lock()
	role := rf.role
	rf.muRole.Unlock()
	return role
}

func (rf *Raft) IsLeader() bool {
	role := rf.GetRole()
	return role == Leader
}

func (rf *Raft) SetRole(r RaftPeerRole) {
	rf.muRole.Lock()
	rf.role = r
	rf.muRole.Unlock()
}

func (rf *Raft) IsBussy() bool {
	state := rf.GetUserState()
	return state == InElection || state == InRecvHeartBeat || state == InSendingHeartBeat
}

func (rf *Raft) GetUserState() RaftState {
	rf.muSt.Lock()
	state := rf.state
	rf.muSt.Unlock()
	return state
}

func (rf *Raft) SetUserState(state RaftState) {
	rf.muSt.Lock()
	rf.state = state
	rf.muSt.Unlock()
}

// follower to candidate
func (rf *Raft) becomeCandidate() {
	DPrintf("Become Candidate %d term %d\n", rf.me, rf.currentTerm)
	rf.SetRole(Candidate)
	go rf.startElection()
}

// candidate to leader
func (rf *Raft) becomeLeader() {
	DPrintf("Become Leader %d term %d\n", rf.me, rf.currentTerm)
	rf.SetRole(Leader)
	go rf.startSendHeartBeats()
}

// candidate / leader to follower
func (rf *Raft) becomeFollower() {
	DPrintf("Become Follower %d term %d\n", rf.me, rf.currentTerm)
	rf.SetRole(Follower)
	go rf.startRecvHeartBeats()
}

// reset before change role from candidate to others
func (rf *Raft) resetCandidate() {
	DPrintf("Reset Candidate\n")
}

// reset before change role from follower to others
func (rf *Raft) resetFollower() {
}

// reset before change role from Leader to others
func (rf *Raft) resetLeader() {
}
