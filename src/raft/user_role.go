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

// follower to candidate
func (rf *Raft) becomeCandidate() {
	rf.currentTerm++
	DPrintf("Become Candidate %d term %d\n", rf.me, rf.currentTerm)
	rf.SetRole(Candidate)
	rf.startElection()
}

// candidate to leader
func (rf *Raft) becomeLeader() {
	DPrintf("Become Leader %d term %d\n", rf.me, rf.currentTerm)
	rf.SetRole(Leader)

	rf.startHeartBeatsPer() // TODO, in another goroutine ?
}

// candidate / leader to follower
func (rf *Raft) becomeFollower() {
	DPrintf("Become Follower %d term %d\n", rf.me, rf.currentTerm)
	rf.SetRole(Follower)
	rf.closeTicker()
	rf.resetHeartBeatTimeOut()
}

// reset before change role from candidate to others
func (rf *Raft) resetCandidate() {
}

// reset before change role from follower to others
func (rf *Raft) resetFollower() {
}

// reset before change role from Leader to others
func (rf *Raft) resetLeader() {
}
