package raft

import (
	"time"
	"math/rand"
)

// return a time that will in rand [150,300) ms
func randomTimeOut() time.Duration {
	t := time.Duration(rand.Intn(150)+150) * time.Millisecond // rand [150,300) ms to time out
	return t
}

// logic for comparing which log is newer(inclusive)
func isNewerLog(aIdx, aTerm int, bIdx, bTerm int) bool {
	if aTerm != bTerm {
		return aTerm >= bTerm
	}
	return aIdx >= bIdx
}


// TODO
func (rf *Raft) PrevLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) PrevLogTerm() int {
	if len(rf.logs) == 0 {
		return -1
	}
	return rf.logs[len(rf.logs)-1][TermKey]
}