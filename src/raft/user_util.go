package raft

import (
	"time"
	"math/rand"
	"fmt"
	"bytes"
)

// return a time that will in rand [150,300) ms
func randomTimeOut() time.Duration {
	t := time.Duration(rand.Intn(150)+150) * time.Millisecond // rand [150,300) ms to time out
	return t
}

// logic for comparing which log is newer(inclusive)
func isNewerLog(aTerm, aIdx int, bTerm, bIdx int) bool {
	if aTerm != bTerm {
		return aTerm > bTerm
	}
	return aIdx >= bIdx
}

// TODO
func (rf *Raft) PrevLogIndexFor(sever int) int {
	rf.mu.Lock()
	ind := rf.nextIndex[sever]
	if ind > len(rf.logs)-1 {
		ind = len(rf.logs) - 1
	}
	rf.mu.Unlock()
	return ind
}

func (rf *Raft) PrevLogTermFor(server int) int {
	rf.mu.Lock()
	ind := rf.nextIndex[server]
	if ind > len(rf.logs)-1 {
		ind = len(rf.logs) - 1
	}
	rf.mu.Unlock()
	if ind < 0 {
		return ind
	}
	return rf.logs[ind].Term
}

func dranRplChV(replyCh chan *RequestVoteReply, me, term int) {
	for range replyCh {
	}
	DVotePrintf("Drain reply Channel V for Request Vote for %d on Term %d\n", me, term)
}

func dranRplChA(replyCh chan *AppendEntriesReply, me, term int) {
	for range replyCh {
	}
	DLogPrintf("Drain reply Channel A for Request Vote for %d on Term %d\n", me, term)
}

func roleStr(role RaftPeerRole) string {
	switch role {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return ""
	}
}

func stateStr(state RaftState) string {
	switch state {
	case InSendingHeartBeat:
		return "SendHB"
	case InRecvHeartBeat:
		return "RecvHB"
	case InElection:
		return "Election"
	default:
		return ""
	}
}

func (rf *Raft) RoleStr() string {
	return roleStr(rf.role)
}

func (rf *Raft) StateStr() string {
	return stateStr(rf.state)
}

func (rf *Raft) DebugStr() string {
	return fmt.Sprintf("%d: term %d role %s state %s votefor:%d logs:%v commit:%d applyed:%d next:%v\n",
		rf.me, rf.currentTerm, rf.RoleStr(), rf.StateStr(), rf.votedFor, rf.logsStr(), rf.commitIndex, rf.lastApplied, rf.nextIndex)
}

func (rf *Raft) logsStr() string {
	buf := bytes.Buffer{}
	buf.WriteByte('[')
	for i, l := range rf.logs {
		if i > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteString(fmt.Sprintf("{i:%d t:%d c:%v}", i, l.Term, l.Content))
	}
	buf.WriteByte(']')
	return buf.String()
}

func (rf *Raft) CommitStr() string {
	return fmt.Sprintf("%d commitInd:%d lastApply:%d\n", rf.me, rf.commitIndex, rf.lastApplied)
}

func (args AppendEntriesArg) DebugStr() string {
	return fmt.Sprintf("Arg SenderId:%d Term:%d Logs:%v PrevIdx:%d PrevTerm:%d LeaderCIdx:%d",
		args.LeaderId, args.Term, args.Logs, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
}

func (rpl *AppendEntriesReply) DebugStr() string {
	return fmt.Sprintf("RPL Sender:%d Rcv:%d STerm:%d RTerm:%d Succ:%t Error:%t\n",
		rpl.Req.LeaderId, rpl.Me, rpl.Req.Term, rpl.Term, rpl.Success, rpl.Error)
}

// true for most peers says agreed
func (rf *Raft) MostAgreed(agree int) bool {
	return agree > len(rf.peers)/2
}

func decodeBitMap(bitmap uint) []int {
	l := 32
	if (uint(1) << 32) > 1 {
		l = 64
	}
	res := make([]int, 0, l)
	for i := 0; i < l; i++ {
		if bitmap&(1<<uint(i)) != 0 {
			res = append(res, i)
		}
	}
	return res
}

func succNum(bitmap uint, total int) int {
	res := 0
	for i := 0; i < total; i++ {
		if bitmap&(1<<uint(i)) != 0 {
			res ++
		}
	}
	return res
}
