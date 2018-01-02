package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "sync"

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
	kvmap     []byte
	notFin    []byte
	done      []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	np.kvmap = ps.kvmap
	np.notFin = ps.notFin
	np.done = ps.done
	return np
}

func (ps *Persister) SaveRaftState(data []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = data
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.raftstate
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

func (ps *Persister) SaveSnapshot(snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.snapshot = snapshot
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}

func (ps *Persister) SaveNotFin(notFin []byte) {
	ps.mu.Lock()
	ps.notFin = notFin
	ps.mu.Unlock()
}

func (ps *Persister) SaveDone(data []byte) {
	ps.mu.Lock()
	ps.done = data
	ps.mu.Unlock()
}

func (ps *Persister) SaveKVMap(data []byte) {
	ps.mu.Lock()
	ps.kvmap = data
	ps.mu.Unlock()
}

func (ps *Persister) ReadNotFin() []byte {
	ps.mu.Lock()
	res := ps.notFin
	ps.mu.Unlock()
	return res
}

func (ps *Persister) ReadDone() []byte {
	ps.mu.Lock()
	res := ps.done
	ps.mu.Unlock()
	return res
}

func (ps *Persister) ReadKVMap() []byte {
	ps.mu.Lock()
	res := ps.kvmap
	ps.mu.Unlock()
	return res
}
