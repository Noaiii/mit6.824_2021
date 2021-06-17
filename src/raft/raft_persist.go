package raft

import (
	"bytes"

	"6.824/labgob"
)

func (rf *Raft) getStateBytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// rf.mu.Lock()
	e.Encode(rf.me)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	// rf.mu.Unlock()
	data := w.Bytes()
	return data
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	data := rf.getStateBytes()
	DPrintf("[persist] raft %d persist data=%v", rf.me, data)
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	var me int
	if d.Decode(&me) != nil {
		DPrintf("[readPersist] error, me=%v", me)
		return
	} else {
		rf.me = me
	}
	var currentTerm int
	if d.Decode(&currentTerm) != nil {
		DPrintf("[readPersist] error, currentTerm=%v", currentTerm)
		return
	} else {
		rf.currentTerm = currentTerm
	}
	var votedFor int
	if d.Decode(&votedFor) != nil {
		DPrintf("[readPersist] error, votedFor=%v", votedFor)
		return
	} else {
		rf.votedFor = votedFor
	}
	var logs []*Entry
	if d.Decode(&logs) != nil {
		DPrintf("[readPersist] error, logs=%v", logs)
		return
	} else {
		rf.logs = logs
	}
	DPrintf("[persist]raft: %d read from persister success data=%v", rf.me, data)
}
