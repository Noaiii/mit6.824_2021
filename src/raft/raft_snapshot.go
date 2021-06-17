package raft

import (
	"bytes"

	"6.824/labgob"
)

type InstallSnapshotArgs struct {
	LastIncludedTerm  int
	LastIncludedIndex int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

// InstallSnapshot load snapshot data
// 接收者实现：
// 1. 如果term < currentTerm就立即回复
// 2. 如果是第一个分块（offset 为 0）就创建一个新的快照  省略
// 3. 在指定偏移量写入数据  省略
// 4. 如果 done 是 false，则继续等待更多的数据 省略
// 5. 保存快照文件，丢弃具有较小索引的任何现有或部分快照
// 6. 如果现存的日志条目与快照中最后包含的日志条目具有相同的索引值和任期号，则保留其后的日志条目并进行回复
// 7. 丢弃整个日志
// 8. 使用快照重置状态机（并加载快照的集群配置）
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("[[snapshot:InstallSnapshot] raft %d receive req: %v]", rf.me, args)
	if args.LastIncludedTerm < rf.currentTerm {
		reply.Term = args.LastIncludedTerm
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  rf.currentTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCh <- msg
	// rf.persister.SaveSnapshot(args.Snapshot)
	// rf.dropLogs(args.LastIncludedIndex)
	// rf.LoadSnapshot()
	return
}

func (rf *Raft) sendAInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	DPrintf("[snapshot:CondInstallSnapshot] raft %d  term: %d, index %d", rf.me, lastIncludedTerm, lastIncludedIndex)
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex < rf.commitIndex || lastIncludedTerm < rf.currentTerm {
		DPrintf("[[snapshot:CondInstallSnapshot] raft %d refuse]", rf.me)
		return false
	}
	DPrintf("[[snapshot:CondInstallSnapshot] raft %d  persist, trim logs]", rf.me)
	state := rf.getStateBytes()
	snapshotb := rf.genSnapshot(lastIncludedIndex, snapshot)
	rf.persister.SaveStateAndSnapshot(state, snapshotb)
	rf.dropLogs(lastIncludedIndex)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	DPrintf("[snapshot:Snapshot] raft %d create snapshot up to %d data=%v", rf.me, index, snapshot)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	state := rf.getStateBytes()
	snapshotb := rf.genSnapshot(index, snapshot)
	rf.persister.SaveStateAndSnapshot(state, snapshotb)
	rf.dropLogs(index)
	// msg := ApplyMsg{
	// 	SnapshotValid: true,
	// 	Snapshot:      snapshot,
	// 	SnapshotTerm:  rf.currentTerm,
	// 	SnapshotIndex: index,
	// }
	// rf.applyCh <- msg
}

// func (rf *Raft) Snapshot(index int, snapshot []byte) {
// 	// Your code here (2D).
// 	w := new(bytes.Buffer)
// 	e := labgob.NewEncoder(w)
// 	// rf.mu.Lock()
// 	e.Encode(index)
// 	e.Encode(rf.currentTerm)
// 	e.Encode(rf.logs[:index])
// 	// rf.mu.Unlock()
// 	data := w.Bytes()
// 	DPrintf("[persist] raft %d create snapshot up to %d data=%v", rf.me, index, data)
// 	rf.persister.SaveSnapshot(data)
// }
func (rf *Raft) genSnapshot(index int, snapshot []byte) []byte {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// rf.mu.Lock()
	e.Encode(index)
	e.Encode(rf.currentTerm)
	w.Write(snapshot)
	return w.Bytes()
}

func (rf *Raft) LoadSnapshot() {
	// Your code here (2D).
	snapshot := rf.persister.ReadSnapshot()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	var index int
	if d.Decode(&index) != nil {
		DPrintf("[LoadSnapshot] error, index=%v", index)
		return
	}
	rf.commitIndex = index
	var currentTerm int
	if d.Decode(&currentTerm) != nil {
		DPrintf("[LoadSnapshot] error, currentTerm=%v", currentTerm)
		return
	}
	rf.currentTerm = currentTerm
	var logs []*Entry
	if d.Decode(&logs) != nil {
		DPrintf("[LoadSnapshot] error, logs=%v", logs)
		return
	}
	rf.logs = logs
}

// dropLogs 丢弃掉包括index之前的所有log
func (rf *Raft) dropLogs(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logs = rf.logs[index:]
}
