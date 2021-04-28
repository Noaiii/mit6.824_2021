package raft

// leader send AppendEntries to one follower, and try to update commitIndex
func (rf *Raft) sendHeartbeat(server int, term int, prevLogIndex int, prevLogTerm int, entries []*Entry, leaderCommitIndex int) {
	DPrintf("[sendHeartbeat] %v send heartsbeats to %v", rf.me, server)
	args := &AppendEntriesArgs{
		Term:              term,
		LeaderId:          rf.me,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
		Entries:           entries,
		LeaderCommitIndex: leaderCommitIndex,
	}
	reply := &AppendEntriesReply{}
	DPrintf("[sendHeartbeat] %v sendHeartbeat to %v, args=%+v", rf.me, server, args)
	if ok := rf.sendAppendEntries(server, args, reply); !ok {
		DPrintf("[sendHeartbeat] leader %v send to %v rpc error", rf.me, server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !reply.Success {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = Follower
			rf.persist()
			DPrintf("[sendHeartbeat] %v sendHeartbeat to %v but get a newer term, term=%v", rf.me, server, rf.currentTerm)
		} else {
			// decrease this server's nextIndex and retry later.
			rf.nextIndex[server] = reply.NextTryIndex
			DPrintf("[sendHeartbeat] %v sendHeartbeat to %v but get refused, now nextIndex[i]=%v", rf.me, server, rf.nextIndex[server])
		}
		return
	}

	DPrintf("[sendHeartbeat] %v sendHeartbeat to %v succeed, args=%+v", rf.me, server, args)
	rf.matchIndex[server] = prevLogIndex + len(entries)
	rf.nextIndex[server] = rf.matchIndex[server] + 1

	// check if we can update commitIndex to newCommitIndex
	newCommitIndex := rf.matchIndex[server]
	if newCommitIndex <= rf.commitIndex {
		// already commited before.
		return
	}

	// count how many nodes have received logs between logs[0] and logs[newCommitIndex]
	var cnt int32 = 0
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.matchIndex[i] >= newCommitIndex {
			cnt++
		}
	}
	DPrintf("[sendHeartbeat] %v try set commitIndex to %v, cnt=%v", rf.me, newCommitIndex, cnt)
	oldCommit := rf.commitIndex

	// if majority of cluster (including leader himself) has received logs of at least logs[newCommitIndex]
	if cnt+1 >= rf.getMajority() {
		rf.commitIndex = newCommitIndex
		DPrintf("[sendHeartbeat] %v leader now commitIndex=%v", rf.me, rf.commitIndex)
		for i := oldCommit + 1; i <= newCommitIndex; i++ {
			valid := false
			if i >= rf.lastApplied {
				valid = true
				rf.lastApplied = i
			}
			msg := ApplyMsg{
				CommandValid: valid,
				Command:      rf.logs[i].Command,
				CommandIndex: i,
			}
			DPrintf("[sendHeartbeat] %v apply msg=%+v", rf.me, msg)
			rf.applyLog(msg)
		}
	}
	DPrintf("[sendHeartbeat] %v sendHeartbeat to %v succeed, now nextIndex[i]=%v, matchIndex[i]=%v", rf.me, server, rf.nextIndex[server], rf.matchIndex[server])
}
