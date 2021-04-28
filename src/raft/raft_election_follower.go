package raft

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// follower node response to RequestVote call.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	defer rf.persist()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[RequestVote] me=%v, be asked to voted to %v, his term=%v", rf.me, args.CandidateId, args.Term)

	reply.Term = rf.currentTerm

	// requestVote from old term Candidate, ignore.
	if args.Term < rf.currentTerm {
		DPrintf("[RequestVote] me=%v, too old term dont give vote, currentTerm=%v", rf.me, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// requestVote from higher term, update our term
	if args.Term > rf.currentTerm {
		DPrintf("[RequestVote] %v get bigger term=%v from %v", rf.me, args.Term, args.CandidateId)
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}

	// Candidate's log should at least up-to-date as Follower's log.
	// refer to 5.4.1
	if moreUpToDate(rf.getLastLogIndex(), rf.getLastLogTerm(), args.LastLogIndex, args.LastLogTerm) {
		DPrintf("[RequestVote] %v cant give vote to %v because he is too old, myEntries=%+v, his LastLogIndex=%v, LastLogTerm=%v", rf.me, args.CandidateId, rf.logs, args.LastLogIndex, args.LastLogTerm)
		reply.VoteGranted = false
	} else if rf.votedFor == args.CandidateId || rf.votedFor == -1 {
		// Follower have voted to him or havenot voted before, then give out tickect.
		DPrintf("[RequestVote] %v give vote1 to %v, myEntries=%v, his LastLogIndex=%v, LastLogTerm=%v", rf.me, args.CandidateId, rf.logs, args.LastLogIndex, args.LastLogTerm)
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.refreshElectionTimeout()
	} else {
		// Follower has voted to other Candidate.
		DPrintf("[RequestVote] %v this term has voted to %v", rf.me, rf.votedFor)
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
	return
}
