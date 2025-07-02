package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) startElection(state State, term int) {
	rf.mu.Lock()

	rf.DPrintf("Starting election with term: %d, state: %d", term, state)

	if rf.state != state || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	rf.state = Candidate

	// 1. Increment current term.
	rf.currentTerm++

	// 2. Vote for self.
	rf.votedFor = rf.me

	// 3. Reset election timer.
	rf.resetElectionTimer()

	rf.mu.Unlock()

	requestVoteCh := make(chan *RequestVoteReply, len(rf.peers)-1)

	// 4. Send RequestVote RPCs to all other servers.
	rf.sendRequestVoteAll(requestVoteCh)

	voteCount := rf.waitRequestVoteReplies(requestVoteCh)
	rf.DPrintf("vote count: %d", voteCount)

	if voteCount >= (len(rf.peers)+1)/2 {
		rf.DPrintf("Become leader")

		rf.toLeader()
		go rf.broadcastAppendEntries()
	} else {
		// Lose the election
		rf.mu.Lock()
		rf.votedFor = -1
		rf.resetElectionTimer()
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendRequestVoteAll(requestVoteCh chan *RequestVoteReply) {
	rf.mu.Lock()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			rf.DPrintf("Sending RequestVote RPC to %d", server)
			reply := &RequestVoteReply{}
			rf.sendRequestVote(server, args, reply)

			requestVoteCh <- reply
		}(server)
	}
}

func (rf *Raft) waitRequestVoteReplies(requestVoteCh chan *RequestVoteReply) int {
	voteCount := 1

	for i := 0; i < len(rf.peers)-1; i++ {
		reply := <-requestVoteCh

		rf.mu.Lock()

		if reply.VoteGranted {
			voteCount++
		} else if reply.Term > rf.currentTerm {
			rf.toFollowerWithTerm(reply.Term)
			rf.resetElectionTimer()
			rf.mu.Unlock()
			break
		}

		rf.mu.Unlock()

		if voteCount >= (len(rf.peers)+1)/2 {
			break
		}
	}

	return voteCount
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.DPrintf("Receive RequestVote RPC from %d", args.CandidateId)

	// 1. Reply false if term < currentTerm.
	if args.Term < rf.currentTerm {
		rf.DPrintf("Reject %d: term < currentTerm", args.CandidateId)

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.toFollowerWithTerm(args.Term)
	}

	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	// 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote.
	upToDate := (args.LastLogTerm > rf.log[len(rf.log)-1].Term) || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimer()
		rf.DPrintf("Vote for %d", args.CandidateId)

		return
	}

	rf.DPrintf("Reject %d, upToDate: %v", args.CandidateId, upToDate)

	reply.VoteGranted = false
}
