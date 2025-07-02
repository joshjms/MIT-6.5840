package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) broadcastAppendEntries() {
	appendEntriesCh := make(chan *AppendEntriesReply, len(rf.peers)-1)

	rf.sendAppendEntriesToAll(appendEntriesCh)

	successCount := 0

	for i := 0; i < len(rf.peers)-1; i++ {
		reply := <-appendEntriesCh

		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.toFollowerWithTerm(reply.Term)
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			successCount++
		}
		rf.mu.Unlock()
	}

	rf.DPrintf("Finished replicating logs")
}

func (rf *Raft) sendAppendEntriesToAll(appendEntriesCh chan *AppendEntriesReply) {
	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.log) - 1,
		PrevLogTerm:  rf.log[len(rf.log)-1].Term,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			rf.DPrintf("Sending AppendEntries RPC to %d", server)
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(server, args, reply)
			appendEntriesCh <- reply
		}(server)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// RPC handler for AppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.DPrintf("Receive AppendEntries RPC from %d", args.LeaderId)

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.toFollowerWithTerm(args.Term)
	}

	rf.DPrintf("Resetting election timer")
	rf.resetElectionTimer()

	reply.Term = rf.currentTerm
	reply.Success = true
}
