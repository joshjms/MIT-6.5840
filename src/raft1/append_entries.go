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

	XTerm  int
	XIndex int
	XLen   int
}

type appendEntriesResult struct {
	reply *AppendEntriesReply

	server       int
	prevLogIndex int
	entriesLen   int

	ok bool
}

func (rf *Raft) broadcastAppendEntries() {
	appendEntriesCh := make(chan appendEntriesResult, len(rf.peers)-1)

	ok := rf.sendAppendEntriesToAll(appendEntriesCh)
	if !ok {
		return
	}

	for i := 0; i < len(rf.peers)-1; i++ {
		res := <-appendEntriesCh
		if !res.ok {
			continue
		}

		reply := res.reply

		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.toFollowerWithTerm(reply.Term)
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			// If successful, update nextIndex and matchIndex for follower
			rf.nextIndex[res.server] = res.prevLogIndex + res.entriesLen + 1
			rf.matchIndex[res.server] = res.prevLogIndex + res.entriesLen
		} else {
			// If desired, the protocol can be optimized to reduce the
			// number of rejected AppendEntries RPCs. For example,
			// when rejecting an AppendEntries request, the follower
			// can include the term of the conflicting entry and the first
			// index it stores for that term. With this information, the
			// leader can decrement nextIndex to bypass all of the con-
			// flicting entries in that term; one AppendEntries RPC will
			// be required for each term with conflicting entries, rather
			// than one RPC per entry. In practice, we doubt this opti-
			// mization is necessary, since failures happen infrequently
			// and it is unlikely that there will be many inconsistent en-
			// tries.

			// Case 1: leader doesn't have XTerm:
			//     nextIndex = XIndex
			// Case 2: leader has XTerm:
			//     nextIndex = (index of leader's last entry for XTerm) + 1
			// Case 3: follower's log is too short:
			//     nextIndex = XLen

			if reply.XTerm >= 0 {
				idx := -1
				for i := len(rf.log) - 1; i >= 1; i-- {
					if rf.log[i].Term == reply.XTerm {
						idx = i
						break
					}
				}
				if idx != -1 {
					// Case 2
					rf.nextIndex[res.server] = idx + 1
				} else {
					// Case 1
					rf.nextIndex[res.server] = reply.XIndex
				}
			} else {
				// Case 3
				rf.nextIndex[res.server] = reply.XLen
			}
		}

		rf.DPrintf("%v-%v", rf.commitIndex+1, len(rf.log)-1)

		for nextCommitIndex := rf.commitIndex + 1; nextCommitIndex < len(rf.log); nextCommitIndex++ {
			cnt := 1
			for follower := range rf.peers {
				if follower != rf.me && rf.matchIndex[follower] >= nextCommitIndex {
					cnt++
				}
			}
			if cnt > len(rf.peers)/2 && rf.log[nextCommitIndex].Term == rf.currentTerm {
				rf.commitIndex = nextCommitIndex
			}
		}

		rf.DPrintf("Leader commit: %d", rf.commitIndex)

		rf.mu.Unlock()
	}

	rf.DPrintf("Finished replicating logs")
}

func (rf *Raft) sendAppendEntriesToAll(appendEntriesCh chan appendEntriesResult) bool {
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return false
	}

	rf.DPrintf("Current log: %v", rf.log)

	args := make([]*AppendEntriesArgs, len(rf.peers))

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		var entries []Entry

		if len(rf.log)-1 >= rf.nextIndex[server] {
			raw := rf.log[rf.nextIndex[server]:]
			entries = make([]Entry, len(raw))
			copy(entries, raw)
		}

		args[server] = &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
	}
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int, args *AppendEntriesArgs) {
			rf.DPrintf("Sending AppendEntries RPC to %d: %v", server, args)
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, reply)
			result := appendEntriesResult{
				reply:        reply,
				server:       server,
				prevLogIndex: args.PrevLogIndex,
				entriesLen:   len(args.Entries),
				ok:           ok,
			}

			appendEntriesCh <- result
		}(server, args[server])
	}

	return true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// RPC handler for AppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.DPrintf("Receive AppendEntries RPC from %d: %v, term: %d", args.LeaderId, args.Entries, args.Term)
	rf.DPrintf("Current log: %v, commit: %d", rf.log, rf.commitIndex)

	if args.Term > rf.currentTerm {
		rf.toFollowerWithTerm(args.Term)
	}

	// 1. Reply false if term < currentTerm.

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.DPrintf("Resetting election timer")
	rf.resetElectionTimer()

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm.

	if len(rf.log)-1 < args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = len(rf.log)

		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for i > 1 && rf.log[i-1].Term == rf.log[args.PrevLogIndex].Term {
			i--
		}
		reply.XIndex = i

		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.

	var entriesIndex int
	for entriesIndex = 0; entriesIndex < len(args.Entries); entriesIndex++ {
		logIndex := args.PrevLogIndex + 1 + entriesIndex

		if logIndex >= len(rf.log) {
			break
		}

		if args.Entries[entriesIndex].Term != rf.log[logIndex].Term {
			rf.log = rf.log[:logIndex]
			break
		}
	}

	// 4. Append any new entries not already in the log

	rf.log = append(rf.log, args.Entries[entriesIndex:]...)
	rf.persist()

	rf.DPrintf("New logs: %v", rf.log)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	rf.DPrintf("Previous Commit: %d", rf.commitIndex)

	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := min(args.LeaderCommit, len(rf.log)-1)

		if rf.log[newCommitIndex].Term == args.Term {
			rf.commitIndex = newCommitIndex
			rf.DPrintf("Commit: %d", rf.commitIndex)
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	rf.DPrintf("Final log: %v, commit: %d", rf.log, rf.commitIndex)
}
