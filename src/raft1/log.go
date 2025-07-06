package raft

import "6.5840/raftapi"

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

	XTerm           int
	XIndex          int
	XLen            int
	RequireSnapshot bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		lastLogIndex := rf.absIndex(len(rf.log) - 1)
		nextIndexSlice := rf.sliceIndex(rf.nextIndex[server])

		if rf.nextIndex[server] <= rf.lastSnapshotIndex {
			go rf.handleInstallSnapshot(
				server,
				&InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastSnapshotIndex,
					LastIncludedTerm:  rf.log[0].Term,
					Offset:            0,
					Data:              rf.snapshot,
					Done:              true,
				},
				&InstallSnapshotReply{},
			)

			continue
		}

		var entries []Entry

		if lastLogIndex >= rf.nextIndex[server] {
			raw := rf.log[nextIndexSlice:]
			entries = make([]Entry, len(raw))
			copy(entries, raw)
		}

		prevLogIndexSlice := rf.sliceIndex(rf.nextIndex[server] - 1)
		var prevLogTerm int

		if prevLogIndexSlice < 0 {
			prevLogTerm = rf.log[0].Term
		} else {
			prevLogTerm = rf.log[prevLogIndexSlice].Term
		}

		go rf.handleAppendEntries(
			server,
			&AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[server] - 1,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			},
			&AppendEntriesReply{},
		)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) handleAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if ok := rf.sendAppendEntries(server, args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.toFollowerWithTerm(reply.Term)
		return
	}

	if reply.Success {
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	} else {
		if reply.RequireSnapshot {
			go rf.handleInstallSnapshot(
				server,
				&InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastSnapshotIndex,
					LastIncludedTerm:  rf.log[0].Term,
					Offset:            0,
					Data:              rf.snapshot,
					Done:              true,
				},
				&InstallSnapshotReply{},
			)
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
				for i := len(rf.log) - 1; i >= 0; i-- {
					if rf.log[i].Term == reply.XTerm {
						idx = i
						break
					}
				}
				if idx != -1 {
					// Case 2
					rf.nextIndex[server] = rf.absIndex(idx + 1)
				} else {
					// Case 1
					rf.nextIndex[server] = reply.XIndex
				}
			} else {
				// Case 3
				rf.nextIndex[server] = reply.XLen
			}
		}
	}

	lastLogIndex := rf.absIndex(len(rf.log) - 1)
	for nextCommitIndex := rf.commitIndex + 1; nextCommitIndex <= lastLogIndex; nextCommitIndex++ {
		cnt := 1
		for follower := range rf.peers {
			if follower != rf.me && rf.matchIndex[follower] >= nextCommitIndex {
				cnt++
			}
		}

		nextCommitIndexSlice := rf.sliceIndex(nextCommitIndex)
		if cnt > len(rf.peers)/2 && rf.log[nextCommitIndexSlice].Term == rf.currentTerm {
			rf.commitIndex = nextCommitIndex
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) handleInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if ok := rf.sendInstallSnapshot(server, args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.toFollowerWithTerm(reply.Term)
		return
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex

	lastLogIndex := rf.absIndex(len(rf.log) - 1)
	for nextCommitIndex := rf.commitIndex + 1; nextCommitIndex <= lastLogIndex; nextCommitIndex++ {
		cnt := 1
		for follower := range rf.peers {
			if follower != rf.me && rf.matchIndex[follower] >= nextCommitIndex {
				cnt++
			}
		}

		nextCommitIndexSlice := rf.sliceIndex(nextCommitIndex)
		if cnt > len(rf.peers)/2 && rf.log[nextCommitIndexSlice].Term == rf.currentTerm {
			rf.commitIndex = nextCommitIndex
		}
	}
}

// RPC handler for AppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm.

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.toFollowerWithTerm(args.Term)
	}

	rf.resetElectionTimer()

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm.
	lastLogIndex := rf.absIndex(len(rf.log) - 1)

	if lastLogIndex < args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = lastLogIndex + 1

		return
	}

	prevLogIndexSlice := rf.sliceIndex(args.PrevLogIndex)

	if args.PrevLogIndex < rf.lastSnapshotIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.RequireSnapshot = true

		return
	}

	if rf.log[prevLogIndexSlice].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XTerm = rf.log[prevLogIndexSlice].Term
		i := prevLogIndexSlice
		for i > 1 && rf.log[i-1].Term == rf.log[prevLogIndexSlice].Term {
			i--
		}
		reply.XIndex = rf.absIndex(i)

		return
	}

	var entriesIndex int
	for entriesIndex = 0; entriesIndex < len(args.Entries); entriesIndex++ {
		logIndex := args.PrevLogIndex + 1 + entriesIndex
		lastLogIndex := rf.absIndex(len(rf.log))

		if logIndex >= lastLogIndex {
			break
		}

		logIndexSlice := rf.sliceIndex(logIndex)

		if args.Entries[entriesIndex].Term != rf.log[logIndexSlice].Term {
			rf.log = rf.log[:logIndexSlice]
			break
		}
	}

	// 4. Append any new entries not already in the log

	rf.log = append(rf.log, args.Entries[entriesIndex:]...)
	rf.persist()

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	if args.LeaderCommit > rf.commitIndex {
		lastLogIndex := rf.absIndex(len(rf.log) - 1)
		newCommitIndex := min(args.LeaderCommit, lastLogIndex)
		rf.commitIndex = newCommitIndex
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.toFollowerWithTerm(args.Term)
	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.lastSnapshotIndex {
		reply.Term = rf.currentTerm
		return
	}

	// 3) Find any matching entry in our old log
	oldBase := rf.lastSnapshotIndex
	keepFrom := -1
	for i := range rf.log {
		if oldBase+i == args.LastIncludedIndex && rf.log[i].Term == args.LastIncludedTerm {
			keepFrom = i + 1
			break
		}
	}

	rf.snapshot = args.Data

	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	if keepFrom >= 0 && keepFrom < len(rf.log) {
		rf.log = append([]Entry{{Term: args.LastIncludedTerm}}, rf.log[keepFrom:]...)
	} else {
		rf.log = []Entry{{Term: args.LastIncludedTerm}}
	}

	rf.persist()

	rf.applyCh <- raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	reply.Term = rf.currentTerm
}
