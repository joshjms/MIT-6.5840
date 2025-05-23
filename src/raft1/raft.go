package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	FOLLOWER  uint8 = 0
	CANDIDATE uint8 = 1
	LEADER    uint8 = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan raftapi.ApplyMsg

	// Persistent state
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// Volatile state on all servers
	leaderId    int
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	state        uint8
	appendCh     chan struct{}
	toFollowerCh chan struct{}
	toLeaderCh   chan struct{}
	grantVoteCh  chan struct{}

	lastPing        time.Time
	electionTimeout time.Duration
}

func (rf *Raft) resetChannels() {
	rf.appendCh = make(chan struct{})
	rf.toFollowerCh = make(chan struct{})
	rf.toLeaderCh = make(chan struct{})
	rf.grantVoteCh = make(chan struct{})
}

func (rf *Raft) toFollower(term int) {
	state := rf.state
	rf.currentTerm = term
	rf.state = FOLLOWER
	rf.votedFor = -1

	if state != FOLLOWER {
		sendToChannel(rf.toFollowerCh)
		// DPrintf("[%d] Server %d becomes follower, current term: %d\n", rf.currentTerm, rf.me, rf.currentTerm)
	} else {
		// DPrintf("[%d] Server %d is already a follower, current term: %d\n", rf.currentTerm, rf.me, rf.currentTerm)
	}
}

func (rf *Raft) toCandidate() {
	if rf.state == LEADER {
		panic("Leader cannot be a candidate")
	}

	rf.resetChannels()

	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votedFor = rf.me
}

func (rf *Raft) toLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != CANDIDATE {
		panic("Only candidates can become leaders")
	}

	rf.resetChannels()

	rf.state = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}

	// DPrintf("[%d] Server %d becomes leader\n", rf.currentTerm, rf.me)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastPing = time.Now()

	// DPrintf("[%d] Server %d received RequestVote from server %d\n", rf.currentTerm, rf.me, args.CandidateId)

	if args.Term < rf.currentTerm {
		// DPrintf("[%d] Server %d rejects RequestVote from server %d, current term: %d, candidate term: %d\n", rf.currentTerm, rf.me, args.CandidateId, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	upToDate := (args.LastLogTerm > rf.logs[len(rf.logs)-1].Term) ||
		(args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= len(rf.logs)-1)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		// DPrintf("[%d] Server %d grants vote to server %d\n", rf.currentTerm, rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		sendToChannel(rf.grantVoteCh)
	} else {
		// DPrintf("[%d] Server %d rejects vote to server %d\n", rf.currentTerm, rf.me, args.CandidateId)
		reply.VoteGranted = false
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastPing = time.Now()

	DPrintf("[%d] Server %d received AppendEntries from server %d\n", rf.currentTerm, rf.me, args.LeaderId)

	if args.Term < rf.currentTerm {
		// DPrintf("[%d] Server %d rejects AppendEntries from server %d, current term: %d, leader term: %d\n", rf.currentTerm, rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	sendToChannel(rf.appendCh)

	if args.PrevLogIndex > len(rf.logs)-1 {
		// DPrintf("[%d] Server %d rejects AppendEntries from server %d, PrevLogIndex out of range\n", rf.currentTerm, rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// DPrintf("[%d] Server %d rejects AppendEntries from server %d, PrevLogTerm mismatch\n", rf.currentTerm, rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// DPrintf("[%d] Server %d accepts AppendEntries from server %d\n", rf.currentTerm, rf.me, args.LeaderId)

	rf.logs = rf.logs[:args.PrevLogIndex+1]
	rf.logs = append(rf.logs, args.Entries...)
	// rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
	}

	DPrintf("[%d] Server logs: %v\n", rf.currentTerm, rf.logs)

	reply.Term = rf.currentTerm
	reply.Success = true
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d] Server %d Start command: %v\n", rf.currentTerm, rf.me, command)

	if rf.state != LEADER {
		// DPrintf("[%d] Server %d is not a leader, cannot start command\n", rf.currentTerm, rf.me)
		return -1, rf.currentTerm, false
	}

	DPrintf("[%d] Server %d is the leader, starting command: %v\n", rf.currentTerm, rf.me, command)

	term = rf.currentTerm

	rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
	index = len(rf.logs) - 1

	DPrintf("[%d] Server %d logs: %v\n", rf.currentTerm, rf.me, rf.logs)

	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	go rf.broadcastAppend()

	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection(fromState uint8) {
	rf.mu.Lock()

	if rf.state != fromState {
		rf.mu.Unlock()
		// DPrintf("[%d] Server %d election race condition\n", rf.currentTerm, rf.me)
		return
	}

	if rf.lastPing.Add(rf.electionTimeout).After(time.Now()) {
		rf.mu.Unlock()
		// DPrintf("[%d] Server %d election timeout not reached\n", rf.currentTerm, rf.me)
		return
	}

	rf.toCandidate()

	term := rf.currentTerm
	me := rf.me
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	rf.mu.Unlock()

	// DPrintf("[%d] Server %d starts election\n", term, me)

	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votingCh := make(chan bool, len(rf.peers)) // buffered to avoid blocking

	for i := range rf.peers {
		if i != me {
			go func(server int) {
				reply := RequestVoteReply{}

				// DPrintf("[%d] Server %d sends RequestVote to server %d\n", term, me, server)

				ok := rf.sendRequestVote(server, &args, &reply)
				if ok {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.toFollower(reply.Term)
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()

					votingCh <- reply.VoteGranted
				}
			}(i)
		}
	}

	go func() {
		votedCount := 1
		for range rf.peers {
			granted := <-votingCh
			if granted {
				votedCount++
			}

			if votedCount > len(rf.peers)/2 {
				rf.mu.Lock()
				sendToChannel(rf.toLeaderCh)
				rf.mu.Unlock()
				break
			}
		}

		// DPrintf("[%d] Server %d election finished, voted count: %d\n", term, me, len(rf.peers))
	}()
}

func (rf *Raft) broadcastAppend() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	state := rf.state
	if state != LEADER {
		// DPrintf("[%d] Server %d is not a leader, cannot send heartbeat\n", rf.currentTerm, rf.me)
		return
	}
	term := rf.currentTerm

	rf.lastPing = time.Now()

	type appendResult struct {
		server int
		args   AppendEntriesArgs
		reply  AppendEntriesReply
	}

	leaderId := rf.me

	prevLogIndexes := make([]int, len(rf.peers))
	for i := range rf.peers {
		prevLogIndexes[i] = rf.nextIndex[i] - 1
	}

	prevLogTerms := make([]int, len(rf.peers))
	for i := range rf.peers {
		prevLogTerms[i] = rf.logs[prevLogIndexes[i]].Term
	}

	logs := rf.logs[:]
	commitIndex := rf.commitIndex

	appendChan := make(chan appendResult, len(rf.peers)-1)

	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     leaderId,
					PrevLogIndex: prevLogIndexes[server],
					PrevLogTerm:  prevLogTerms[server],
					Entries:      logs[prevLogIndexes[server]+1:],
					LeaderCommit: commitIndex,
				}

				reply := AppendEntriesReply{}
				// DPrintf("[%d] Server %d sends heartbeat to server %d\n", rf.currentTerm, rf.me, i)

				ok := rf.sendAppendEntries(server, &args, &reply)
				if ok {
					appendChan <- appendResult{server: server, args: args, reply: reply}
				}
			}(i)
		}
	}

	go func() {
		for i := 0; i < len(rf.peers)-1; i++ {
			appendResult := <-appendChan
			rf.mu.Lock()

			server := appendResult.server
			args := appendResult.args
			reply := appendResult.reply

			if reply.Term > rf.currentTerm {
				rf.toFollower(reply.Term)
			}

			if reply.Success {
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			} else {
				rf.nextIndex[server]--
			}

			for j := rf.commitIndex + 1; j < len(rf.logs); j++ {
				cnt := 1 // count the leader itself
				for k := range rf.peers {
					if k != rf.me && rf.matchIndex[k] >= j {
						cnt++
					}
				}
				if cnt > len(rf.peers)/2 && rf.logs[j].Term == rf.currentTerm {
					rf.commitIndex = j
				}
			}

			rf.mu.Unlock()
		}
		// DPrintf("[%d] Server %d heartbeat finished\n", term, rf.me)
	}()
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i,
			}

			rf.applyCh <- msg

			rf.lastApplied = i
		}

		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.electionTimeout = time.Duration(500+rand.Intn(500)) * time.Millisecond
		heartbeatInterval := time.Duration(50) * time.Millisecond

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch {
		case state == LEADER:
			select {
			case <-rf.toFollowerCh:
				// DPrintf("[%d] Server %d from leader to follower\n", rf.currentTerm, rf.me)
			case <-time.After(heartbeatInterval):
				rf.broadcastAppend()
			}
		case state == CANDIDATE:
			select {
			case <-rf.toFollowerCh:
				// DPrintf("[%d] Server %d from candidate to follower", rf.currentTerm, rf.me)
			case <-rf.toLeaderCh:
				rf.toLeader()
				rf.broadcastAppend()
			case <-time.After(rf.electionTimeout):
				rf.startElection(CANDIDATE)
			}

		case state == FOLLOWER:
			select {
			case <-rf.grantVoteCh:
			case <-rf.appendCh:
			case <-time.After(rf.electionTimeout):
				rf.startElection(FOLLOWER)
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.mu.Lock()

	rf.peers = peers
	rf.persister = persister
	rf.applyCh = applyCh
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []LogEntry{{Term: 0}}

	rf.toFollower(0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.mu.Unlock()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}

func sendToChannel(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
