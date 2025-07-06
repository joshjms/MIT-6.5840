package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
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

	currentTerm int
	votedFor    int
	log         []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state State

	resetElectionTimerCh chan struct{}

	applyCh chan raftapi.ApplyMsg

	lastSnapshotIndex int
	snapshot          []byte
}

type Entry struct {
	Term    int
	Command any
}

func (rf *Raft) resetElectionTimer() {
	select {
	case rf.resetElectionTimerCh <- struct{}{}:
	default:
	}
}

func (rf *Raft) toFollowerWithTerm(term int) {
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = term
	rf.persist()
}

func (rf *Raft) toLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate {
		return
	}

	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.absIndex(len(rf.log))
		rf.matchIndex[i] = 0
	}
}

// sliceIndex takes an absolute index and converts it to the index in the server's log
func (rf *Raft) sliceIndex(i int) int {
	return i - rf.lastSnapshotIndex
}

// absIndex takes an index relative to the start of the log and converts it to the actual index of all
func (rf *Raft) absIndex(i int) int {
	return i + rf.lastSnapshotIndex
}

func (rf *Raft) printLog() {
	if Debug {
		for i := range rf.log {
			fmt.Printf("%d ", rf.absIndex(i))
		}

		fmt.Println()
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, (rf.state == Leader)
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastSnapshotIndex)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var votedFor int
	var logEntries []Entry
	var lastSnapshotIndex int

	if err := d.Decode(&term); err != nil {
		log.Fatalf("error decoding term: %v", err)
	}

	if err := d.Decode(&votedFor); err != nil {
		log.Fatalf("error decoding votedFor: %v", err)
	}

	if err := d.Decode(&logEntries); err != nil {
		log.Fatalf("error decoding log: %v", err)
	}

	if err := d.Decode(&lastSnapshotIndex); err != nil {
		log.Fatalf("error decoding lastSnapshotIndex: %v", err)
	}

	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.log = logEntries
	rf.lastSnapshotIndex = lastSnapshotIndex
	rf.commitIndex = lastSnapshotIndex
	rf.lastApplied = lastSnapshotIndex

	rf.snapshot = rf.persister.ReadSnapshot()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastSnapshotIndex >= index || index > rf.lastApplied {
		return
	}

	indexSlice := rf.sliceIndex(index)

	rf.log = append([]Entry{{Term: rf.log[indexSlice].Term}}, rf.log[indexSlice+1:]...)
	rf.lastSnapshotIndex = index

	rf.snapshot = snapshot

	rf.persist()
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == Leader

	if !isLeader {
		return -1, rf.currentTerm, false
	}

	rf.log = append(rf.log, Entry{Term: rf.currentTerm, Command: command})
	rf.persist()
	index := len(rf.log) - 1

	rf.matchIndex[rf.me] = rf.absIndex(index)
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1

	go rf.broadcastAppendEntries()

	return rf.absIndex(index), rf.currentTerm, isLeader
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		electionTimeout := time.Duration(300+(rand.Int63()%300)) * time.Millisecond
		heartbeatTimeout := time.Duration(30) * time.Millisecond

		rf.mu.Lock()
		state := rf.state
		term := rf.currentTerm
		rf.mu.Unlock()

		switch state {
		case Follower:
			select {
			case <-rf.resetElectionTimerCh:
			case <-time.After(electionTimeout):
				go rf.startElection(state, term)
			}
		case Candidate:
			select {
			case <-rf.resetElectionTimerCh:
			case <-time.After(electionTimeout):
				go rf.startElection(state, term)
			}
		case Leader:
			<-time.After(heartbeatTimeout)
			go rf.broadcastAppendEntries()
		default:
			panic("invalid state")
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		var applyMsgs []raftapi.ApplyMsg

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			iSlice := rf.sliceIndex(i)

			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[iSlice].Command,
				CommandIndex: i,
			}

			applyMsgs = append(applyMsgs, msg)

			rf.lastApplied = i
		}

		rf.mu.Unlock()

		for _, msg := range applyMsgs {
			rf.applyCh <- msg
		}

		time.Sleep(20 * time.Millisecond)
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{
		Term:    0,
		Command: nil,
	})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.resetElectionTimerCh = make(chan struct{}, 1)

	rf.applyCh = applyCh

	rf.lastSnapshotIndex = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applier goroutine
	go rf.applier()

	return rf
}
