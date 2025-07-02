package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

func (rf *Raft) DPrintf(format string, a ...interface{}) {
	if Debug {
		state := ""

		switch rf.state {
		case Follower:
			state = "follower"
		case Candidate:
			state = "candidate"
		case Leader:
			state = "leader"
		}

		serverDetails := fmt.Sprintf("[%d] Server %d (%s): ", rf.currentTerm, rf.me, state)
		format = serverDetails + format

		log.Printf(format, a...)
	}
}
