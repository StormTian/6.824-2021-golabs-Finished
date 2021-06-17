package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) lock(i int, msg string) {
	rf.mu.Lock()
	DPrintf("rf lock: %d %v", i, msg)
}

func (rf *Raft) unlock(i int, msg string) {
	rf.mu.Unlock()
	DPrintf("rf unlock: %d %v", i, msg)
}
