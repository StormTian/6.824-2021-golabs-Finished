package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const null = -1

const (
	follower  = 0
	candidate = 1
	leader    = 2
)

const (
	heartbeatInterval     = 100 * time.Millisecond
	checkElectionInterval = 20 * time.Millisecond
	retryInterval         = 50 * time.Millisecond
	// applyInterval         = 40 * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int
	votedFor    int
	log         []Entry
	// volatile state
	commitIndex int
	lastApplied int
	// volatile state for leader
	nextIndex  []int
	matchIndex []int

	role            int // follower, candidate, leader
	startTime       time.Time
	electionTimeout time.Duration
	numOfVote       int
	majority        int
	applyCh         chan ApplyMsg

	lastIncludedTerm  int
	lastIncludedIndex int
	informApplyCh     chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.lock(rf.me, "GetState")
	defer rf.unlock(rf.me, "GetState")
	term = rf.currentTerm
	isleader = false
	if rf.role == leader {
		isleader = true
	}
	return term, isleader
}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastIncludedIndex)
	data := w.Bytes()
	return data
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	/*
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		// locking
		// rf.lock(rf.me, "persist")
		e.Encode(rf.currentTerm)
		e.Encode(rf.votedFor)
		e.Encode(rf.log)
		// rf.unlock(rf.me, "persist")
		data := w.Bytes()
	*/
	data := rf.encodeRaftState()
	rf.persister.SaveRaftState(data)
	DPrintf("%d RaftStateSize: %v", rf.me, rf.persister.RaftStateSize())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("%d no data.", rf.me)
		return
	}
	// Your code here (2C).
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
	var currentTermTmp int
	var votedForTmp int
	var logTmp []Entry
	var lastIncludedTermTmp int
	var lastIncludedIndexTmp int
	if d.Decode(&currentTermTmp) != nil ||
		d.Decode(&votedForTmp) != nil ||
		d.Decode(&logTmp) != nil ||
		d.Decode(&lastIncludedTermTmp) != nil ||
		d.Decode(&lastIncludedIndexTmp) != nil {
		DPrintf("decode raft state fail.")
		return
	}
	rf.lock(rf.me, "readPersist")
	rf.currentTerm = currentTermTmp
	rf.votedFor = votedForTmp
	rf.log = logTmp
	rf.lastIncludedTerm = lastIncludedTermTmp
	rf.lastIncludedIndex = lastIncludedIndexTmp
	DPrintf("%d log:\n%v", rf.me, rf.log)
	rf.unlock(rf.me, "readPersist")
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock(rf.me, "InstallSnapshot")
	defer rf.unlock(rf.me, "InstallSnapshot")
	DPrintf("be called InstallSnapshot %d -> %d", args.LeaderID, rf.me)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("args.Term %d < %d Term %d", args.Term, rf.me, rf.currentTerm)
		return
	} else if rf.role == candidate && args.Term == rf.currentTerm ||
		args.Term > rf.currentTerm {
		rf.transToFollower(args.Term)
	}
	rf.resetTimer()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		DPrintf("%d -> %d stale snapshot", args.LeaderID, rf.me)
		return
	}
	if args.LastIncludedIndex < rf.getRealLength() &&
		rf.log[rf.getPos(args.LastIncludedIndex)].Term == args.LastIncludedTerm {
		// this snapshot describes a prefix of log due to retransmission or by mistake.
		// delete entries covered by the snapshot and retain the entries following the snapshot.
		rf.log = rf.log[rf.getPos(args.LastIncludedIndex):]
	} else {
		dummyEntry := Entry{
			Term:  args.LastIncludedTerm,
			Index: args.LastIncludedIndex,
		}
		rf.log = []Entry{dummyEntry}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persistAll(args.Data)
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCh <- msg

	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) callInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	for !rf.killed() {
		DPrintf("[InstallSnapshot] RPC call %d -> %d", rf.me, server)
		ok := rf.sendInstallSnapshot(server, args, reply)
		if !ok {
			return
		}
		rf.lock(rf.me, "callInstallSnapshot")
		defer rf.unlock(rf.me, "callInstallSnapshot")
		if rf.role != leader || rf.currentTerm != args.Term {
			DPrintf("%d get stale reply.", rf.me)
			return
		}
		if reply.Term > rf.currentTerm {
			rf.transToFollower(reply.Term)
			return
		}
		// success
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
		// LastIncludedIndex surely has been committed, thus don't
		// need to check update commitIndex
		return
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	DPrintf("%d get snapshot from service", rf.me)
	rf.lock(rf.me, "Snapshot")
	defer rf.unlock(rf.me, "Snapshot")
	rf.log = rf.log[rf.getPos(index):] // store the last entry as dummy entry
	rf.lastIncludedTerm = rf.log[0].Term
	rf.lastIncludedIndex = index
	/*
		newLog := []Entry{}
		for _, entry := range rf.log[rf.getPos(index):] {
			newLog = append(newLog, entry)
		}
		rf.log = newLog
	*/
	rf.persistAll(snapshot)
}

// locking
// save Raft's persistent state and snapshot to stable storage.
func (rf *Raft) persistAll(snapshot []byte) {
	raftState := rf.encodeRaftState()
	rf.persister.SaveStateAndSnapshot(raftState, snapshot)
	DPrintf("%d persist all\n"+
		"lastIncludedIndex %d, lastIncludedTerm %d\n"+
		"log: %v\n"+"snapshot length: %d",
		rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.log, len(snapshot))
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int
	// same index + same term -> same entry
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock(rf.me, "RequestVote")
	defer rf.unlock(rf.me, "RequestVote")
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.transToFollower(args.Term)
	}
	reply.VoteGranted = false
	if rf.votedFor == null || rf.votedFor == args.CandidateID {
		lastEntry := rf.log[len(rf.log)-1]
		if args.LastLogTerm > lastEntry.Term ||
			args.LastLogTerm == lastEntry.Term && args.LastLogIndex >= lastEntry.Index {
			rf.votedFor = args.CandidateID
			rf.persist()
			reply.VoteGranted = true
			rf.resetTimer()
			DPrintf("%d votes to %d", rf.me, args.CandidateID)
		}
	}
	return
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock(rf.me, "AppendEntries")
	defer rf.unlock(rf.me, "AppendEntries")
	DPrintf("be called AppendEntries %d -> %d", args.LeaderID, rf.me)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("args.Term %d < %d Term %d", args.Term, rf.me, rf.currentTerm)
		return
	} else if rf.role == candidate && args.Term == rf.currentTerm ||
		args.Term > rf.currentTerm {
		rf.transToFollower(args.Term)
	}
	rf.resetTimer()

	reply.Success = false
	if rf.getRealLength() <= args.PrevLogIndex {
		// log is too short
		reply.ConflictIndex = rf.getRealLength()
		reply.ConflictTerm = null
		DPrintf("[not have] log inconsistency %d -> %d.\n"+"ConflictTerm %d, ConflictIndex %d\n"+
			"follower log: %v\nprev: term %d, index %d",
			args.LeaderID, rf.me, reply.ConflictTerm, reply.ConflictIndex, rf.log, args.PrevLogTerm, args.PrevLogIndex)
		return
	}
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = null
		return
	}
	if rf.log[rf.getPos(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[rf.getPos(args.PrevLogIndex)].Term
		var i int
		for i = args.PrevLogIndex - 1; i >= rf.lastIncludedIndex; i-- {
			// DPrintf("%v", rf.log[i])
			if rf.log[rf.getPos(i)].Term != reply.ConflictTerm {
				// DPrintf("break")
				break
			}
		}
		reply.ConflictIndex = i + 1 // the first index that stores for conflictTerm
		DPrintf("[conflict] log inconsistency %d -> %d.\n"+"ConflictTerm %d, ConflictIndex %d\n"+
			"follower log: %v\nprev: term %d, index %d",
			args.LeaderID, rf.me, reply.ConflictTerm, reply.ConflictIndex, rf.log, args.PrevLogTerm, args.PrevLogIndex)
		return
	}

	reply.Success = true
	for _, entry := range args.Entries {
		if entry.Index == rf.getRealLength() {
			rf.log = append(rf.log, entry)
			rf.persist()
			continue
		}
		if rf.log[rf.getPos(entry.Index)].Term != entry.Term {
			// conflict, delete the existing entry and all that follow it.
			rf.log = rf.log[0:rf.getPos(entry.Index)]
			// append this new entry
			rf.log = append(rf.log, entry)
			rf.persist()
		} else {
			// already have this entry
		}
	}
	lastNewEntryIndex := rf.getRealLength() - 1
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit <= lastNewEntryIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewEntryIndex
		}
		DPrintf("%d updates commitIndex to %d.", rf.me, rf.commitIndex)
		// go rf.applyEntries()
		go func() {
			rf.informApplyCh <- struct{}{}
		}()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.lock(rf.me, "Start")
	defer rf.unlock(rf.me, "Start")
	if rf.role != leader {
		isLeader = false
		return index, term, isLeader
	}
	index = rf.getRealLength()
	term = rf.currentTerm
	entry := Entry{
		Term:    term,
		Index:   index,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	DPrintf("%d start new entry: term %d, index %d, command %v.",
		rf.me, term, index, command)
	go rf.oneHeartbeat()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.lock(rf.me, "ticker")
		if (rf.role == follower || rf.role == candidate) &&
			time.Since(rf.startTime) > rf.electionTimeout {
			// timeout, start a leader election
			DPrintf("%d becomes candidate.", rf.me)
			rf.role = candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.persist()
			rf.numOfVote = 1
			rf.resetTimer()
			// call RequestVote to each other server
			lastEntry := rf.log[len(rf.log)-1]
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: lastEntry.Index,
				LastLogTerm:  lastEntry.Term,
			}
			for i, _ := range rf.peers {
				if i != rf.me {
					reply := RequestVoteReply{}
					go rf.callRequestVote(i, &args, &reply) // call concurrently
				}
			}
		}
		rf.unlock(rf.me, "ticker")
		time.Sleep(checkElectionInterval)
	}
}

func (rf *Raft) callRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	for !rf.killed() {
		DPrintf("[RequestVote] RPC call %d -> %d", rf.me, server)
		ok := rf.sendRequestVote(server, args, reply)
		if !ok {
			// retry
			time.Sleep(retryInterval)
			rf.lock(rf.me, "callRequestVote retry")
			if rf.role == candidate && rf.currentTerm == args.Term {
				rf.unlock(rf.me, "callRequestVote retry")
				continue
			} else {
				rf.unlock(rf.me, "callRequestVote retry")
				return
			}
		}
		rf.lock(rf.me, "callRequestVote")
		defer rf.unlock(rf.me, "callRequestVote")
		if rf.role != candidate || rf.currentTerm != args.Term {
			DPrintf("not candidate %d.", rf.me)
			return
		}
		if reply.Term > rf.currentTerm {
			rf.transToFollower(reply.Term)
			return
		}
		if reply.VoteGranted {
			rf.numOfVote++
			if rf.numOfVote >= rf.majority && rf.role == candidate {
				DPrintf("%d becomes leader.", rf.me)
				rf.role = leader
				// init nI and mI
				rf.nextIndex = []int{}
				rf.matchIndex = []int{}
				nI := rf.getRealLength()
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex = append(rf.nextIndex, nI)
					rf.matchIndex = append(rf.matchIndex, 0)
				}
				go rf.sendHeartbeats()
			}
		}
		return
	}
}

// leader sends heartbeats to each server periodically.
func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		isLeader := rf.oneHeartbeat()
		if !isLeader {
			return
		}
		time.Sleep(heartbeatInterval)
	}
}

// start a heartbeat.
func (rf *Raft) oneHeartbeat() (isLeader bool) {
	isLeader = true
	rf.lock(rf.me, "oneHeartbeat")
	defer rf.unlock(rf.me, "oneHeartbeat")
	if rf.role != leader {
		isLeader = false
		return
	}
	for i, _ := range rf.peers {
		if i != rf.me {
			nI := rf.nextIndex[i]
			if nI <= rf.lastIncludedIndex {
				// send snapshot
				snapshot := rf.persister.ReadSnapshot()
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderID:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              snapshot,
				}
				reply := InstallSnapshotReply{}
				go rf.callInstallSnapshot(i, &args, &reply)
				continue
			}
			prevEntry := rf.log[rf.getPos(nI-1)]
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevEntry.Index,
				PrevLogTerm:  prevEntry.Term,
				Entries:      rf.log[rf.getPos(nI):],
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			go rf.callAppendEntries(i, &args, &reply)
		}
	}
	return
}

func (rf *Raft) callAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	for !rf.killed() {
		DPrintf("[AppendEntries] RPC call %d -> %d\n"+"%v", rf.me, server, args.Entries)
		ok := rf.sendAppendEntries(server, args, reply)
		if !ok {
			// DPrintf("AppendEntries %d -> %d fail.", rf.me, server)
			return
			/*
				// retry
				time.Sleep(retryInterval)
				rf.lock(rf.me, "callAppendEntries retry")
				if rf.role == leader {
					rf.unlock(rf.me, "callAppendEntries retry")
					continue
				} else {
					rf.unlock(rf.me, "callAppendEntries retry")
					return
				}
			*/
		}
		rf.lock(rf.me, "callAppendEntries")
		defer rf.unlock(rf.me, "callAppendEntries")
		if rf.role != leader || rf.currentTerm != args.Term {
			// if rf.role != leader {
			DPrintf("%d get stale reply.", rf.me)
			return
		}

		if reply.Term > rf.currentTerm {
			rf.transToFollower(reply.Term)
			return
		}

		if !reply.Success {
			// log inconsistency, just decrement nI and wait for next heartbeat.
			if rf.nextIndex[server] != args.PrevLogIndex+1 {
				return
			}

			DPrintf("%d -> %d prev: term %d, index %d\n"+"conflictTerm %d, conflictIndex %d\n",
				rf.me, server, args.PrevLogTerm, args.PrevLogIndex, reply.ConflictTerm, reply.ConflictIndex)
			if reply.ConflictTerm == null {
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				var i int // index of the last entry in conflictTerm in the log
				for i := args.PrevLogIndex - 1; i >= rf.lastIncludedIndex; i-- {
					if rf.log[rf.getPos(i)].Term == reply.ConflictTerm {
						break
					}
				}
				if i == rf.lastIncludedIndex-1 {
					// doesn't find any entry in conflictTerm
					rf.nextIndex[server] = reply.ConflictIndex
				} else {
					// set nextIndex to be the one beyond the index of the last entry
					// in conflictTerm in the log
					rf.nextIndex[server] = i + 1
				}
			}
			// rf.nextIndex[server] = args.PrevLogIndex
			DPrintf("%d -> %d log inconsistency.\nnextIndex[%d] = %d",
				rf.me, server, server, rf.nextIndex[server])
			return
		}

		// success
		if len(args.Entries) == 0 {
			// heartbeat
			return
		}
		lastAppendedIndex := args.Entries[len(args.Entries)-1].Index
		if lastAppendedIndex <= rf.matchIndex[server] {
			return
		}
		rf.nextIndex[server] = lastAppendedIndex + 1
		rf.matchIndex[server] = lastAppendedIndex
		DPrintf("%d -> %d append success.\nnextIndex[%d] = %d, matchIndex[%d] = %d.",
			rf.me, server, server, rf.nextIndex[server], server, rf.matchIndex[server])
		// update commitIndex
		if rf.matchIndex[server] > rf.commitIndex &&
			args.Entries[len(args.Entries)-1].Term == rf.currentTerm {
			count := 0
			for _, mI := range rf.matchIndex {
				if mI >= rf.matchIndex[server] {
					count++
					if count >= rf.majority-1 {
						rf.commitIndex = rf.matchIndex[server]
						DPrintf("leader %d updates commitIndex to %d.", rf.me, rf.commitIndex)
						// go rf.applyEntries()
						go func() {
							rf.informApplyCh <- struct{}{}
						}()
						break
					}
				}
			}
		}
		return
	}
}

// check commitIndex and apply new entries to the application.
func (rf *Raft) applyEntries() {
	for !rf.killed() {
		// time.Sleep(applyInterval)
		<-rf.informApplyCh
		rf.lock(rf.me, "applyEntries")
		if rf.commitIndex > rf.lastApplied {
			// exist new entries to apply
			DPrintf("%d commitIndex %d, lastApplied %d", rf.me, rf.commitIndex, rf.lastApplied)
			cI := rf.commitIndex
			lA := rf.lastApplied
			toApply := rf.log[rf.getPos(lA+1):rf.getPos(cI+1)]
			rf.unlock(rf.me, "applyEntries")
			for _, entry := range toApply {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
				rf.applyCh <- msg
			}
			rf.lock(rf.me, "applyEntries1")
			if rf.lastApplied == lA {
				rf.lastApplied = cI
			}
			rf.unlock(rf.me, "applyEntries1")
			DPrintf("%d apply %v", rf.me, toApply)
			continue
		}
		rf.unlock(rf.me, "applyEntries")
	}
}

/*
func (rf *Raft) applyEntries() {
	// rf.applymu.Lock()
	// defer rf.applymu.Unlock()
	rf.lock(rf.me, "applyEntries")
	if rf.commitIndex > rf.lastApplied {
		// exist new entries to apply
		DPrintf("%d commitIndex %d, lastApplied %d", rf.me, rf.commitIndex, rf.lastApplied)
		toApply := rf.log[rf.getPos(rf.lastApplied+1):rf.getPos(rf.commitIndex+1)]
		rf.unlock(rf.me, "applyEntries")
		for _, entry := range toApply {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			rf.applyCh <- msg
		}
		rf.lock(rf.me, "applyEntries")
		rf.lastApplied = rf.commitIndex
		rf.unlock(rf.me, "applyEntries")
		DPrintf("%d apply %v", rf.me, toApply)
		return
	}
	rf.unlock(rf.me, "applyEntries")
}
*/

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = null
	rf.log = []Entry{}
	empEntry := Entry{
		Term:  null,
		Index: 0,
	}
	rf.log = append(rf.log, empEntry) // log contains an empty entry at head

	rf.role = follower
	rf.numOfVote = 0
	rf.majority = len(rf.peers)/2 + 1
	rf.applyCh = applyCh

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = null
	// rf.condCh = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	rf.informApplyCh = make(chan struct{}, 5) // 0 is ok

	// init timer
	rf.resetTimer()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyEntries()

	return rf
}

// reset timer while receiving AppendEntries RPC from current leader
// or granting vote to candidate.
// locking.
func (rf *Raft) resetTimer() {
	rf.electionTimeout = time.Duration(200+rand.Intn(150)) * time.Millisecond // 200~350ms
	rf.startTime = time.Now()
}

// transform to follower when RPC request or response contains
// term T > currentTerm.
// locking.
func (rf *Raft) transToFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.role = follower
	rf.votedFor = null
	rf.persist()
	DPrintf("%d trans to follower.\nterm: %d", rf.me, rf.currentTerm)
}

// locking
// given the entry's index, return its position in current log
func (rf *Raft) getPos(index int) int {
	// because of dummy entry
	DPrintf("%d index %d, lastIncludedIndex %d", rf.me, index, rf.lastIncludedIndex)
	return index - rf.lastIncludedIndex
}

// locking
// get the real length of log
func (rf *Raft) getRealLength() int {
	// because of dummy entry
	// thus lastIncludedIndex need to be inited to 0
	DPrintf("%d length %d, lastIncludedIndex %d", rf.me, len(rf.log), rf.lastIncludedIndex)
	return len(rf.log) + rf.lastIncludedIndex
}
