package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf("kv | "+format, a...)
	}
	return
}

const threshold = 100

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       string // "Get" "Put" or "Append"
	Key      string
	Value    string // for PutAppend
	ClientID int64
	SeqNum   int64 // for PutAppend
}

// for transfering the executing result to the according RPC handler
type res struct {
	clientID int64
	seqNum   int64
	err      Err
	value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db        map[string]string // database
	resChan   map[int]chan res  // channels for transferring res, index -> channel
	dupDetect map[int64]int64   // clientID -> latest seq num
	persister *raft.Persister
	// recvedIndex int // latest index of snapshot
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Op:       GetOp,
		Key:      args.Key,
		ClientID: args.ClientID,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.lock(kv.me, "Get")
	c := make(chan res, 1)
	kv.resChan[index] = c
	kv.unlock(kv.me, "Get")

	DPrintf("%d waiting for op %v index %d", kv.me, op, index)
	// r := <-kv.resChan[index]
	r := kv.bePoked(c)
	if r.clientID == -1 || r.clientID != op.ClientID || r.seqNum != op.SeqNum {
		// different req appears at the index, leader has changed
		reply.Err = ErrFail
		return
	}
	reply.Err = r.err
	reply.Value = r.value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Op:       args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		SeqNum:   args.SeqNum,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.lock(kv.me, "PutAppend")
	c := make(chan res, 1)
	kv.resChan[index] = c
	kv.unlock(kv.me, "PutAppend")

	DPrintf("%d waiting for op %v index %d", kv.me, op, index)
	// r := <-kv.resChan[index]
	r := kv.bePoked(c)
	if r.clientID == -1 || r.clientID != op.ClientID || r.seqNum != op.SeqNum {
		reply.Err = ErrFail
		return
	}
	reply.Err = r.err
	return
}

func (kv *KVServer) bePoked(c chan res) res {
	ticker := time.NewTicker(time.Second)
	r := res{
		clientID: -1,
	}
	select {
	case r = <-c:
		return r
	case <-ticker.C:
		return r
	}
}

// applier reads message from apply ch and execute it
func (kv *KVServer) applier() {
	for !kv.killed() {
		m := <-kv.applyCh
		if m.CommandValid {
			// contains a newly committed log entry
			// kv.lock(kv.me, "updateRecvedIndex")
			// kv.recvedIndex = m.CommandIndex
			// kv.unlock(kv.me, "updateRecvedIndex")
			op := m.Command.(Op)
			index := m.CommandIndex
			DPrintf("%d recv op %v index %d", kv.me, op, index)
			r := res{
				clientID: op.ClientID,
				seqNum:   op.SeqNum,
			}
			switch op.Op {
			case GetOp:
				{
					kv.lock(kv.me, "execute")
					v, ok := kv.db[op.Key]
					DPrintf("%d get %v - %v", kv.me, op.Key, v)
					kv.unlock(kv.me, "execute")
					if ok {
						r.value = v
						r.err = OK
					} else {
						r.value = ""
						r.err = ErrNoKey
					}
				}
			case PutOp:
				{
					kv.lock(kv.me, "execute")
					latestSeqNum, ok := kv.dupDetect[op.ClientID]
					if !ok || op.SeqNum > latestSeqNum {
						kv.db[op.Key] = op.Value
						DPrintf("%d db[%v]=%v", kv.me, op.Key, kv.db[op.Key])
						kv.dupDetect[op.ClientID] = op.SeqNum
					}
					kv.unlock(kv.me, "execute")
					r.err = OK
				}
			case AppendOp:
				{
					kv.lock(kv.me, "execute")
					latestSeqNum, ok := kv.dupDetect[op.ClientID]
					if !ok || op.SeqNum > latestSeqNum {
						v, ok := kv.db[op.Key]
						if ok {
							newV := v + op.Value
							kv.db[op.Key] = newV
						} else {
							kv.db[op.Key] = op.Value
						}
						DPrintf("%d db[%v]=%v", kv.me, op.Key, kv.db[op.Key])
						kv.dupDetect[op.ClientID] = op.SeqNum
					}
					kv.unlock(kv.me, "execute")
					r.err = OK
				}
			}
			kv.lock(kv.me, "resChan")
			c, ok := kv.resChan[index]
			kv.unlock(kv.me, "resChan")
			if ok {
				c <- r
			}
			if kv.maxraftstate != -1 {
				if kv.maxraftstate-kv.persister.RaftStateSize() < threshold {
					kv.doSnapshot(index)
				}
			}
		}

		if m.SnapshotValid {
			// snapshot msg
			DPrintf("%d recv snapshot %d from leader", kv.me, m.SnapshotIndex)
			/*
				kv.lock(kv.me, "check snapshot op")
				if m.SnapshotIndex <= kv.recvedIndex {
					// stale snapshot
					DPrintf("%d recvedIndex %d, ssIndex %d, stale", kv.me, kv.recvedIndex, m.SnapshotIndex)
					kv.unlock(kv.me, "check snapshot op")
					continue
				}
				kv.unlock(kv.me, "check snapshot op")
			*/
			kv.readSnapshot(m.Snapshot)
		}
	}
}

// RaftStateSize is too large, do a snapshot
func (kv *KVServer) doSnapshot(index int) {
	DPrintf("%d before snapshot: %d", kv.me, kv.persister.RaftStateSize())
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.lock(kv.me, "doSnapshot")
	e.Encode(kv.db)
	e.Encode(kv.dupDetect)
	// e.Encode(kv.recvedIndex)
	kv.unlock(kv.me, "doSnapshot")
	snapshot := w.Bytes()
	go kv.rf.Snapshot(index, snapshot)
}

// be inited or get snapshot from leader
func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		DPrintf("%d no data.", kv.me)
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var dbTmp map[string]string
	var dupTmp map[int64]int64
	// var ssIndexTmp int
	if d.Decode(&dbTmp) != nil ||
		d.Decode(&dupTmp) != nil {
		DPrintf("decode snapshot fail.")
		return
	}
	kv.lock(kv.me, "readSnapshot")
	kv.db = dbTmp
	kv.dupDetect = dupTmp
	// kv.recvedIndex = ssIndexTmp
	DPrintf("%d db: %v", kv.me, kv.db)
	kv.unlock(kv.me, "readSnapshot")
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.resChan = make(map[int]chan res)
	kv.dupDetect = make(map[int64]int64)
	kv.persister = persister
	// kv.recvedIndex = -1

	kv.readSnapshot(kv.persister.ReadSnapshot())
	go kv.applier()

	return kv
}

func (kv *KVServer) lock(i int, msg string) {
	kv.mu.Lock()
	DPrintf("kv lock: %d %v", i, msg)
}

func (kv *KVServer) unlock(i int, msg string) {
	kv.mu.Unlock()
	DPrintf("kv unlock: %d %v", i, msg)
}
